package redis

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

const pingDelay = 60 * time.Second

type SubChan <-chan []byte

type PubSub interface {
	Cache
	PSubscribe(patterns []string) (SubChan, error)
	PUnsubscribe(sub SubChan) error
	Publish(key string, data []byte) error
}

func NewPubSub(endpoint, keyNamespace string) (PubSub, error) {
	subConn, err := newSubConn(endpoint)
	if err != nil {
		return nil, err
	}

	pubsub := &redisPubSub{
		redisC:           New(endpoint, keyNamespace).(*redisC),
		subscriptionConn: subConn,
		clientsByChan:    map[SubChan]*pubsubClient{},
		clientsByPattern: map[string][]*pubsubClient{},
	}
	go pubsub.mainLoop()

	return pubsub, nil
}

type redisPubSub struct {
	*redisC

	subscriptionConn *subConn

	// Both maps hold the same subscriptions, the only difference is that in
	// subsByPattern they are indexed by subscription pattern so that receiving
	// from a Redis channel and forwarding to applicable subscriptions doesn't
	// need iterating through all subscriptions.
	clientsLock      sync.RWMutex
	clientsByChan    map[SubChan]*pubsubClient
	clientsByPattern map[string][]*pubsubClient
}

func (r *redisPubSub) PSubscribe(patterns []string) (SubChan, error) {
	patterns, err := remoteKeys(r.keyNamespace, patterns)
	if err != nil {
		return nil, err
	}

	recvCh, newPatterns := r.startSub(patterns)
	if len(newPatterns) > 0 {
		if err := r.subscriptionConn.PSubscribe(newPatterns); err != nil {
			r.deactivate(recvCh)
			return nil, errors.WithStack(err)
		}
	}
	return recvCh, nil
}

func (r *redisPubSub) PUnsubscribe(recvCh SubChan) error {
	unusedPatterns, err := r.deactivate(recvCh)
	if err != nil {
		return err
	}

	if len(unusedPatterns) > 0 {
		if err := r.subscriptionConn.PUnsubscribe(unusedPatterns); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (r *redisPubSub) Publish(key string, data []byte) error {
	key, err := remoteKey(r.keyNamespace, key)
	if err != nil {
		return err
	}

	if _, err := r.doCmd("PUBLISH", key, string(data)); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *redisPubSub) mainLoop() {
	for msg := range r.subscriptionConn.ReceiveChan() {
		r.send(msg.Pattern, msg.Data)
	}
}

func (r *redisPubSub) send(pattern string, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			logrus.
				WithField("code", "pubsub_error").
				WithField("panic", r).
				WithField("pattern", pattern).
				WithField("data", string(data)).
				Error("Error executing redis pub/sub callback")
		}
	}()

	r.clientsLock.RLock()
	defer r.clientsLock.RUnlock()

	for _, cl := range r.clientsByPattern[pattern] {
		cl.Send(data)
	}
}

func (r *redisPubSub) startSub(patterns []string) (SubChan, []interface{}) {
	r.clientsLock.Lock()
	defer r.clientsLock.Unlock()

	cl, recvCh := newPubSubClient(patterns)
	r.clientsByChan[recvCh] = cl

	newPatterns := []interface{}{}
	for _, p := range patterns {
		patternClients := r.clientsByPattern[p]
		if len(patternClients) == 0 {
			newPatterns = append(newPatterns, p)
		}
		patternClients = append(patternClients, cl)

		r.clientsByPattern[p] = patternClients
	}

	return recvCh, newPatterns
}

func (r *redisPubSub) deactivate(recvCh SubChan) ([]interface{}, error) {
	cl := r.clientByChan(recvCh)
	if cl == nil {
		return nil, errors.Errorf("Attempt to deactive unknown subscription: %v", recvCh)
	}
	cl.Close()

	r.clientsLock.Lock()
	defer r.clientsLock.Unlock()

	delete(r.clientsByChan, recvCh)

	unusedPatterns := []interface{}{}
	for _, p := range cl.patterns {
		patternClients := r.clientsByPattern[p]

		patternClients = removeClient(patternClients, cl)
		if len(patternClients) > 0 {
			r.clientsByPattern[p] = patternClients
		} else {
			delete(r.clientsByPattern, p)
			unusedPatterns = append(unusedPatterns, p)
		}
	}

	return unusedPatterns, nil
}

func removeClient(clients []*pubsubClient, cl *pubsubClient) []*pubsubClient {
	for i, elm := range clients {
		if elm == cl {
			return append(clients[:i], clients[i+1:]...)
		}
	}
	return clients
}

func (r *redisPubSub) clientByChan(subChan SubChan) *pubsubClient {
	r.clientsLock.RLock()
	cl := r.clientsByChan[subChan]
	r.clientsLock.RUnlock()
	return cl
}
