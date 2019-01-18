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
		subsByChan:       map[SubChan]*subscription{},
		subsByPattern:    map[string][]*subscription{},
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
	subsLock      sync.RWMutex
	subsByChan    map[SubChan]*subscription
	subsByPattern map[string][]*subscription
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

func (r *redisPubSub) PUnsubscribe(sub SubChan) error {
	unusedPatterns, err := r.deactivate(sub)
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

	subs := r.getSubsByPattern(pattern)
	for _, sub := range subs {
		sub.Send(data)
	}
}

func (r *redisPubSub) startSub(patterns []string) (SubChan, []interface{}) {
	r.subsLock.Lock()
	defer r.subsLock.Unlock()

	sub, recvCh := newSubscription(patterns)
	r.subsByChan[recvCh] = sub

	newPatterns := []interface{}{}
	for _, p := range patterns {
		subsToPattern := r.subsByPattern[p]
		if len(subsToPattern) == 0 {
			newPatterns = append(newPatterns, p)
		}
		subsToPattern = append(subsToPattern, sub)

		r.subsByPattern[p] = subsToPattern
	}

	return recvCh, newPatterns
}

func (r *redisPubSub) deactivate(subChan SubChan) ([]interface{}, error) {
	sub := r.getSubByChan(subChan)
	if sub == nil {
		return nil, errors.Errorf("Attempt to deactive unknown subscription: %v", subChan)
	}
	sub.Close()

	r.subsLock.Lock()
	defer r.subsLock.Unlock()

	delete(r.subsByChan, subChan)

	unusedPatterns := []interface{}{}
	for _, p := range sub.patterns {
		subsToPattern := r.subsByPattern[p]

		subsToPattern = removeSub(subsToPattern, sub)
		if len(subsToPattern) > 0 {
			r.subsByPattern[p] = subsToPattern
		} else {
			delete(r.subsByPattern, p)
			unusedPatterns = append(unusedPatterns, p)
		}
	}

	return unusedPatterns, nil
}

func removeSub(subs []*subscription, sub *subscription) []*subscription {
	for i, elm := range subs {
		if elm == sub {
			return append(subs[:i], subs[i+1:]...)
		}
	}
	return subs
}

func (r *redisPubSub) getSubByChan(subChan SubChan) *subscription {
	r.subsLock.RLock()
	sub := r.subsByChan[subChan]
	r.subsLock.RUnlock()
	return sub
}

func (r *redisPubSub) getSubsByPattern(pattern string) []*subscription {
	r.subsLock.RLock()
	sub := r.subsByPattern[pattern]
	r.subsLock.RUnlock()
	return sub
}
