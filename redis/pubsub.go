package redis

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
)

type RedisSubChan <-chan []byte

type RedisPubSub interface {
	Redis
	PSubscribe(patterns []string) (RedisSubChan, error)
	PUnsubscribe(sub RedisSubChan) error
	Publish(key string, data []byte) error
}

func NewPubSub(endpoint, keyNamespace string) (Redis, error) {
	pool := newRedisPool(endpoint, 10, 20)

	pubsub := &redisPubSub{
		redisC:        &redisC{pool: pool, keyNamespace: keyNamespace},
		subsByChan:    map[RedisSubChan]*subscription{},
		subsByPattern: map[string]map[*subscription]bool{},
	}
	if err := pubsub.resetPubSubConn(); err != nil {
		return nil, err
	}
	go pubsub.mainLoop()

	return pubsub, nil
}

type subscription struct {
	patterns []string
	sendCh   chan []byte
}

type redisPubSub struct {
	*redisC

	lock             sync.RWMutex
	subscriptionConn *redis.PubSubConn

	// Both maps hold the same subscriptions, the only difference is that in
	// subsByPattern they are indexed by subscription pattern so that receiving
	// from a Redis channel and forwarding to applicable subscriptions doesn't
	// need iterating through all subscriptions.
	subsByChan    map[RedisSubChan]*subscription
	subsByPattern map[string]map[*subscription]bool
}

func (r *redisPubSub) PSubscribe(patterns []string) (RedisSubChan, error) {
	recvCh, newPatterns := r.startSub(patterns)

	if len(newPatterns) > 0 {
		if err := r.subscriptionConn.PSubscribe(newPatterns...); err != nil {
			r.deactivate(recvCh)
			return nil, errors.WithStack(err)
		}
	}
	return recvCh, nil
}

func (r *redisPubSub) PUnsubscribe(sub RedisSubChan) error {
	unusedPatterns := r.deactivate(sub)

	if len(unusedPatterns) > 0 {
		if err := r.subscriptionConn.PUnsubscribe(unusedPatterns...); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (r *redisPubSub) Publish(key string, data []byte) error {
	if _, err := r.doCmd("PUBLISH", key, string(data)); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *redisPubSub) mainLoop() {
	msgChan := make(chan redis.PMessage, 10)
	go func() {
		for {
			switch v := r.subscriptionConn.Receive().(type) {
			case redis.PMessage:
				msgChan <- v

			case error:
				logError(v, "pubsub_error", r.keyNamespace, "", "Redis pub/sub error")
				r.recoverPubSubConn()
			}
		}
	}()

	pingTicker := time.NewTicker(pingDelay)
	for {
		select {
		case <-pingTicker.C:
			r.subscriptionConn.Ping("")
		case msg := <-msgChan:
			r.send(msg.Pattern, msg.Data)
		}
	}
}

func (r *redisPubSub) startSub(patterns []string) (RedisSubChan, []interface{}) {
	r.lock.Lock()
	defer r.lock.Unlock()

	sub := &subscription{
		patterns: patterns,
		sendCh:   make(chan []byte, 1),
	}
	recvCh := RedisSubChan(sub.sendCh)
	r.subsByChan[recvCh] = sub

	newPatterns := []interface{}{}
	for _, p := range patterns {
		cs, ok := r.subsByPattern[p]
		if !ok {
			cs = map[*subscription]bool{}
			r.subsByPattern[p] = cs
			newPatterns = append(newPatterns, p)
		}
		cs[sub] = true
	}

	return recvCh, newPatterns
}

func (r *redisPubSub) deactivate(subChan RedisSubChan) []interface{} {
	r.lock.Lock()
	defer r.lock.Unlock()

	sub, ok := r.subsByChan[subChan]
	if !ok {
		return nil
	}
	delete(r.subsByChan, subChan)
	close(sub.sendCh)

	unusedPatterns := []interface{}{}
	for _, p := range sub.patterns {
		cs := r.subsByPattern[p]

		delete(cs, sub)
		if len(cs) == 0 {
			delete(r.subsByPattern, p)
			unusedPatterns = append(unusedPatterns, p)
		}
	}

	return unusedPatterns
}

// Retries to reset pub/sub connection until no error occurrs
func (r *redisPubSub) recoverPubSubConn() {
	if err := r.subscriptionConn.Conn.Err(); err != nil {
		logError(err, "redis_conn_error", r.keyNamespace, "", "Redis connection error")
	}

	for {
		err := r.resetPubSubConn()
		if err == nil {
			break
		}
		logError(err, "redis_conn_reset_error", r.keyNamespace, "", "Error resetting Redis connection")
		time.Sleep(1 * time.Second)
	}
}

func (r *redisPubSub) resetPubSubConn() error {
	r.lock.RLock()
	defer r.lock.RUnlock()

	psc := &redis.PubSubConn{Conn: r.pool.Get()}

	// In order to always have a Redis connection in the PUB/SUB state (which
	// changes the PING behavior for example), subscribe to a dummy channel that
	// is never unsubscribed nor has anything published to it. Call Do directly
	// in the connection to wait for the successful response from Redis as well.
	if _, err := psc.Conn.Do("SUBSCRIBE", "_dummy_"); err != nil {
		return errors.Wrap(err, "Failed to subscribe to test channel")
	}

	if len(r.subsByPattern) > 0 {
		patterns := make([]interface{}, 0, len(r.subsByPattern))
		for p := range r.subsByPattern {
			patterns = append(patterns, p)
		}
		if err := psc.PSubscribe(patterns...); err != nil {
			psc.Close()
			return errors.Wrapf(err, "Error re-subscribing to patterns %s", patterns)
		}
	}

	prevConn := r.subscriptionConn
	r.subscriptionConn = psc
	if prevConn != nil {
		prevConn.Close()
	}
	return nil
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
	r.lock.RLock()
	defer r.lock.RUnlock()

	if cs, ok := r.subsByPattern[pattern]; ok {
		for sub := range cs {
			sub.sendCh <- data
		}
	}
}
