package redis

import (
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

const pongTimeout = 5 * time.Second

type subConn struct {
	pool       *redis.Pool
	outputChan chan redis.Message

	mu                 sync.RWMutex
	subscribedPatterns map[interface{}]bool

	currConn *redis.PubSubConn
}

func newSubConn(endpoint string) (*subConn, error) {
	pool := newRedisPool(endpoint, poolOptions{
		MaxActive:      3,
		MaxIdle:        1,
		SetReadTimeout: false,
	})
	subConn := &subConn{
		pool:               pool,
		outputChan:         make(chan redis.Message, 10),
		subscribedPatterns: map[interface{}]bool{},
	}

	if err := subConn.resetConn(); err != nil {
		return nil, err
	}
	go subConn.mainLoop()

	return subConn, nil
}

func (c *subConn) PSubscribe(patterns []interface{}) error {
	if err := c.currConn.PSubscribe(patterns...); err != nil {
		return errors.WithStack(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pattern := range patterns {
		c.subscribedPatterns[pattern] = true
	}
	return nil
}

func (c *subConn) PUnsubscribe(patterns []interface{}) error {
	if len(patterns) == 0 {
		return errors.New("Must send at least one pattern to unsubscribe")
	}

	if err := c.currConn.PUnsubscribe(patterns...); err != nil {
		return errors.WithStack(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pattern := range patterns {
		delete(c.subscribedPatterns, pattern)
	}
	return nil
}

func (c *subConn) ReceiveChan() <-chan redis.Message {
	return c.outputChan
}

func (c *subConn) mainLoop() {
	var (
		pingTicker      = time.NewTicker(pingDelay)
		pongTimeoutChan <-chan time.Time

		done    = make(chan struct{})
		msgChan = connReceiveChan(c.currConn, done)
	)
	recover := func() {
		close(done)

		c.recoverConn()
		done = make(chan struct{})
		msgChan = connReceiveChan(c.currConn, done)
	}
	for {
		select {
		case <-pingTicker.C:
			err := c.currConn.Ping("")
			if err == nil {
				pongTimeoutChan = time.Tick(pongTimeout)
			} else {
				logError(err, "pubsub_ping_error", "", "", "Redis error pinging pub/sub connection")
				recover()
			}

		case <-pongTimeoutChan:
			pongTimeoutChan = nil
			recover()

		case msg := <-msgChan:
			switch v := msg.(type) {
			case redis.Message:
				c.outputChan <- v

			case redis.Pong:
				pongTimeoutChan = nil

			case error:
				logError(v, "pubsub_error", "", "", "Redis pub/sub error")
				recover()
			}
		}
	}
}

// Retries to reset pub/sub connection until no error occurrs
func (c *subConn) recoverConn() {
	if err := c.currConn.Conn.Err(); err != nil {
		logError(err, "redis_conn_error", "", "", "Redis connection error")
	}

	for {
		err := c.resetConn()
		if err == nil {
			break
		}
		logError(err, "redis_conn_reset_error", "", "", "Error resetting Redis connection")
		time.Sleep(1 * time.Second)
	}
}

func (c *subConn) resetConn() error {
	psc := &redis.PubSubConn{Conn: c.pool.Get()}

	// In order to always have a Redis connection in the PUB/SUB state (which
	// changes the PING behavior for example), subscribe to a dummy channel that
	// is never unsubscribed nor has anything published to it. Call Do directly
	// in the connection to wait for the successful response from Redis as well.
	if _, err := psc.Conn.Do("SUBSCRIBE", "_dummy_"); err != nil {
		psc.Close()
		return errors.Wrap(err, "Failed to subscribe to test channel")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.subscribedPatterns) > 0 {
		patterns := make([]interface{}, 0, len(c.subscribedPatterns))
		for p := range c.subscribedPatterns {
			patterns = append(patterns, p)
		}
		if err := psc.PSubscribe(patterns...); err != nil {
			psc.Close()
			return errors.Wrapf(err, "Error re-subscribing to patterns %s", patterns)
		}
	}

	prevConn := c.currConn
	c.currConn = psc
	if prevConn != nil {
		prevConn.Close()
	}
	return nil
}

func connReceiveChan(conn *redis.PubSubConn, done <-chan struct{}) <-chan interface{} {
	msgChan := make(chan interface{}, 10)
	go func() {
		for {
			if conn.Conn.Err() != nil {
				return
			}
			select {
			case msgChan <- conn.Receive():
			case <-done:
				return
			}
		}
	}()
	return msgChan
}
