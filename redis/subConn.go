package redis

import (
	"time"

	goErrors "errors"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

const (
	pongTimeout      = 5 * time.Second
	subscribeTimeout = 10 * time.Second
)

var (
	unknownMessageTypeErr = goErrors.New("Unknown message type")
	pongTimeoutErr        = goErrors.New("Pong timeout")
)

type subConn struct {
	pool       *redis.Pool
	outputChan chan redis.Message

	subscribeChan      chan []interface{}
	unsubscribeChan    chan []interface{}
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
		subscribeChan:      make(chan []interface{}, 10),
		unsubscribeChan:    make(chan []interface{}, 10),
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
	if len(patterns) == 0 {
		return errors.New("Must send at least one pattern to subscribe")
	}

	select {
	case c.subscribeChan <- patterns:
		return nil
	case <-time.After(subscribeTimeout):
		return errors.New("Timeout")
	}
}

func (c *subConn) PUnsubscribe(patterns []interface{}) error {
	if len(patterns) == 0 {
		return errors.New("Must send at least one pattern to unsubscribe")
	}

	c.unsubscribeChan <- patterns
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
	recoverConn := func() {
		close(done)

		c.recoverConn()
		done = make(chan struct{})
		msgChan = connReceiveChan(c.currConn, done)
	}
	for {
		err, errCode, errMsg := c.mainIteration(pingTicker.C, &pongTimeoutChan, msgChan, recoverConn)
		if err != nil {
			logError(err, errCode, "", "", errMsg)
			recoverConn()
		}
	}
}

func (c *subConn) mainIteration(pingTicker <-chan time.Time, pongTimeoutChan *<-chan time.Time, msgChan <-chan interface{}, recoverConn func()) (err error, errCode, errMsg string) {
	select {
	case <-pingTicker:
		err = c.currConn.Ping("")
		if err != nil {
			return err, "pubsub_ping_error", "Redis error pinging pub/sub connection"
		}
		*pongTimeoutChan = time.Tick(pongTimeout)
		return nil, "", ""

	case msg := <-msgChan:
		switch v := msg.(type) {
		case redis.Pong:
			pongTimeoutChan = nil
			return nil, "", ""

		case redis.Message:
			c.outputChan <- v
			return nil, "", ""

		case error:
			return v, "pubsub_error", "Redis pub/sub error"
		}
		return errors.WithStack(unknownMessageTypeErr), "unknown_msg_type", "An unknown message type was received from Redis"

	case <-*pongTimeoutChan:
		*pongTimeoutChan = nil
		return errors.WithStack(pongTimeoutErr), "pong_timeout", "Timed out waiting for Redis ping response"

	case patterns := <-c.subscribeChan:
		if err = c.currConn.PSubscribe(patterns...); err != nil {
			// Retry at most once otherwise just drop the subscription request.
			// This is a safety guard (instead of retrying indefinitely), in case
			// some bad input is sent to us.
			recoverConn()
			err = c.currConn.PSubscribe(patterns...)
			if err != nil {
				return err, "subscribe_error", "Redis PSubscribe command error"
			}
		}

		for _, pattern := range patterns {
			c.subscribedPatterns[pattern] = true
		}
		return nil, "", ""

	case patterns := <-c.unsubscribeChan:
		toUnsubscribe := make([]interface{}, 0, len(patterns))
		for _, pattern := range patterns {
			if c.subscribedPatterns[pattern] {
				toUnsubscribe = append(toUnsubscribe, pattern)
				delete(c.subscribedPatterns, pattern)
			}
		}
		if len(toUnsubscribe) == 0 {
			return nil, "", ""
		}

		if err = c.currConn.PUnsubscribe(toUnsubscribe...); err != nil {
			// We don't need to retry here, since we've already removed the specific subscriptions
			// from the subscribedPatterns field, so when we recover the connection belo it will
			// come back already unsubscribed from the requested patterns.
			return err, "unsubscribe_error", "Redis PUnsubscribe command error"
		}
		return nil, "", ""
	}
}

// Retries to reset pub/sub connection until no error occurrs
func (c *subConn) recoverConn() {
	err := c.currConn.Conn.Err()
	logError(err, "redis_conn_recover", "", "", "Recovering Redis connection due to error")

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
