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

	loopState *pubSubLoopState
}

type pubSubLoopState struct {
	parent             *subConn
	subscribedPatterns map[interface{}]bool
	subscribeChan      chan []interface{}
	unsubscribeChan    chan []interface{}

	currConn          *redis.PubSubConn
	pingTicker        <-chan time.Time
	pongTimeoutChan   <-chan time.Time
	msgChan           <-chan interface{}
	cancelCurrMsgChan func()
}

func newSubConn(endpoint string) (*subConn, error) {
	subConn := &subConn{
		pool: newRedisPool(endpoint, poolOptions{
			MaxActive:      3,
			MaxIdle:        1,
			SetReadTimeout: false,
		}),
		outputChan: make(chan redis.Message, 10),
	}

	state, err := startMainLoop(subConn)
	if err != nil {
		return nil, err
	}
	subConn.loopState = state
	return subConn, nil
}

func (c *subConn) PSubscribe(patterns []interface{}) error {
	if len(patterns) == 0 {
		return errors.New("Must send at least one pattern to subscribe")
	}

	select {
	case c.loopState.subscribeChan <- patterns:
		return nil
	case <-time.After(subscribeTimeout):
		return errors.New("Timeout sending patterns to subscriptions channel")
	}
}

func (c *subConn) PUnsubscribe(patterns []interface{}) error {
	if len(patterns) == 0 {
		return errors.New("Must send at least one pattern to unsubscribe")
	}

	c.loopState.unsubscribeChan <- patterns
	return nil
}

func (c *subConn) ReceiveChan() <-chan redis.Message {
	return c.outputChan
}

func startMainLoop(parent *subConn) (*pubSubLoopState, error) {
	loopState := &pubSubLoopState{
		parent:             parent,
		subscribeChan:      make(chan []interface{}, 10),
		unsubscribeChan:    make(chan []interface{}, 10),
		subscribedPatterns: map[interface{}]bool{},
		pingTicker:         time.Tick(pingDelay),
	}
	if err := loopState.resetConn(); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	loopState.msgChan = connReceiveChan(loopState.currConn, done)
	loopState.cancelCurrMsgChan = func() { close(done) }

	go func() {
		recoverConn := func() {
			loopState.cancelCurrMsgChan()
			loopState.recoverConn()

			done := make(chan struct{})
			loopState.msgChan = connReceiveChan(loopState.currConn, done)
			loopState.cancelCurrMsgChan = func() { close(done) }
		}
		for {
			err, errCode, errMsg := loopState.mainIteration(recoverConn)
			if err != nil {
				logError(err, errCode, "", "", errMsg)
				recoverConn()
			}
		}
	}()
	return loopState, nil
}

func (s *pubSubLoopState) mainIteration(recoverConn func()) (err error, errCode, errMsg string) {
	select {
	case <-s.pingTicker:
		err = s.currConn.Ping("")
		if err != nil {
			return err, "pubsub_ping_error", "Redis error pinging pub/sub connection"
		}
		s.pongTimeoutChan = time.After(pongTimeout)
		return nil, "", ""

	case msg := <-s.msgChan:
		switch v := msg.(type) {
		case redis.Pong:
			s.pongTimeoutChan = nil
			return nil, "", ""

		case redis.Message:
			s.parent.outputChan <- v
			return nil, "", ""

		case error:
			return v, "pubsub_error", "Redis pub/sub error"
		}
		return errors.WithStack(unknownMessageTypeErr), "unknown_msg_type", "An unknown message type was received from Redis"

	case <-s.pongTimeoutChan:
		s.pongTimeoutChan = nil
		return errors.WithStack(pongTimeoutErr), "pong_timeout", "Timed out waiting for Redis ping response"

	case patterns := <-s.subscribeChan:
		if err = s.currConn.PSubscribe(patterns...); err != nil {
			// Retry at most once otherwise just drop the subscription request.
			// This is a safety guard (instead of retrying indefinitely), in case
			// some bad input is sent to us.
			recoverConn()
			err = s.currConn.PSubscribe(patterns...)
			if err != nil {
				return err, "subscribe_error", "Redis PSubscribe command error"
			}
		}

		for _, pattern := range patterns {
			s.subscribedPatterns[pattern] = true
		}
		return nil, "", ""

	case patterns := <-s.unsubscribeChan:
		toUnsubscribe := make([]interface{}, 0, len(patterns))
		for _, pattern := range patterns {
			if s.subscribedPatterns[pattern] {
				toUnsubscribe = append(toUnsubscribe, pattern)
				delete(s.subscribedPatterns, pattern)
			}
		}
		if len(toUnsubscribe) == 0 {
			return nil, "", ""
		}

		if err = s.currConn.PUnsubscribe(toUnsubscribe...); err != nil {
			// We don't need to retry here, since we've already removed the specific subscriptions
			// from the subscribedPatterns field, so when we recover the connection belo it will
			// come back already unsubscribed from the requested patterns.
			return err, "unsubscribe_error", "Redis PUnsubscribe command error"
		}
		return nil, "", ""
	}
}

// Retries to reset pub/sub connection until no error occurrs
func (c *pubSubLoopState) recoverConn() {
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

func (c *pubSubLoopState) resetConn() error {
	psc := &redis.PubSubConn{Conn: c.parent.pool.Get()}

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
