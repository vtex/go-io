package redis

import (
	"context"
	"time"

	goErrors "errors"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

const (
	channelsBuffersSize = 100
	pongTimeout         = 5 * time.Second
	subscribeTimeout    = 3 * time.Second
)

var (
	unknownMessageTypeErr = goErrors.New("Unknown message type")
	pongTimeoutErr        = goErrors.New("Pong timeout")
)

type subConn struct {
	pool *redis.Pool

	outputChan      chan redis.Message
	subscribeChan   chan []interface{}
	unsubscribeChan chan []interface{}
}

func newSubConn(endpoint string) (*subConn, error) {
	subConn := &subConn{
		pool: newRedisPool(endpoint, poolOptions{
			MaxActive:      3,
			MaxIdle:        1,
			SetReadTimeout: false,
		}),
		outputChan:      make(chan redis.Message, channelsBuffersSize),
		subscribeChan:   make(chan []interface{}, channelsBuffersSize),
		unsubscribeChan: make(chan []interface{}, channelsBuffersSize),
	}

	err := startMainLoop(subConn)
	if err != nil {
		return nil, err
	}
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
		return errors.New("Timeout sending patterns to subscriptions channel")
	}
}

func (c *subConn) PUnsubscribe(patterns []interface{}) error {
	if len(patterns) == 0 {
		return errors.New("Must send at least one pattern to unsubscribe")
	}

	// Since this is almost a clean-up done after the consumers are done with us,
	// we don't have a timeout here so we don't risk leaving an inconsistent state.
	// If we have any problems with this, consider moving this to a background
	// routine so we don't block the caller.
	c.unsubscribeChan <- patterns
	return nil
}

func (c *subConn) ReceiveChan() <-chan redis.Message {
	return c.outputChan
}

type pubSubLoopState struct {
	parent             *subConn
	subscribedPatterns map[interface{}]bool

	pingTicker      <-chan time.Time
	pongTimeoutChan <-chan time.Time

	currConn     *redis.PubSubConn
	msgChan      <-chan interface{}
	closeMsgChan func()
}

func startMainLoop(parent *subConn) error {
	loopState := &pubSubLoopState{
		parent:             parent,
		subscribedPatterns: map[interface{}]bool{},
		pingTicker:         time.Tick(pingDelay),
	}
	if err := loopState.resetConn(); err != nil {
		return err
	}

	go func() {
		for {
			err, errCode, errMsg := loopState.mainIteration()
			if err != nil {
				logError(err, errCode, "", "", errMsg)
				loopState.recoverConn()
			}
		}
	}()
	return nil
}

func (s *pubSubLoopState) mainIteration() (err error, errCode, errMsg string) {
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

	case patterns := <-s.parent.subscribeChan:
		if err = s.currConn.PSubscribe(patterns...); err != nil {
			// Retry at most once otherwise just drop the subscription request.
			// This is a safety guard (instead of retrying indefinitely), in case
			// some bad input is sent to us.
			s.recoverConn()
			err = s.currConn.PSubscribe(patterns...)
			if err != nil {
				return err, "subscribe_error", "Redis PSubscribe command error"
			}
		}

		for _, pattern := range patterns {
			s.subscribedPatterns[pattern] = true
		}
		return nil, "", ""

	case patterns := <-s.parent.unsubscribeChan:
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
func (s *pubSubLoopState) recoverConn() {
	err := s.currConn.Conn.Err()
	logError(err, "redis_conn_recover", "", "", "Recovering Redis connection due to error")

	for {
		err := s.resetConn()
		if err == nil {
			break
		}
		logError(err, "redis_conn_reset_error", "", "", "Error resetting Redis connection")
		time.Sleep(1 * time.Second)
	}
}

func (s *pubSubLoopState) resetConn() error {
	psc := &redis.PubSubConn{Conn: s.parent.pool.Get()}

	// In order to always have a Redis connection in the PUB/SUB state (which
	// changes the PING behavior for example), subscribe to a dummy channel that
	// is never unsubscribed nor has anything published to it. Call Do directly
	// in the connection to wait for the successful response from Redis as well.
	if _, err := psc.Conn.Do("SUBSCRIBE", "_dummy_"); err != nil {
		psc.Close()
		return errors.Wrap(err, "Failed to subscribe to test channel")
	}

	if len(s.subscribedPatterns) > 0 {
		patterns := make([]interface{}, 0, len(s.subscribedPatterns))
		for p := range s.subscribedPatterns {
			patterns = append(patterns, p)
		}
		if err := psc.PSubscribe(patterns...); err != nil {
			psc.Close()
			return errors.Wrapf(err, "Error re-subscribing to patterns %s", patterns)
		}
	}

	if s.currConn != nil {
		s.closeMsgChan()
		s.currConn.Close()
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.currConn = psc
	s.msgChan = connReceiveChan(ctx, psc, s.msgChan)
	s.closeMsgChan = cancel
	return nil
}

func connReceiveChan(ctx context.Context, conn *redis.PubSubConn, oldChan <-chan interface{}) <-chan interface{} {
	msgChan := make(chan interface{}, channelsBuffersSize)
	go func() {
		defer close(msgChan)
		drainOldMsgChan(msgChan, oldChan)
		for {
			if conn.Conn.Err() != nil {
				return
			}
			msg := conn.Receive()
			select {
			case msgChan <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return msgChan
}

func drainOldMsgChan(newChan chan<- interface{}, oldChan <-chan interface{}) {
	if oldChan == nil {
		return
	}
	for msg := range oldChan {
		if _, isRedisMsg := msg.(redis.Message); isRedisMsg {
			// We know this won't block since we only create a new channel
			// after (triggering) closing of the previous one, and all of
			// them have the same buffer size.
			newChan <- msg
		}
	}
}
