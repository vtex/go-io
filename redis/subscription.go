package redis

import (
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
)

type subscription struct {
	mu       sync.RWMutex
	patterns []string
	sendCh   chan<- []byte
	done     chan struct{}
}

func newSubscription(patterns []string) (*subscription, <-chan []byte) {
	subChan := make(chan []byte, 10)
	sub := &subscription{
		patterns: patterns,
		sendCh:   subChan,
		done:     make(chan struct{}),
	}
	return sub, subChan
}

func (s *subscription) Send(data []byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	select {
	case s.sendCh <- data:
		return true
	case <-s.done:
		return false
	default:
		logrus.
			WithField("code", "pubsub_buffer_full").
			WithField("data", string(data)).
			WithField("patterns", strings.Join(s.patterns, "; ")).
			Error("Dropping message due to Redis client buffer full")
		return false
	}
}

func (s *subscription) Close() {
	close(s.done)

	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.sendCh)
}
