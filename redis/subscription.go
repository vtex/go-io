package redis

import "sync"

type subscription struct {
	mu       sync.RWMutex
	patterns []string
	sendCh   chan<- []byte
	done     chan struct{}
}

func newSubscription(patterns []string) (*subscription, <-chan []byte) {
	subChan := make(chan []byte, 1)
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
	}
}

func (s *subscription) Close() {
	close(s.done)

	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.sendCh)
}
