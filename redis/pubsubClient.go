package redis

import (
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
)

type pubsubClient struct {
	mu       sync.RWMutex
	patterns []string
	sendCh   chan<- []byte
	done     chan struct{}
}

func newPubSubClient(patterns []string) (*pubsubClient, <-chan []byte) {
	subChan := make(chan []byte, 10)
	sub := &pubsubClient{
		patterns: patterns,
		sendCh:   subChan,
		done:     make(chan struct{}),
	}
	return sub, subChan
}

func (c *pubsubClient) Send(data []byte) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.isClosed() {
		return false
	}

	select {
	case c.sendCh <- data:
		return true
	case <-c.done:
		return false
	default:
		logrus.
			WithField("code", "pubsub_buffer_full").
			WithField("data", string(data)).
			WithField("patterns", strings.Join(c.patterns, "; ")).
			Error("Dropping message due to Redis client buffer full")
		return false
	}
}

func (c *pubsubClient) Close() {
	close(c.done)

	c.mu.Lock()
	defer c.mu.Unlock()

	close(c.sendCh)
}

func (c *pubsubClient) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
