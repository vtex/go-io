package worker

import (
	"sync"
)

// SyncQueue is a helper for a (channel-like) queue that allows multiple concurrent
// senders and receivers and never blocks indefinitely on enqueueuing by resizing
// the internal channel when full. It doesn't guarantee execution order when
// queue resizes occur but that should stop once queue size stabilizes.
type SyncQueue struct {
	resizeLock sync.RWMutex
	queue      chan interface{}
}

func NewSyncQueue(initialCapacity int) *SyncQueue {
	if initialCapacity <= 0 {
		panic("Sync queue capacity must be greater than zero")
	}
	return &SyncQueue{
		queue: make(chan interface{}, initialCapacity),
	}
}

func (q *SyncQueue) Enqueue(item interface{}) {
	for !q.tryEnqueue(item) {
		q.resize()
	}
}

func (q *SyncQueue) Dequeue() interface{} {
	for {
		if item, ok := <-q.queue; ok {
			return item
		}
		// If channel was closed a resize operation is taking place. Lock&unlock to wait
		// for it to finish and try again.
		q.resizeLock.RLock()
		q.resizeLock.RUnlock()
	}
}

func (q *SyncQueue) Len() int {
	return len(q.queue)
}

func (q *SyncQueue) tryEnqueue(item interface{}) bool {
	q.resizeLock.RLock()
	defer q.resizeLock.RUnlock()
	select {
	case q.queue <- item:
		return true
	default:
		return false
	}
}

func (q *SyncQueue) resize() {
	q.resizeLock.Lock()
	defer q.resizeLock.Unlock()

	if hasSpaceInBuffer(q.queue) {
		// Queue probably resized by some other routine
		return
	}

	close(q.queue)
	resized := make(chan interface{}, 2*cap(q.queue))
	for item := range q.queue {
		resized <- item
	}
	q.queue = resized
}

// We use a simple heuristic to say if there is enough space in the buffer which
// is checking if at least a third of its capacity is still unused. Checking only
// if len < cap would be bug-prone as a single concurrent receive in the channel
// could make that become true and we get stuck in a tryEnqueue-resize loop.
func hasSpaceInBuffer(c <-chan interface{}) bool {
	return 3*len(c) <= 2*cap(c)
}
