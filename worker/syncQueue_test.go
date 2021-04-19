package worker

import (
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const defaultTimeout = 1 * time.Second

func TestSyncQueue(t *testing.T) {
	Convey("Disallow creating empty queue", t, withTimeout(func() {
		So(func() {
			NewSyncQueue(0)
		}, ShouldPanic)
	}))

	Convey("Should never block on enqueueing", t, withTimeout(func() {
		initCap := 10
		queue := NewSyncQueue(initCap)

		Convey("Without exceeding capacity", func() {
			populateQueue(queue, initCap)
		})
		Convey("Exceeding capacity by 10", func() {
			populateQueue(queue, 10*initCap)
		})
		Convey("Exceeding capacity by 100", func() {
			populateQueue(queue, 100*initCap)
		})
	}))

	Convey("Should keep insertion order without resizes", t, withTimeout(func() {
		numItems := 100
		queue := NewSyncQueue(numItems)

		go populateQueue(queue, numItems)

		for i := 0; i < numItems; i++ {
			So(queue.Dequeue(), ShouldEqual, i)
		}
	}))

	Convey("Should not lose items even with resizes", t, withTimeout(func() {
		numItems := 100
		queue := NewSyncQueue(1)

		Convey("Single sender", func() {
			go populateQueue(queue, numItems)

			Convey("Single receiver", func() {
				received := consumeQueue(queue, numItems)
				So(len(received), ShouldEqual, numItems)
			})
			Convey("Multiple receivers", func() {
				received := concurrentConsumeQueue(queue, numItems)
				So(len(received), ShouldEqual, numItems)
			})
		})

		Convey("Multiple senders", func() {
			go concurrentPopulateQueue(queue, numItems)

			Convey("Single receiver", func() {
				received := consumeQueue(queue, numItems)
				So(len(received), ShouldEqual, numItems)
			})
			Convey("Multiple receivers", func() {
				received := concurrentConsumeQueue(queue, numItems)
				So(len(received), ShouldEqual, numItems)
			})
		})
	}))
}

func withTimeout(f func()) func() {
	return func() {
		done := make(chan struct{})
		defer close(done)

		go func() {
			select {
			case <-done:
			case <-time.After(defaultTimeout):
				panic("Timeout in SyncQueue tests!")
			}
		}()

		f()
	}
}

func populateQueue(queue *SyncQueue, numItems int) {
	for i := 0; i < numItems; i++ {
		queue.Enqueue(i)
	}
}

func concurrentPopulateQueue(queue *SyncQueue, numItems int) {
	for i := 0; i < numItems; i++ {
		go queue.Enqueue(i)
	}
}

func consumeQueue(queue *SyncQueue, numItems int) map[int]bool {
	received := map[int]bool{}
	for i := 0; i < numItems; i++ {
		receivedVal := queue.Dequeue().(int)
		if received[receivedVal] {
			panic("Duplicate value received!")
		}
		received[receivedVal] = true
	}
	return received
}

func concurrentConsumeQueue(queue *SyncQueue, numItems int) map[int]bool {
	receivedChan := make(chan int, numItems)
	wg := sync.WaitGroup{}
	wg.Add(numItems)
	for i := 0; i < numItems; i++ {
		go func() {
			defer wg.Done()
			receivedChan <- queue.Dequeue().(int)
		}()
	}
	wg.Wait()
	close(receivedChan)

	received := map[int]bool{}
	for receivedVal := range receivedChan {
		if received[receivedVal] {
			panic("Duplicate value received!")
		}
		received[receivedVal] = true
	}
	return received
}
