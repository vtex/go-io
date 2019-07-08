package ioext

import (
	"io"
	"net/http"
	"sync"
	"time"
)

type BufIoWriteFlusher interface {
	io.Writer
	Flush() error
}

type WriteFlusher interface {
	io.Writer
	http.Flusher
}

type bufIoWrapper struct {
	WriteFlusher
}

func (b bufIoWrapper) Flush() error {
	b.WriteFlusher.Flush()
	return nil
}

func NewAutoFlusher(wf WriteFlusher, flushLatency time.Duration) *AutoFlusher {
	return NewBufIoAutoFlusher(bufIoWrapper{wf}, flushLatency)
}

func NewBufIoAutoFlusher(wf BufIoWriteFlusher, flushLatency time.Duration) *AutoFlusher {
	af := &AutoFlusher{
		wf:   wf,
		done: make(chan struct{}),
	}
	go af.flushLoop(flushLatency)
	return af
}

type AutoFlusher struct {
	wf   BufIoWriteFlusher
	done chan struct{}

	mu           sync.Mutex
	err          error
	flushPending bool
}

func (af *AutoFlusher) Write(p []byte) (int, error) {
	af.mu.Lock()
	defer af.mu.Unlock()

	if err := af.err; err != nil {
		af.err = nil
		return 0, err
	}
	n, err := af.wf.Write(p)
	if n > 0 {
		af.flushPending = true
	}
	return n, err
}

func (af *AutoFlusher) Stop() {
	af.done <- struct{}{}
	close(af.done)
}

func (af *AutoFlusher) flushLoop(latency time.Duration) {
	ticker := time.NewTicker(latency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			af.mu.Lock()
			if af.flushPending {
				af.err = af.wf.Flush()
				af.flushPending = false
			}
			af.mu.Unlock()
		case <-af.done:
			return
		}
	}
}
