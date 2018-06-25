package ioext

import (
	"io"
	"net/http"
	"sync"
	"time"
)

type WriteFlusher interface {
	io.Writer
	http.Flusher
}

func NewAutoFlusher(wf WriteFlusher, flushLatency time.Duration) *AutoFlusher {
	af := &AutoFlusher{
		wf:   wf,
		done: make(chan struct{}),
	}
	go af.flushLoop(flushLatency)
	return af
}

type AutoFlusher struct {
	wf   WriteFlusher
	done chan struct{}

	mu           sync.Mutex
	flushPending bool
}

func (af *AutoFlusher) Write(p []byte) (int, error) {
	af.mu.Lock()
	defer af.mu.Unlock()

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
				af.wf.Flush()
				af.flushPending = false
			}
			af.mu.Unlock()
		case <-af.done:
			return
		}
	}
}
