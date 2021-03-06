package sharedflight

import (
	"context"
	"sync"
	"time"
)

type unionContext struct {
	inner  context.Context
	cancel func()

	mu          sync.RWMutex
	subContexts []context.Context
}

type UnionContext interface {
	context.Context
	AddContext(ctx context.Context) bool
}

func NewUnionContext(base context.Context) UnionContext {
	inner, cancel := context.WithCancel(context.Background())
	union := &unionContext{
		inner:       inner,
		cancel:      cancel,
		subContexts: []context.Context{base},
	}
	go union.cancelLoop()
	return union
}

func (u *unionContext) Deadline() (time.Time, bool) {
	return u.inner.Deadline()
}

func (u *unionContext) Done() <-chan struct{} {
	return u.inner.Done()
}

func (u *unionContext) Err() error {
	return u.inner.Err()
}

func (u *unionContext) Value(key interface{}) interface{} {
	u.mu.RLock()
	defer u.mu.RUnlock()
	for _, ctx := range u.subContexts {
		if val := ctx.Value(key); val != nil {
			return val
		}
	}
	return nil
}

func (u *unionContext) AddContext(ctx context.Context) bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.Err() != nil {
		return false
	}
	u.subContexts = append(u.subContexts, ctx)
	return true
}

func (u *unionContext) cancelLoop() {
	for {
		for {
			first := u.getFirstWithLock()
			if first == nil {
				break
			}

			select {
			case <-first.Done():
				u.popFirstWithLock()
			case <-u.Done():
				return
			}
		}
		if u.cancelIfEmpty() {
			return
		}
	}
}

func (u *unionContext) cancelIfEmpty() (cancelled bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if len(u.subContexts) == 0 {
		u.cancel()
		return true
	}
	return false
}

func (u *unionContext) getFirstWithLock() context.Context {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if len(u.subContexts) == 0 {
		return nil
	}
	return u.subContexts[0]
}

func (u *unionContext) popFirstWithLock() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.subContexts) == 0 {
		return
	}
	u.subContexts[0] = nil
	u.subContexts = u.subContexts[1:]
}
