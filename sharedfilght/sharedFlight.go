package sharedflight

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"
)

type Group struct {
	mu           sync.RWMutex
	contexts     map[interface{}]UnionContext
	singleFlight singleflight.Group
}

func (g *Group) Do(key string, ctx context.Context, fn func(context.Context) (interface{}, error)) (v interface{}, err error, shared bool) {
	sharedCtx := g.getSharedContext(key, ctx)
	return g.singleFlight.Do(key, func() (interface{}, error) {
		return fn(sharedCtx)
	})
}

func (g *Group) getSharedContext(key interface{}, base context.Context) context.Context {
	g.mu.RLock()
	sharedCtx, ok := g.contexts[key]
	g.mu.RUnlock()
	if ok && sharedCtx.AddContext(base) {
		return sharedCtx
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.contexts == nil {
		g.contexts = map[interface{}]UnionContext{}
	}

	sharedCtx, ok = g.contexts[key]
	if ok && sharedCtx.AddContext(base) {
		return sharedCtx
	}

	newCtx := NewUnionContext(base)
	g.contexts[key] = newCtx
	go func() {
		<-newCtx.Done()
		g.mu.Lock()
		if sharedCtx == g.contexts[key] {
			delete(g.contexts, key)
		}
		g.mu.Unlock()
	}()
	return newCtx
}
