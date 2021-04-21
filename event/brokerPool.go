package event

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

type EventSourceFactory func(ctx context.Context, id string, arg interface{}) (EventSource, error)

// BrokerPool manages a group of brokers which can be accessed and retrieved via an ID.
// It also simplifies the access to the brokers themselves, providing a single Subscribe
// function which automatically makes the unsubscription when the context is cancelled.
//
// The EventSourceFactory passed as argument to the constructor should create a new
// EventSource channel, using the optional factoryArg that is passed on subscription.
//
// In the simplest case, for example for messaging events between internal application
// components, there could be a single set of EventSources and the factory would grab them
// by their ID and return otherwise return an error for inexistent event source. In more
// complex scenarios, these event sources could be actual subscriptions to some remote
// messaging service for example Redis or RabbitMQ.
type BrokerPool interface {
	Subscribe(ctx context.Context, id string, factoryArg interface{}) (EventSource, error)
}

func NewPool(factory EventSourceFactory) BrokerPool {
	return &brokerPool{
		factory: factory,
	}
}

type brokerPool struct {
	factory EventSourceFactory

	lock    sync.RWMutex
	brokers sync.Map
}

type poolEntry struct {
	id string

	initOnce sync.Once
	initErr  error
	broker   Broker
	stopFn   func()
}

func (e *poolEntry) init(factory EventSourceFactory, factoryArg interface{}) (bool, error) {
	var inited bool
	e.initOnce.Do(func() {
		inited = true
		ctx, cancel := context.WithCancel(context.Background())
		source, err := factory(ctx, e.id, factoryArg)
		if err != nil {
			cancel()
			e.initErr = err
			return
		}

		e.broker = NewBroker(ctx, source)
		e.stopFn = cancel
	})
	return inited, e.initErr
}

func (p *brokerPool) Subscribe(ctx context.Context, id string, factoryArg interface{}) (EventSource, error) {
	p.lock.RLock()
	entryIface, _ := p.brokers.LoadOrStore(id, &poolEntry{id: id})
	entry := entryIface.(*poolEntry)
	if entry.broker != nil {
		defer p.lock.RUnlock()
		return p.subscribeLocked(ctx, entry)
	}
	p.lock.RUnlock()

	inited, err := entry.init(p.factory, factoryArg)
	if err != nil {
		if inited {
			// Only the routine that actually did the init removes the broker from the pool
			// to avoid any race conditions with concurrent Deletes and LoadOrStores.
			p.brokers.Delete(id)
		}
		return nil, err
	}
	p.lock.RLock()
	defer p.lock.RLock()
	return p.subscribeLocked(ctx, entry)
}

// This function must grab a lock, otherwise it could be executed concurrently
// with the unsubscribe function below which could cause an invalid state to
// arise. It can be an RLock since it doesn't modify the pool, and the
// unsubscribe always grabs a (W)Lock itself.
//
// e.g. If not locked, unsubscribe could stop and remove the broker from the
// pool right before this one attempts to create a subscription.
func (p *brokerPool) subscribeLocked(ctx context.Context, entry *poolEntry) (EventSource, error) {
	sub, err := entry.broker.Subscribe(ctx)
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		p.unsubscribe(entry, sub)
	}()
	return sub, nil
}

func (p *brokerPool) unsubscribe(entry *poolEntry, sub EventSource) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if _, ok := p.brokers.Load(entry.id); !ok {
		logrus.WithFields(logrus.Fields{
			"code":   "broker_not_found_err",
			"subsId": entry.id,
		}).Error("Unsubscribing from broker not in pool anymore")
	}

	hasMore, err := entry.broker.Unsubscribe(sub)
	if err != nil {
		logrus.WithError(err).
			WithFields(logrus.Fields{
				"code":   "broker_unsubscribe_err",
				"subsId": entry.id,
			}).
			Error("Thought valid subscription not found in origin broker")
	}
	if !hasMore {
		p.brokers.Delete(entry.id)
		entry.stopFn()
	}
}
