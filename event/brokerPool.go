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
		brokers: map[string]*poolEntry{},
	}
}

type brokerPool struct {
	factory EventSourceFactory

	lock    sync.RWMutex
	brokers map[string]*poolEntry
}

type poolEntry struct {
	id     string
	broker Broker
	stopFn func()
}

func (p *brokerPool) Subscribe(ctx context.Context, id string, factoryArg interface{}) (EventSource, error) {
	p.lock.RLock()
	if entry, ok := p.brokers[id]; ok {
		defer p.lock.RUnlock()
		return p.subscribeLocked(ctx, entry)
	}
	p.lock.RUnlock()

	entry, err := p.createBroker(id, factoryArg)
	if err != nil {
		return nil, err
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if existing, ok := p.brokers[id]; ok {
		entry.stopFn()
		entry = existing
	} else {
		p.brokers[id] = entry
	}

	return p.subscribeLocked(ctx, entry)
}

func (p *brokerPool) createBroker(id string, factoryArg interface{}) (*poolEntry, error) {
	ctx, cancel := context.WithCancel(context.Background())
	source, err := p.factory(ctx, id, factoryArg)
	if err != nil {
		cancel()
		return nil, err
	}

	broker := NewBroker(ctx, source)
	return &poolEntry{id, broker, cancel}, nil
}

// This function must be called in whilst locked in the broker pool, otherwise
// it could be executed concurrently with the unsubscribe function below, which
// could cause an invalid state to arise. It could be an RLock just fine, as the
// unsubscribe always grab a (W)Lock itself.
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

	if _, ok := p.brokers[entry.id]; !ok {
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
		delete(p.brokers, entry.id)
		entry.stopFn()
	}
}
