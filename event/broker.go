package event

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

// Broker provides a friendly interface to fan-out events to as many subscribers
// registered with it. Notice that an explicit unsubscription is necessary even
// with a context.Context being sent on the subscription call (to be improved).
type Broker interface {
	Subscribe(ctx context.Context) (EventSource, error)
	Unsubscribe(src EventSource) (hasMore bool, err error)
}

type broker struct {
	ctx    context.Context
	source EventSource

	lock    sync.Mutex
	clients []client
}

type client struct {
	C   chan Event
	ctx context.Context
}

func NewBroker(ctx context.Context, source EventSource) Broker {
	b := &broker{
		ctx:     ctx,
		source:  source,
		clients: make([]client, 0, 10),
	}
	go b.sendEventsLoop()
	return b
}

func (b *broker) sendEventsLoop() {
	defer b.unsubscribeAll()

	for {
		select {
		case ev, ok := <-b.source:
			if !ok {
				return
			}
			b.sendToClients(ev)

		case <-b.ctx.Done():
			return
		}
	}
}

func (b *broker) isStopped() bool {
	select {
	case <-b.ctx.Done():
		return true
	default:
		return false
	}
}

func (b *broker) sendToClients(ev Event) {
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, client := range b.clients {
		select {
		case client.C <- ev:
		case <-client.ctx.Done():
			// client done, simply ignore them until they're properly unsubscribed
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *broker) Subscribe(ctx context.Context) (EventSource, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.isStopped() {
		return nil, errors.New("Attempt to subscribe to an already stopped broker")
	}

	ch := make(chan Event, 1)
	b.clients = append(b.clients, client{ch, ctx})

	return ch, nil
}

func (b *broker) Unsubscribe(client EventSource) (hasMore bool, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	idx := findClientIdx(b.clients, client)
	if idx < 0 {
		return len(b.clients) > 0, errors.New("Tried to unsubscribe inexistent client")
	}

	close(b.clients[idx].C)
	b.clients = append(b.clients[:idx], b.clients[idx+1:]...)
	return len(b.clients) > 0, nil
}

func (b *broker) unsubscribeAll() {
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, cl := range b.clients {
		close(cl.C)
	}
	b.clients = nil
}

func findClientIdx(clients []client, elm <-chan Event) int {
	for i, cl := range clients {
		if cl.C == elm {
			return i
		}
	}
	return -1
}
