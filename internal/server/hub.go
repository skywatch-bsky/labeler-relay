// pattern: Imperative Shell
// Hub manages live fan-out of LiveEvents to subscribers. It is the Imperative
// Shell for broadcast coordination — all subscriber state mutation and channel
// sends happen here.

package server

import (
	"sync"

	"github.com/scarndp/labeler-relay/internal/store"
)

type hubSub struct {
	ch chan store.LiveEvent
}

// Hub fans out live store.LiveEvents to subscribed consumers. It is registered
// as the broadcaster via LabelPersist.SetBroadcaster and therefore Broadcast
// MUST be non-blocking: it is called while LabelPersist holds its persist mutex.
type Hub struct {
	mu   sync.Mutex
	subs map[int]*hubSub
	next int
}

// NewHub creates an empty Hub ready for subscriptions.
func NewHub() *Hub {
	return &Hub{
		subs: make(map[int]*hubSub),
	}
}

// Broadcast delivers e to every subscriber. Non-blocking per subscriber:
// if a subscriber's buffer is full the subscriber is dropped — its channel is
// closed and removed. This guarantees the broadcast never stalls the write path
// regardless of how many slow consumers are registered.
//
// Broadcast is called by LabelPersist while it holds the persist mutex, so it
// must return immediately.
func (h *Hub) Broadcast(e store.LiveEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for id, sub := range h.subs {
		select {
		case sub.ch <- e:
			// delivered
		default:
			// subscriber buffer full — drop it: close channel, remove entry
			close(sub.ch)
			delete(h.subs, id)
		}
	}
}

// Subscribe returns a buffered channel of live events and a cleanup func.
// bufSize bounds the per-subscriber backlog; if Broadcast finds the buffer
// full the subscriber is dropped (channel closed, entry removed).
// The cleanup func removes the subscription and closes the channel if it has
// not already been closed by a drop.
func (h *Hub) Subscribe(bufSize int) (<-chan store.LiveEvent, func()) {
	h.mu.Lock()
	id := h.next
	h.next++
	sub := &hubSub{
		ch: make(chan store.LiveEvent, bufSize),
	}
	h.subs[id] = sub
	h.mu.Unlock()

	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[id]; ok {
			close(sub.ch)
			delete(h.subs, id)
		}
	}

	return sub.ch, cancel
}
