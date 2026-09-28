package download

import (
	"sync"
	"time"
)

// Event is a lightweight domain notification. Delivery is best-effort and
// in-memory only: subscribers reload canonical task state from PostgreSQL when
// they need it, so an event that is missed or dropped costs a refresh rather
// than a lost fact.
type Event struct {
	ID        int64     `json:"id"`
	JobID     string    `json:"jobId"`
	RequestID string    `json:"requestId,omitempty"`
	Kind      string    `json:"kind"`
	Status    string    `json:"status,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

type eventBus struct {
	mu   sync.RWMutex
	next uint64
	subs map[uint64]chan Event
}

func newEventBus() *eventBus { return &eventBus{subs: make(map[uint64]chan Event)} }

// Subscribe returns an unsubscribe function. The bounded channel ensures a
// slow presentation consumer cannot stall downloads.
func (b *eventBus) Subscribe() (<-chan Event, func()) {
	b.mu.Lock()
	id := b.next
	b.next++
	ch := make(chan Event, 32)
	b.subs[id] = ch
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		if current, ok := b.subs[id]; ok {
			delete(b.subs, id)
			close(current)
		}
		b.mu.Unlock()
	}
}

func (b *eventBus) publish(event Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs {
		select {
		case ch <- event:
		default:
		}
	}
}

// SubscribeEvents lets delivery adapters (Web SSE, Bot, future Webhooks)
// observe one task-state stream without depending on worker internals.
func (m *Manager) SubscribeEvents() (<-chan Event, func()) { return m.events.Subscribe() }

// emit publishes one task-state notification to the in-memory bus.
//
// It deliberately writes no row. Every state transition used to append to a
// download_events table as well, one insert per item transition, and nothing
// ever read it: both consumers subscribe to this bus, and the table was only
// ever inserted into and trimmed by age. On the table that holds tens of
// millions of rows that was one write and one index entry per transition for
// no reader.
func (m *Manager) emit(jobID, requestID, kind, status string) {
	m.events.publish(Event{JobID: jobID, RequestID: requestID, Kind: kind, Status: status, CreatedAt: time.Now().UTC()})
}
