package download

import (
	"sync"
	"time"

	"github.com/vacks/tdl/internal/applog"
)

// Event is a lightweight domain notification. Persistent history lives in
// download_events; subscribers are best-effort and always reload canonical
// task state from PostgreSQL when necessary.
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

func (m *Manager) emit(jobID, requestID, kind, status string) {
	now := time.Now().UTC()
	event := Event{JobID: jobID, RequestID: requestID, Kind: kind, Status: status, CreatedAt: now}
	_, err := m.db.Exec(`INSERT INTO download_events(job_id, request_id, kind, status, created_at) VALUES (?, ?, ?, ?, ?)`, jobID, requestID, kind, status, now.Format(time.RFC3339Nano))
	if err != nil {
		// Delivery adapters need the state wake-up even if the historical audit
		// row cannot be written. Canonical state remains in the task tables.
		applog.Error("download", "event_record_failed", "job_id", jobID, "kind", kind, "error", err.Error())
		m.events.publish(event)
		return
	}
	m.events.publish(event)
}
