package events

import (
	"sync"
	"time"
)

type Event struct {
	EventID        int64     `json:"event_id,omitempty"`
	Type           string    `json:"type"`
	SessionID      string    `json:"session_id,omitempty"`
	TurnID         *int64    `json:"turn_id,omitempty"`
	GenerationID   string    `json:"generation_id,omitempty"`
	OutputSequence *int64    `json:"output_sequence,omitempty"`
	ProxyRequestID string    `json:"proxy_request_id,omitempty"`
	Stream         string    `json:"stream,omitempty"`
	Severity       string    `json:"severity,omitempty"`
	Time           time.Time `json:"time"`
	Payload        any       `json:"payload,omitempty"`
}

type Hub struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan Event]struct{}
}

func NewHub() *Hub {
	return &Hub{subscribers: make(map[string]map[chan Event]struct{})}
}

func (h *Hub) Subscribe(sessionID string) (<-chan Event, func()) {
	ch := make(chan Event, 64)
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.subscribers[sessionID] == nil {
		h.subscribers[sessionID] = make(map[chan Event]struct{})
	}
	h.subscribers[sessionID][ch] = struct{}{}

	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if subscribers := h.subscribers[sessionID]; subscribers != nil {
			delete(subscribers, ch)
			if len(subscribers) == 0 {
				delete(h.subscribers, sessionID)
			}
		}
		close(ch)
	}
	return ch, cancel
}

func (h *Hub) Publish(event Event) {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	h.publishLocked("", event)
	if event.SessionID != "" {
		h.publishLocked(event.SessionID, event)
	}
}

func (h *Hub) publishLocked(key string, event Event) {
	for ch := range h.subscribers[key] {
		select {
		case ch <- event:
		default:
		}
	}
}
