package runtime

import (
	"sync"
)

// OutputEvent represents a single line of output from a container stream.
type OutputEvent struct {
	Stream string // "stdout", "stderr", or "runtime"
	Line   string
}

// OutputHub is a pub/sub hub for container output events.
// It allows multiple subscribers to independently consume output from a single container.
// This decouples output producers (scanLines goroutines) from consumers (stream parsers).
type OutputHub struct {
	mu          sync.RWMutex
	subscribers map[chan OutputEvent]struct{}
	closed      bool
}

// NewOutputHub creates a new OutputHub.
func NewOutputHub() *OutputHub {
	return &OutputHub{
		subscribers: make(map[chan OutputEvent]struct{}),
	}
}

// Subscribe creates a new subscription to this hub.
// Returns a channel that will receive output events and a cancel function.
// The caller MUST call the cancel function when done to avoid goroutine leaks.
func (h *OutputHub) Subscribe() (<-chan OutputEvent, func()) {
	ch := make(chan OutputEvent, 64) // buffered to avoid blocking publishers

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		close(ch)
		return ch, func() {}
	}

	h.subscribers[ch] = struct{}{}

	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, exists := h.subscribers[ch]; exists {
			delete(h.subscribers, ch)
			close(ch)
		}
	}

	return ch, cancel
}

// Publish sends an event to all subscribers.
// Uses non-blocking send to avoid slow consumers blocking the publisher.
// If a subscriber's channel is full, the event is dropped for that subscriber.
func (h *OutputHub) Publish(event OutputEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return
	}

	for ch := range h.subscribers {
		select {
		case ch <- event:
			// Event sent successfully
		default:
			// Channel full, drop event for this subscriber to avoid blocking
		}
	}
}

// Close closes the hub and all subscriber channels.
// After Close, no more events can be published and Subscribe will return a closed channel.
func (h *OutputHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}

	h.closed = true
	for ch := range h.subscribers {
		close(ch)
		delete(h.subscribers, ch)
	}
}
