package runtime

import (
	"sync"
)

const outputHubBufferSize = 64

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
	subscribers map[*outputSubscriber]struct{}
	closed      bool
	done        chan struct{}
	closeOnce   sync.Once
}

type outputSubscriber struct {
	ch   chan OutputEvent
	done chan struct{}
	once sync.Once
}

func newOutputSubscriber() *outputSubscriber {
	return &outputSubscriber{
		ch:   make(chan OutputEvent, outputHubBufferSize),
		done: make(chan struct{}),
	}
}

func (s *outputSubscriber) signalDone() {
	s.once.Do(func() { close(s.done) })
}

// NewOutputHub creates a new OutputHub.
func NewOutputHub() *OutputHub {
	return &OutputHub{
		subscribers: make(map[*outputSubscriber]struct{}),
		done:        make(chan struct{}),
	}
}

// Subscribe creates a new subscription to this hub.
// Returns a channel that will receive output events and a cancel function.
// The caller MUST call the cancel function when done to avoid goroutine leaks.
func (h *OutputHub) Subscribe() (<-chan OutputEvent, func()) {
	sub := newOutputSubscriber()

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		sub.signalDone()
		close(sub.ch)
		return sub.ch, func() {}
	}

	h.subscribers[sub] = struct{}{}

	cancel := func() {
		sub.signalDone()

		h.mu.Lock()
		defer h.mu.Unlock()
		if _, exists := h.subscribers[sub]; exists {
			delete(h.subscribers, sub)
			close(sub.ch)
		}
	}

	return sub.ch, cancel
}

// Publish sends an event to all subscribers.
// Publish applies backpressure when a subscriber falls behind. Runtime output is
// part of the turn protocol, so silently dropping frames can prevent the parser
// from seeing completion events.
func (h *OutputHub) Publish(event OutputEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return
	}

	for sub := range h.subscribers {
		select {
		case sub.ch <- event:
		case <-sub.done:
		case <-h.done:
			return
		}
	}
}

// Close closes the hub and all subscriber channels.
// After Close, no more events can be published and Subscribe will return a closed channel.
func (h *OutputHub) Close() {
	h.closeOnce.Do(func() {
		close(h.done)

		h.mu.Lock()
		defer h.mu.Unlock()

		h.closed = true
		for sub := range h.subscribers {
			sub.signalDone()
			close(sub.ch)
			delete(h.subscribers, sub)
		}
	})
}
