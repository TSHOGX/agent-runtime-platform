package events

import (
	"testing"
	"time"
)

func TestPublishDoesNotBlockOnSlowSubscribers(t *testing.T) {
	hub := NewHub()
	globalCh, cancelGlobal := hub.Subscribe("")
	defer cancelGlobal()
	sessionCh, cancelSession := hub.Subscribe("sess_slow")
	defer cancelSession()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1_000; i++ {
			hub.Publish(Event{Type: "emit_output", SessionID: "sess_slow"})
		}
	}()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("publish blocked behind unread subscribers")
	}
	if len(globalCh) == 0 || len(sessionCh) == 0 {
		t.Fatalf("expected slow subscribers to receive buffered events, global=%d session=%d", len(globalCh), len(sessionCh))
	}
	if len(globalCh) > 64 || len(sessionCh) > 64 {
		t.Fatalf("subscriber buffers exceeded capacity, global=%d session=%d", len(globalCh), len(sessionCh))
	}
}
