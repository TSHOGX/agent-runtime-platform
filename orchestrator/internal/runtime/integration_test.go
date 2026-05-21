package runtime

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMultiTurnConversation tests that multiple messages can be sent to the same container
// and each message's output is correctly routed to its respective callback.
// This is the core test for the OutputHub architecture fix.
func TestMultiTurnConversation(t *testing.T) {
	// Skip if not in integration test mode (requires actual container runtime)
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// This test simulates the multi-turn conversation scenario without requiring
	// actual container infrastructure. It validates the OutputHub routing logic.

	hub := NewOutputHub()
	defer hub.Close()

	// Simulate first message
	var firstOutput []string
	var firstMu sync.Mutex
	firstDone := make(chan bool)

	outputCh1, cancel1 := hub.Subscribe()
	go func() {
		defer cancel1()
		for event := range outputCh1 {
			firstMu.Lock()
			firstOutput = append(firstOutput, event.Line)
			firstMu.Unlock()
			if event.Line == "first_done" {
				firstDone <- true
				return
			}
		}
	}()

	// Publish first message output
	hub.Publish(OutputEvent{Stream: "stdout", Line: "response to first message"})
	hub.Publish(OutputEvent{Stream: "stdout", Line: "first_done"})

	select {
	case <-firstDone:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for first message")
	}

	// Verify first message output
	firstMu.Lock()
	if len(firstOutput) != 2 {
		t.Errorf("expected 2 lines for first message, got %d", len(firstOutput))
	}
	if firstOutput[0] != "response to first message" {
		t.Errorf("unexpected first line: %s", firstOutput[0])
	}
	firstMu.Unlock()

	// Simulate second message (this is where the old architecture would fail)
	var secondOutput []string
	var secondMu sync.Mutex
	secondDone := make(chan bool)

	outputCh2, cancel2 := hub.Subscribe()
	go func() {
		defer cancel2()
		for event := range outputCh2 {
			secondMu.Lock()
			secondOutput = append(secondOutput, event.Line)
			secondMu.Unlock()
			if event.Line == "second_done" {
				secondDone <- true
				return
			}
		}
	}()

	// Publish second message output
	hub.Publish(OutputEvent{Stream: "stdout", Line: "response to second message"})
	hub.Publish(OutputEvent{Stream: "stdout", Line: "second_done"})

	select {
	case <-secondDone:
		// Success - this proves the fix works!
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for second message - OutputHub routing failed")
	}

	// Verify second message output
	secondMu.Lock()
	if len(secondOutput) != 2 {
		t.Errorf("expected 2 lines for second message, got %d", len(secondOutput))
	}
	if secondOutput[0] != "response to second message" {
		t.Errorf("unexpected first line: %s", secondOutput[0])
	}
	secondMu.Unlock()

	// Simulate third message to ensure it continues working
	var thirdOutput []string
	var thirdMu sync.Mutex
	thirdDone := make(chan bool)

	outputCh3, cancel3 := hub.Subscribe()
	go func() {
		defer cancel3()
		for event := range outputCh3 {
			thirdMu.Lock()
			thirdOutput = append(thirdOutput, event.Line)
			thirdMu.Unlock()
			if event.Line == "third_done" {
				thirdDone <- true
				return
			}
		}
	}()

	// Publish third message output
	hub.Publish(OutputEvent{Stream: "stdout", Line: "response to third message"})
	hub.Publish(OutputEvent{Stream: "stdout", Line: "third_done"})

	select {
	case <-thirdDone:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for third message")
	}

	// Verify third message output
	thirdMu.Lock()
	if len(thirdOutput) != 2 {
		t.Errorf("expected 2 lines for third message, got %d", len(thirdOutput))
	}
	if thirdOutput[0] != "response to third message" {
		t.Errorf("unexpected first line: %s", thirdOutput[0])
	}
	thirdMu.Unlock()
}

// TestSystemStatusSeparation tests that runtime stream messages are properly
// separated from stdout/stderr messages.
func TestSystemStatusSeparation(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	var outputs []OutputEvent
	var mu sync.Mutex

	outputCh, cancel := hub.Subscribe()
	defer cancel()

	done := make(chan bool)
	go func() {
		for event := range outputCh {
			mu.Lock()
			outputs = append(outputs, event)
			if len(outputs) >= 4 {
				mu.Unlock()
				done <- true
				return
			}
			mu.Unlock()
		}
	}()

	// Publish different stream types
	hub.Publish(OutputEvent{Stream: "runtime", Line: "starting fresh container"})
	hub.Publish(OutputEvent{Stream: "stdout", Line: "assistant response"})
	hub.Publish(OutputEvent{Stream: "stderr", Line: "debug log"})
	hub.Publish(OutputEvent{Stream: "runtime", Line: "resuming from checkpoint"})

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for events")
	}

	// Verify stream types are preserved
	mu.Lock()
	defer mu.Unlock()

	if len(outputs) != 4 {
		t.Fatalf("expected 4 events, got %d", len(outputs))
	}

	// Check runtime stream events
	if outputs[0].Stream != "runtime" || !strings.Contains(outputs[0].Line, "starting fresh") {
		t.Errorf("expected runtime stream for first event, got %s: %s", outputs[0].Stream, outputs[0].Line)
	}

	// Check stdout event
	if outputs[1].Stream != "stdout" || outputs[1].Line != "assistant response" {
		t.Errorf("expected stdout stream for second event, got %s: %s", outputs[1].Stream, outputs[1].Line)
	}

	// Check stderr event
	if outputs[2].Stream != "stderr" || outputs[2].Line != "debug log" {
		t.Errorf("expected stderr stream for third event, got %s: %s", outputs[2].Stream, outputs[2].Line)
	}

	// Check second runtime event
	if outputs[3].Stream != "runtime" || !strings.Contains(outputs[3].Line, "resuming from checkpoint") {
		t.Errorf("expected runtime stream for fourth event, got %s: %s", outputs[3].Stream, outputs[3].Line)
	}
}

// TestOutputHubNoLeaks tests that subscriptions are properly cleaned up
// and don't leak goroutines or channels.
func TestOutputHubNoLeaks(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	// Create and cancel many subscriptions
	for i := 0; i < 100; i++ {
		ch, cancel := hub.Subscribe()
		// Immediately cancel without consuming
		cancel()

		// Verify channel is closed
		select {
		case _, ok := <-ch:
			if ok {
				t.Fatal("channel should be closed after cancel")
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for channel close")
		}
	}

	// Hub should still work after all those cancellations
	ch, cancel := hub.Subscribe()
	defer cancel()

	hub.Publish(OutputEvent{Stream: "stdout", Line: "test"})

	select {
	case event := <-ch:
		if event.Line != "test" {
			t.Errorf("expected 'test', got '%s'", event.Line)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout receiving event after many cancellations")
	}
}
