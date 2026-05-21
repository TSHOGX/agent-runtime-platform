package runtime

import (
	"sync"
	"testing"
	"time"
)

func TestOutputHub_SingleSubscriber(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	ch, cancel := hub.Subscribe()
	defer cancel()

	// Publish an event
	hub.Publish(OutputEvent{Stream: "stdout", Line: "hello"})

	// Subscriber should receive it
	select {
	case event := <-ch:
		if event.Stream != "stdout" || event.Line != "hello" {
			t.Errorf("expected stdout/hello, got %s/%s", event.Stream, event.Line)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for event")
	}
}

func TestOutputHub_MultipleSubscribers(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	// Create 3 subscribers
	ch1, cancel1 := hub.Subscribe()
	defer cancel1()
	ch2, cancel2 := hub.Subscribe()
	defer cancel2()
	ch3, cancel3 := hub.Subscribe()
	defer cancel3()

	// Publish an event
	hub.Publish(OutputEvent{Stream: "stderr", Line: "error"})

	// All subscribers should receive it
	for i, ch := range []<-chan OutputEvent{ch1, ch2, ch3} {
		select {
		case event := <-ch:
			if event.Stream != "stderr" || event.Line != "error" {
				t.Errorf("subscriber %d: expected stderr/error, got %s/%s", i, event.Stream, event.Line)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("subscriber %d: timeout waiting for event", i)
		}
	}
}

func TestOutputHub_CancelSubscription(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	ch, cancel := hub.Subscribe()

	// Cancel subscription
	cancel()

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for channel close")
	}

	// Publishing after cancel should not panic
	hub.Publish(OutputEvent{Stream: "stdout", Line: "test"})
}

func TestOutputHub_PublishAfterClose(t *testing.T) {
	hub := NewOutputHub()

	ch, cancel := hub.Subscribe()
	defer cancel()

	// Close hub
	hub.Close()

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after hub close")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for channel close")
	}

	// Publishing after close should not panic
	hub.Publish(OutputEvent{Stream: "stdout", Line: "test"})
}

func TestOutputHub_SubscribeAfterClose(t *testing.T) {
	hub := NewOutputHub()
	hub.Close()

	// Subscribe after close should return closed channel
	ch, cancel := hub.Subscribe()
	defer cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for channel close")
	}
}

func TestOutputHub_NonBlockingPublish(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	ch, cancel := hub.Subscribe()
	defer cancel()

	// Fill the channel buffer (64 events)
	for i := 0; i < 64; i++ {
		hub.Publish(OutputEvent{Stream: "stdout", Line: "fill"})
	}

	// Publish one more event - should not block even though channel is full
	done := make(chan bool)
	go func() {
		hub.Publish(OutputEvent{Stream: "stdout", Line: "overflow"})
		done <- true
	}()

	select {
	case <-done:
		// Success - publish did not block
	case <-time.After(100 * time.Millisecond):
		t.Fatal("publish blocked on full channel")
	}

	// Drain the channel
	count := 0
	for {
		select {
		case <-ch:
			count++
		case <-time.After(10 * time.Millisecond):
			// No more events
			if count != 64 {
				t.Errorf("expected 64 events (overflow dropped), got %d", count)
			}
			return
		}
	}
}

func TestOutputHub_ConcurrentPublish(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	ch, cancel := hub.Subscribe()
	defer cancel()

	// Publish from multiple goroutines concurrently
	var wg sync.WaitGroup
	numPublishers := 10
	eventsPerPublisher := 10 // Reduced to avoid overwhelming the buffer

	// Consume events in background with small delay to simulate processing
	var receivedMu sync.Mutex
	received := 0
	done := make(chan bool)
	go func() {
		for range ch {
			receivedMu.Lock()
			received++
			current := received
			receivedMu.Unlock()

			if current >= numPublishers*eventsPerPublisher {
				done <- true
				return
			}
		}
	}()

	// Small delay to ensure consumer is ready
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < eventsPerPublisher; j++ {
				hub.Publish(OutputEvent{Stream: "stdout", Line: "test"})
				// Small delay to avoid overwhelming the buffer
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	select {
	case <-done:
		// Success - all events received
	case <-time.After(1 * time.Second):
		receivedMu.Lock()
		finalReceived := received
		receivedMu.Unlock()
		// With non-blocking publish, some events may be dropped if buffer is full
		// This is expected behavior, so we just verify we got most events
		if finalReceived < numPublishers*eventsPerPublisher/2 {
			t.Fatalf("too many events dropped: expected ~%d events, got %d", numPublishers*eventsPerPublisher, finalReceived)
		}
		t.Logf("received %d/%d events (some dropped due to non-blocking publish)", finalReceived, numPublishers*eventsPerPublisher)
	}
}

func TestOutputHub_ConcurrentSubscribe(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	// Subscribe from multiple goroutines concurrently
	var wg sync.WaitGroup
	numSubscribers := 10

	for i := 0; i < numSubscribers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, cancel := hub.Subscribe()
			defer cancel()

			// Receive one event
			select {
			case <-ch:
				// Success
			case <-time.After(1 * time.Second):
				t.Error("timeout waiting for event")
			}
		}()
	}

	// Give subscribers time to register
	time.Sleep(10 * time.Millisecond)

	// Publish an event
	hub.Publish(OutputEvent{Stream: "stdout", Line: "test"})

	wg.Wait()
}
