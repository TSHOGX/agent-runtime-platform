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

func TestOutputHub_BackpressuresWhenSubscriberIsFull(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	ch, cancel := hub.Subscribe()
	defer cancel()

	// Fill the channel buffer.
	for i := 0; i < outputHubBufferSize; i++ {
		hub.Publish(OutputEvent{Stream: "stdout", Line: "fill"})
	}

	done := make(chan struct{})
	go func() {
		hub.Publish(OutputEvent{Stream: "stdout", Line: "overflow"})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("publish returned before subscriber made room")
	case <-time.After(20 * time.Millisecond):
		// Expected: publish backpressures until a subscriber drains one event.
	}

	<-ch
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("publish did not unblock after subscriber made room")
	}

	for i := 0; i < outputHubBufferSize-1; i++ {
		event := <-ch
		if event.Line != "fill" {
			t.Fatalf("expected fill event, got %+v", event)
		}
	}
	event := <-ch
	if event.Line != "overflow" {
		t.Fatalf("expected overflow event to be delivered, got %+v", event)
	}
}

func TestOutputHub_CancelUnblocksBlockedPublish(t *testing.T) {
	hub := NewOutputHub()
	defer hub.Close()

	_, cancel := hub.Subscribe()

	for i := 0; i < outputHubBufferSize; i++ {
		hub.Publish(OutputEvent{Stream: "stdout", Line: "fill"})
	}

	publishDone := make(chan struct{})
	go func() {
		hub.Publish(OutputEvent{Stream: "stdout", Line: "overflow"})
		close(publishDone)
	}()

	select {
	case <-publishDone:
		t.Fatal("publish returned before subscriber made room")
	case <-time.After(20 * time.Millisecond):
	}

	cancelDone := make(chan struct{})
	go func() {
		cancel()
		close(cancelDone)
	}()

	select {
	case <-publishDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cancel did not unblock blocked publish")
	}
	select {
	case <-cancelDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cancel did not return")
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
	eventsPerPublisher := 10

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
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		receivedMu.Lock()
		finalReceived := received
		receivedMu.Unlock()
		t.Fatalf("expected %d events, got %d", numPublishers*eventsPerPublisher, finalReceived)
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
