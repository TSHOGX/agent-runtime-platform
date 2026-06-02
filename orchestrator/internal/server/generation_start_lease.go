package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"harness-platform/orchestrator/internal/store"
)

var errGenerationStartLeaseLost = errors.New("generation start lease lost")

type startLeaseKeeper struct {
	cancel context.CancelFunc
	done   chan struct{}
	renew  func() error

	mu      sync.Mutex
	failure error
}

func noopStartLeaseKeeper() *startLeaseKeeper {
	done := make(chan struct{})
	close(done)
	return &startLeaseKeeper{
		cancel: func() {},
		done:   done,
		renew:  func() error { return nil },
	}
}

func (s *Server) beginGenerationStartLease(ctx context.Context, sessionID, generationID, owner string) (context.Context, *startLeaseKeeper, error) {
	startCtx, cancel := context.WithCancel(ctx)
	ttl := s.cfg.Harness.Bridge.LeaseTTL.Duration
	keeper := &startLeaseKeeper{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	keeper.renew = func() error {
		renewCtx, renewCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer renewCancel()
		return s.store.RenewGenerationStartLease(renewCtx, store.RenewGenerationStartLeaseParams{
			SessionID:    sessionID,
			GenerationID: generationID,
			Owner:        owner,
			LeaseTTL:     ttl,
			Now:          time.Now().UTC(),
		})
	}
	if err := keeper.ensureOwned(); err != nil {
		cancel()
		close(keeper.done)
		return startCtx, keeper, err
	}
	go func() {
		defer close(keeper.done)
		ticker := time.NewTicker(startLeaseRenewalInterval(ttl))
		defer ticker.Stop()
		for {
			select {
			case <-startCtx.Done():
				return
			case <-ticker.C:
				if err := keeper.ensureOwned(); err != nil {
					return
				}
			}
		}
	}()
	return startCtx, keeper, nil
}

func startLeaseRenewalInterval(ttl time.Duration) time.Duration {
	interval := ttl / 2
	if interval < time.Millisecond {
		return time.Millisecond
	}
	return interval
}

func (k *startLeaseKeeper) ensureOwned() error {
	if err := k.getErr(); err != nil {
		return err
	}
	if k.renew == nil {
		return nil
	}
	if err := k.renew(); err != nil {
		wrapped := fmt.Errorf("%w: %v", errGenerationStartLeaseLost, err)
		k.setErr(wrapped)
		return wrapped
	}
	return k.getErr()
}

func (k *startLeaseKeeper) err() error {
	return k.getErr()
}

func (k *startLeaseKeeper) getErr() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.failure
}

func (k *startLeaseKeeper) setErr(err error) {
	if err == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.failure != nil {
		return
	}
	k.failure = err
	k.cancel()
}

func (k *startLeaseKeeper) stop() {
	k.cancel()
	<-k.done
}
