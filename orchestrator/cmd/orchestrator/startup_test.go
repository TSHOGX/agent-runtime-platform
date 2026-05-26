package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/store"
)

func TestRunStartupRuntimeRecoveryOrdersExpiryRuntimeRepairAndReaping(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 27, 10, 0, 0, 0, time.UTC)
	cfg := config.Config{
		SessionRetention: 0,
		Phase7: config.Phase7Config{
			Reaper: config.ReaperConfig{
				FailedRetention: config.Duration{Duration: 17 * time.Second},
			},
		},
	}
	var order []string

	result, err := runStartupRuntimeRecovery(ctx, cfg, "owner-1", startupRuntimeRecoveryHooks{
		Now: func() time.Time { return now },
		ClearActiveSessionExpiry: func(_ context.Context, got time.Time) (int64, error) {
			if !got.Equal(now) {
				t.Fatalf("clear time=%s want %s", got, now)
			}
			order = append(order, "clear")
			return 2, nil
		},
		ConstructRuntime: func() error {
			order = append(order, "runtime")
			return nil
		},
		RecoverExpiredRuntimeResources: func(_ context.Context, got time.Time) (store.StartupRecoveryResult, error) {
			if !reflect.DeepEqual(order, []string{"clear", "runtime"}) {
				t.Fatalf("recovery observed startup order %v", order)
			}
			if !got.Equal(now) {
				t.Fatalf("recover time=%s want %s", got, now)
			}
			order = append(order, "recover")
			return store.StartupRecoveryResult{ExpiredLifecycleFailed: 1}, nil
		},
		ReapResources: func(_ context.Context, p store.ReaperParams) (store.ReaperResult, error) {
			if !reflect.DeepEqual(order, []string{"clear", "runtime", "recover"}) {
				t.Fatalf("reaper observed startup order %v", order)
			}
			if p.OwnerUUID != "owner-1" || p.FailedRetention != 17*time.Second || !p.Now.Equal(now) {
				t.Fatalf("unexpected reaper params: %+v", p)
			}
			order = append(order, "reap")
			return store.ReaperResult{FailedMarkedReclaimable: 3}, nil
		},
		DestroyReclaimableGenerationResources: func(_ context.Context, got time.Time) {
			if !reflect.DeepEqual(order, []string{"clear", "runtime", "recover", "reap"}) {
				t.Fatalf("destroy observed startup order %v", order)
			}
			if !got.Equal(now) {
				t.Fatalf("destroy time=%s want %s", got, now)
			}
			order = append(order, "destroy")
		},
	})
	if err != nil {
		t.Fatalf("run startup runtime recovery: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"clear", "runtime", "recover", "reap", "destroy"}) {
		t.Fatalf("unexpected startup order: %v", order)
	}
	if result.ClearedActiveSessionExpiries != 2 ||
		result.Recovery.ExpiredLifecycleFailed != 1 ||
		result.Reaped.FailedMarkedReclaimable != 3 {
		t.Fatalf("unexpected startup result: %+v", result)
	}
}

func TestRunStartupRuntimeRecoverySkipsExpiryClearWhenRetentionEnabled(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{SessionRetention: time.Hour}
	var order []string

	_, err := runStartupRuntimeRecovery(ctx, cfg, "owner-1", startupRuntimeRecoveryHooks{
		Now: func() time.Time { return time.Now().UTC() },
		ClearActiveSessionExpiry: func(context.Context, time.Time) (int64, error) {
			t.Fatalf("clear should not run when session retention is enabled")
			return 0, nil
		},
		ConstructRuntime: func() error {
			order = append(order, "runtime")
			return nil
		},
		RecoverExpiredRuntimeResources: func(context.Context, time.Time) (store.StartupRecoveryResult, error) {
			order = append(order, "recover")
			return store.StartupRecoveryResult{}, nil
		},
		ReapResources: func(context.Context, store.ReaperParams) (store.ReaperResult, error) {
			order = append(order, "reap")
			return store.ReaperResult{}, nil
		},
		DestroyReclaimableGenerationResources: func(context.Context, time.Time) {
			order = append(order, "destroy")
		},
	})
	if err != nil {
		t.Fatalf("run startup runtime recovery: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"runtime", "recover", "reap", "destroy"}) {
		t.Fatalf("unexpected startup order: %v", order)
	}
}
