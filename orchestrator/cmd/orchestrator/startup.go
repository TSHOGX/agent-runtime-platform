package main

import (
	"context"
	"fmt"
	"time"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/store"
)

type startupRuntimeRecoveryHooks struct {
	Now                                   func() time.Time
	ClearActiveSessionExpiry              func(context.Context, time.Time) (int64, error)
	ConstructRuntime                      func() error
	RecoverExpiredRuntimeResources        func(context.Context, time.Time) (store.StartupRecoveryResult, error)
	ReapResources                         func(context.Context, store.ReaperParams) (store.ReaperResult, error)
	DestroyReclaimableGenerationResources func(context.Context, time.Time)
}

type startupRuntimeRecoveryResult struct {
	ClearedActiveSessionExpiries int64
	Recovery                     store.StartupRecoveryResult
	Reaped                       store.ReaperResult
}

func runStartupRuntimeRecovery(ctx context.Context, cfg config.Config, ownerUUID string, hooks startupRuntimeRecoveryHooks) (startupRuntimeRecoveryResult, error) {
	now := hooks.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	var result startupRuntimeRecoveryResult
	if cfg.SessionRetention == 0 {
		if hooks.ClearActiveSessionExpiry == nil {
			return result, fmt.Errorf("clear active session expiry hook is required")
		}
		cleared, err := hooks.ClearActiveSessionExpiry(ctx, now())
		if err != nil {
			return result, fmt.Errorf("clear active session expiry: %w", err)
		}
		result.ClearedActiveSessionExpiries = cleared
	}
	if hooks.ConstructRuntime == nil {
		return result, fmt.Errorf("construct runtime hook is required")
	}
	if err := hooks.ConstructRuntime(); err != nil {
		return result, fmt.Errorf("construct runtime: %w", err)
	}
	if hooks.RecoverExpiredRuntimeResources == nil {
		return result, fmt.Errorf("recover expired runtime resources hook is required")
	}
	recovered, err := hooks.RecoverExpiredRuntimeResources(ctx, now())
	if err != nil {
		return result, fmt.Errorf("recover expired runtime resources: %w", err)
	}
	result.Recovery = recovered
	if hooks.ReapResources == nil {
		return result, fmt.Errorf("reap resources hook is required")
	}
	reaped, err := hooks.ReapResources(ctx, store.ReaperParams{
		OwnerUUID:       ownerUUID,
		FailedRetention: cfg.Phase7.Reaper.FailedRetention.Duration,
		Now:             now(),
	})
	if err != nil {
		return result, fmt.Errorf("reap resources: %w", err)
	}
	result.Reaped = reaped
	if hooks.DestroyReclaimableGenerationResources == nil {
		return result, fmt.Errorf("destroy reclaimable generation resources hook is required")
	}
	hooks.DestroyReclaimableGenerationResources(ctx, now())
	return result, nil
}
