package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

const checkpointTimeout = 2 * time.Minute

func (s *Server) RunMaintenance(ctx context.Context) error {
	if strings.TrimSpace(s.ownerUUID) == "" {
		return fmt.Errorf("maintenance requires owner uuid")
	}
	heartbeatInterval := s.cfg.Harness.Bridge.HeartbeatInterval.Duration
	if heartbeatInterval <= 0 {
		return fmt.Errorf("bridge heartbeat interval must be > 0")
	}
	pollInterval := s.cfg.Harness.Bridge.PollInterval.Duration
	if pollInterval <= 0 {
		return fmt.Errorf("bridge poll interval must be > 0")
	}
	owner := store.GenerationLeaseOwner(s.ownerUUID)
	processor := &bridge.Processor{
		Store:                   bridgeStore(s.store),
		Owner:                   owner,
		LeaseTTL:                s.cfg.Harness.Bridge.LeaseTTL.Duration,
		AckStartedGrace:         s.cfg.Harness.Bridge.AckStartedGrace.Duration,
		RequiredProtocolVersion: bridge.RequiredProtocolVersionV2,
		RequiredTurnInputSchema: bridge.RequiredTurnInputRunTurn,
		AfterCommit:             s.handleBridgeCommittedEnvelope,
	}
	touchHostHeartbeat := func(generation store.BridgePollGeneration, now time.Time) {
		if err := bridge.TouchHeartbeat(generation.BridgeDirPath, bridge.HostHeartbeatFile, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("bridge host heartbeat failed", "session_id", generation.SessionID, "generation_id", generation.GenerationID, "error", err)
		}
	}

	runMaintenance := func(now time.Time) {
		if s.cfg.SessionRetention == 0 {
			if _, err := s.store.ClearActiveSessionExpiry(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Warn("active-session expiry clear failed", "error", err)
			}
		}
		if _, err := s.store.SweepExpiredSessions(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("expired-session sweep failed", "error", err)
		}
		if _, err := s.store.CancelTerminalSessionPendingTurns(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("terminal-session turn cleanup failed", "error", err)
		}
		if _, err := s.RecoverExpiredRuntimeResources(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("allocation recovery failed", "error", err)
		}
		generations, err := s.store.ListBridgePollGenerations(ctx, owner, now, s.cfg.Harness.Bridge.AckStartedGrace.Duration)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("bridge heartbeat generation list failed", "error", err)
			}
		} else {
			for _, generation := range generations {
				touchHostHeartbeat(generation, now)
			}
		}
		retiredCheckpoints, err := s.store.RetireExpiredCheckpoints(ctx, store.RetireExpiredCheckpointsParams{
			OwnerUUID:                s.ownerUUID,
			Now:                      now,
			CheckpointImageRetention: s.cfg.Harness.Reaper.CheckpointImageRetention.Duration,
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("checkpoint retirement failed", "error", err)
			}
		} else {
			for _, retired := range retiredCheckpoints {
				s.publishDurableEvent(ctx, retired.EventID)
			}
		}
		if _, err := s.store.ReapResources(ctx, store.ReaperParams{
			OwnerUUID:       s.ownerUUID,
			FailedRetention: s.cfg.Harness.Reaper.FailedRetention.Duration,
			Now:             now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("resource reaper failed", "error", err)
		}
		s.destroyReclaimableGenerationResources(ctx, now)
		if _, err := s.store.PruneEvents(ctx, store.PruneEventsParams{
			RetentionWindow: s.cfg.Harness.Events.RetentionWindow.Duration,
			RetentionRows:   s.cfg.Harness.Events.RetentionRows,
			Now:             now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("event retention prune failed", "error", err)
		}
	}
	pollBridge := func(now time.Time) {
		generations, err := s.store.ListBridgePollGenerations(ctx, owner, now, s.cfg.Harness.Bridge.AckStartedGrace.Duration)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("bridge generation list failed", "error", err)
			}
			return
		}
		for _, generation := range generations {
			processor.MarkReady(generation.SessionID, generation.GenerationID)
			if err := processor.ProcessOnce(ctx, generation.BridgeDirPath); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				s.log.Warn("bridge poll failed", "session_id", generation.SessionID, "generation_id", generation.GenerationID, "error", err)
			}
		}
	}

	runMaintenance(time.Now().UTC())
	pollBridge(time.Now().UTC())
	maintenanceTicker := time.NewTicker(heartbeatInterval)
	defer maintenanceTicker.Stop()
	bridgeTicker := time.NewTicker(pollInterval)
	defer bridgeTicker.Stop()
	for {
		select {
		case now := <-maintenanceTicker.C:
			runMaintenance(now.UTC())
		case now := <-bridgeTicker.C:
			pollBridge(now.UTC())
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) RecoverExpiredRuntimeResources(ctx context.Context, now time.Time) (store.StartupRecoveryResult, error) {
	if strings.TrimSpace(s.ownerUUID) == "" {
		return store.StartupRecoveryResult{}, fmt.Errorf("runtime recovery requires owner uuid")
	}
	params := store.StartupRecoveryParams{
		OwnerUUID:       s.ownerUUID,
		Now:             now,
		LeaseTTL:        s.cfg.Harness.Bridge.LeaseTTL.Duration,
		ReconnectGrace:  s.cfg.Harness.Bridge.ReconnectGrace.Duration,
		AckStartedGrace: s.cfg.Harness.Bridge.AckStartedGrace.Duration,
	}
	candidates, err := s.store.ListExpiredRuntimeRecoveryCandidates(ctx, params)
	if err != nil {
		return store.StartupRecoveryResult{}, err
	}
	cleaned := make([]store.ExpiredRuntimeRecoveryCandidate, 0, len(candidates))
	result := store.StartupRecoveryResult{}
	for _, candidate := range candidates {
		runtimeID := strings.TrimSpace(candidate.RuntimeID)
		if runtimeID == "" {
			result.RuntimeCleanupSkipped++
			s.log.Warn("recovery candidate has no runtime id", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID)
			continue
		}
		if err := s.runtime.Destroy(ctx, runtimeID); err != nil {
			if errors.Is(err, context.Canceled) {
				return result, err
			}
			result.RuntimeCleanupSkipped++
			s.log.Warn("runtime cleanup before recovery failed", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "runtime_id", runtimeID, "error", err)
			continue
		}
		cleaned = append(cleaned, candidate)
	}
	repaired, err := s.store.RepairExpiredRuntimeRecovery(ctx, params, cleaned)
	if err != nil {
		return result, err
	}
	repaired.RuntimeCleanupSkipped += result.RuntimeCleanupSkipped
	for _, eventID := range repaired.EventIDs {
		s.publishDurableEvent(ctx, eventID)
	}
	return repaired, nil
}

func (s *Server) DestroyReclaimableGenerationResources(ctx context.Context, now time.Time) {
	s.destroyReclaimableGenerationResources(ctx, now)
}

func (s *Server) destroyReclaimableGenerationResources(ctx context.Context, now time.Time) {
	candidates, err := s.store.ListDestroyableReclaimableGenerations(ctx, now, s.cfg.Harness.Reaper.FailedRetention.Duration)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("destroyable resource list failed", "error", err)
		}
		return
	}
	for _, candidate := range candidates {
		if err := s.cleanupGenerationResources(ctx, candidate.SessionID, candidate.GenerationID, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("generation resource cleanup failed", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", err)
		}
	}
}

func (s *Server) cleanupGenerationResources(ctx context.Context, sessionID, generationID string, now time.Time) error {
	details, err := s.store.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		return fmt.Errorf("lookup generation resources: %w", err)
	}
	resourceInstance, err := s.store.GetRuntimeResourceCleanupIdentity(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("runtime resource instance is required for generation cleanup")
	}
	if err != nil {
		return fmt.Errorf("lookup runtime resource instance: %w", err)
	}
	if err := s.claimRuntimeResourceCleanup(ctx, resourceInstance, now); err != nil {
		return err
	}
	details = runtimeDetailsWithResourceInstance(details, resourceInstance)
	cleanup, err := s.runtime.DestroyGenerationResources(ctx, details)
	if err != nil {
		return fmt.Errorf("destroy generation resources: %w", err)
	}
	if err := s.store.MarkGenerationResourcesDestroyed(ctx, store.DestroyGenerationResourcesParams{
		SessionID:    sessionID,
		GenerationID: generationID,
		Now:          now,
	}); err != nil {
		return fmt.Errorf("mark generation resources destroyed: %w", err)
	}
	if err := s.completeRuntimeResourceCleanup(ctx, resourceInstance, cleanup, now); err != nil {
		return fmt.Errorf("mark runtime resource destroyed: %w", err)
	}
	return nil
}

func (s *Server) MonitorIdleSessions(ctx context.Context) error {
	if strings.EqualFold(strings.TrimSpace(s.cfg.RunscNetwork), "host") {
		s.log.Info("idle checkpoint monitor disabled because runsc host network is not checkpointable")
		return nil
	}
	if !s.cfg.Harness.Checkpoint.AutoEnabled {
		s.log.Info("idle checkpoint monitor disabled by policy")
		return nil
	}
	if strings.TrimSpace(s.ownerUUID) == "" {
		return fmt.Errorf("idle checkpoint monitor requires owner uuid")
	}

	owner := store.GenerationLeaseOwner(s.ownerUUID)
	interval := s.cfg.Harness.Checkpoint.MonitorInterval.Duration
	if interval <= 0 {
		return fmt.Errorf("checkpoint monitor interval must be > 0")
	}
	idleThreshold := s.cfg.Harness.Checkpoint.IdleThreshold.Duration
	if idleThreshold <= 0 {
		return fmt.Errorf("checkpoint idle threshold must be > 0")
	}
	heartbeatInterval := s.cfg.Harness.Bridge.HeartbeatInterval.Duration
	if heartbeatInterval <= 0 {
		return fmt.Errorf("bridge heartbeat interval must be > 0")
	}
	tick := func(now time.Time) {
		candidates, err := s.store.ListAutoCheckpointCandidates(ctx, owner, now, idleThreshold)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("failed to list auto checkpoint candidates", "error", err)
			}
			return
		}
		for _, candidate := range candidates {
			if !bridgeCheckpointReady(candidate.BridgeDirPath, now, heartbeatInterval) {
				continue
			}
			if err := s.checkpointGeneration(ctx, candidate, owner, now); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Warn("auto checkpoint failed", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", err)
			}
		}
	}

	tick(time.Now().UTC())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			tick(now.UTC())
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) checkpointGeneration(ctx context.Context, candidate store.CheckpointCandidate, owner string, now time.Time) error {
	if err := s.store.BeginGenerationCheckpoint(ctx, candidate.SessionID, candidate.GenerationID, owner, now); err != nil {
		return err
	}
	details, err := s.store.GetRuntimeGenerationDetails(ctx, candidate.SessionID, candidate.GenerationID)
	if err != nil {
		abortNow := time.Now().UTC()
		if abortErr := s.store.AbortGenerationCheckpoint(ctx, candidate.SessionID, candidate.GenerationID, owner, abortNow); abortErr != nil {
			s.log.Warn("failed to abort generation checkpoint after metadata load failure", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", abortErr)
		}
		return err
	}
	artifacts, err := s.generationPlanRuntimeArtifacts(ctx, candidate.GenerationID)
	if err != nil {
		abortNow := time.Now().UTC()
		if abortErr := s.store.AbortGenerationCheckpoint(ctx, candidate.SessionID, candidate.GenerationID, owner, abortNow); abortErr != nil {
			s.log.Warn("failed to abort generation checkpoint after plan artifact load failure", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", abortErr)
		}
		return err
	}
	if err := s.verifyGenerationPlanFrozenEvidence(ctx, candidate.GenerationID, details, artifacts); err != nil {
		abortNow := time.Now().UTC()
		if abortErr := s.store.AbortGenerationCheckpoint(ctx, candidate.SessionID, candidate.GenerationID, owner, abortNow); abortErr != nil {
			s.log.Warn("failed to abort generation checkpoint after plan verification failure", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", abortErr)
		}
		return err
	}
	plan, err := s.store.GetGenerationPlan(ctx, candidate.GenerationID)
	if err != nil {
		abortNow := time.Now().UTC()
		if abortErr := s.store.AbortGenerationCheckpoint(ctx, candidate.SessionID, candidate.GenerationID, owner, abortNow); abortErr != nil {
			s.log.Warn("failed to abort generation checkpoint after plan load failure", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", abortErr)
		}
		return err
	}
	checkpointCtx, cancel := context.WithTimeout(ctx, checkpointTimeout)
	defer cancel()
	checkpointResult, err := s.runtime.Checkpoint(checkpointCtx, runtime.CheckpointRequest{
		SessionID:      candidate.SessionID,
		GenerationID:   candidate.GenerationID,
		CheckpointPath: details.CheckpointPath,
		Generation:     details,
	})
	if err != nil {
		abortNow := time.Now().UTC()
		if abortErr := s.store.AbortGenerationCheckpoint(ctx, candidate.SessionID, candidate.GenerationID, owner, abortNow); abortErr != nil {
			s.log.Warn("failed to abort generation checkpoint", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", abortErr)
		} else {
			s.hub.Publish(events.Event{Type: "session." + string(sessionstate.RunningIdle), SessionID: candidate.SessionID, GenerationID: candidate.GenerationID, Payload: map[string]string{"checkpoint_error": err.Error()}})
		}
		return err
	}
	completeNow := time.Now().UTC()
	if err := s.store.CompleteGenerationCheckpoint(ctx, store.CompleteCheckpointParams{
		SessionID:                       candidate.SessionID,
		GenerationID:                    candidate.GenerationID,
		Owner:                           owner,
		CheckpointPath:                  details.CheckpointPath,
		RunscPlatform:                   details.RunscPlatform,
		RunscVersion:                    artifacts.RunscVersion,
		RunscBinaryPath:                 artifacts.RunscBinaryPath,
		RunscBinaryDigest:               artifacts.RunscBinaryDigest,
		CheckpointBundleDigest:          artifacts.BundleDigest,
		CheckpointRuntimeConfigDigest:   artifacts.RuntimeConfigDigest,
		CheckpointControlManifestDigest: artifacts.ProjectedManifestDigest,
		CheckpointPlanDigest:            plan.PlanDigest,
		CheckpointImageManifestDigest:   checkpointResult.ImageManifestDigest,
		Now:                             completeNow,
	}); err != nil {
		return err
	}
	if err := s.reserveRuntimeResourceCheckpoint(ctx, candidate.GenerationID); err != nil {
		return err
	}
	s.hub.Publish(events.Event{Type: "session." + string(sessionstate.Checkpointed), SessionID: candidate.SessionID, GenerationID: candidate.GenerationID})
	return nil
}

func bridgeCheckpointReady(root string, now time.Time, heartbeatInterval time.Duration) bool {
	if strings.TrimSpace(root) == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if heartbeatInterval <= 0 {
		return false
	}
	maxAge := heartbeatInterval * 2
	if maxAge < heartbeatInterval+5*time.Second {
		maxAge = heartbeatInterval + 5*time.Second
	}
	heartbeatPath := filepath.Join(root, bridge.HeartbeatDir, bridge.BridgeHeartbeatFile)
	readyPath := filepath.Join(root, bridge.HeartbeatDir, bridge.CheckpointReadyFile)
	return controlFileFresh(heartbeatPath, now, maxAge) && controlFileFresh(readyPath, now, maxAge)
}

func controlFileFresh(path string, now time.Time, maxAge time.Duration) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	timestamp, ok := parseBridgeControlTimestamp(strings.TrimSpace(string(data)))
	if !ok {
		timestamp = info.ModTime()
	}
	if timestamp.After(now.Add(5 * time.Second)) {
		return false
	}
	return now.Sub(timestamp) <= maxAge
}

func parseBridgeControlTimestamp(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.UTC(), true
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, value).UTC(), true
}
