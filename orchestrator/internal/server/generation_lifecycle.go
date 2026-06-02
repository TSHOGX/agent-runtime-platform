package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

var errGenerationBusy = errors.New("generation lifecycle is busy")

type ensuredGeneration struct {
	Allocation            store.GenerationAllocation
	IsNew                 bool
	RestoreFromCheckpoint bool
}

func (s *Server) failGenerationBeforeTurn(session store.Session, generationID, owner string, failure error, failureMode generationStartFailureMode) {
	reason := ""
	if failure != nil {
		reason = failure.Error()
	}
	errorClass := runtimeFailureClass(reason)
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	retryableStatus := string(sessionstate.RunningIdle)
	if failureMode == startFailureInputBlocking {
		retryableStatus = string(sessionstate.RunningActive)
	} else if session.Status == string(sessionstate.Created) {
		retryableStatus = string(sessionstate.Created)
	}
	eventID, err := s.store.FailGenerationStart(ctx, store.FailGenerationStartParams{
		SessionID:      session.ID,
		GenerationID:   generationID,
		Owner:          owner,
		SessionStatus:  retryableStatus,
		ErrorClass:     errorClass,
		Reason:         reason,
		EventType:      "generation.error",
		EventDedupeKey: "generation_error:" + generationID,
		Now:            now,
	})
	if err != nil {
		s.log.Warn("failed to fail generation before turn start", "session_id", session.ID, "generation_id", generationID, "error", err)
		return
	}
	s.publishDurableEvent(ctx, eventID)
}

func (s *Server) ensureActiveGeneration(ctx context.Context, session store.Session, owner string) (ensuredGeneration, error) {
	return s.ensureActiveGenerationWithRestoreRefetch(ctx, session, owner, true)
}

func (s *Server) ensureActiveGenerationWithRestoreRefetch(ctx context.Context, session store.Session, owner string, allowRestoreRefetch bool) (ensuredGeneration, error) {
	verifySessionDeployment := func() error {
		mode := strings.TrimSpace(session.Mode)
		if mode == "" {
			return fmt.Errorf("session mode is required")
		}
		if _, capabilityErr := s.resolveDriverDeployment(mode, agents.ID(session.DriverID)); capabilityErr != nil {
			return capabilityErr
		}
		return nil
	}
	activeGenerationID := strings.TrimSpace(session.ActiveGenerationID)
	if activeGenerationID != "" {
		status, err := s.store.GetRuntimeGenerationStatus(ctx, session.ID, activeGenerationID)
		if err != nil {
			return ensuredGeneration{}, err
		}
		if status == "checkpointed" {
			allocation, err := s.store.ClaimCheckpointedGenerationForRestore(ctx, store.ClaimCheckpointedGenerationParams{
				SessionID:    session.ID,
				GenerationID: activeGenerationID,
				Owner:        owner,
				LeaseTTL:     s.cfg.Harness.Bridge.LeaseTTL.Duration,
				Now:          time.Now().UTC(),
			})
			if err != nil {
				if allowRestoreRefetch && errors.Is(err, store.ErrStaleCheckpointRestore) {
					refreshed, refreshErr := s.store.GetSession(ctx, session.ID)
					if refreshErr != nil {
						return ensuredGeneration{}, refreshErr
					}
					return s.ensureActiveGenerationWithRestoreRefetch(ctx, refreshed, owner, false)
				}
				return ensuredGeneration{}, err
			}
			return ensuredGeneration{
				Allocation:            allocation,
				RestoreFromCheckpoint: true,
			}, nil
		}
		if generationLifecycleBusy(status) {
			return ensuredGeneration{}, errGenerationBusy
		}
		if status != "failed" {
			return ensuredGeneration{
				Allocation: store.GenerationAllocation{
					GenerationID: activeGenerationID,
					Owner:        owner,
				},
				IsNew: false,
			}, nil
		}
		if err := verifySessionDeployment(); err != nil {
			return ensuredGeneration{}, err
		}
		allocation, err := s.store.AllocateGeneration(ctx, store.AllocateGenerationParams{
			SessionID:            session.ID,
			ExpectedGenerationID: sql.NullString{String: activeGenerationID, Valid: true},
			Owner:                owner,
			LeaseTTL:             s.cfg.Harness.Bridge.LeaseTTL.Duration,
			Now:                  time.Now().UTC(),
			Config:               s.resourceAllocatorConfig(session.DriverID),
		})
		if err != nil {
			return ensuredGeneration{}, err
		}
		return ensuredGeneration{Allocation: allocation, IsNew: true}, nil
	}
	if err := verifySessionDeployment(); err != nil {
		return ensuredGeneration{}, err
	}
	allocation, err := s.store.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     owner,
		LeaseTTL:  s.cfg.Harness.Bridge.LeaseTTL.Duration,
		Now:       time.Now().UTC(),
		Config:    s.resourceAllocatorConfig(session.DriverID),
	})
	if err != nil {
		return ensuredGeneration{}, err
	}
	return ensuredGeneration{Allocation: allocation, IsNew: true}, nil
}

func generationLifecycleBusy(status string) bool {
	switch status {
	case "allocating", "starting", "probing", "checkpointing", "restoring":
		return true
	default:
		return false
	}
}
