package server

import (
	"context"
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/store"
)

func (s *Server) generationContentSnapshots(ctx context.Context, session store.Session, details store.RuntimeGenerationDetails) ([]store.ContentSnapshotRecord, error) {
	driverID, err := agents.CanonicalDriverID(details.DriverID)
	if err != nil {
		return nil, err
	}
	mode := strings.TrimSpace(session.Mode)
	if mode == "" {
		return nil, fmt.Errorf("session mode is required")
	}
	deployment, capabilityErr := s.resolveDriverDeployment(mode, driverID)
	if capabilityErr != nil {
		return nil, capabilityErr
	}
	policy := agents.DefaultFeaturePolicyForDriver(deployment.DriverSpec)
	return s.selectGenerationContentSnapshots(ctx, policy)
}

type contentSnapshotFeatureRequirement struct {
	feature agents.FeatureID
	kind    string
}

var contentSnapshotFeatureRequirements = []contentSnapshotFeatureRequirement{
	{feature: agents.FeatureSkillsSnapshot, kind: store.ContentSnapshotKindSkills},
	{feature: agents.FeatureManagedSettings, kind: store.ContentSnapshotKindManagedSettings},
}

func (s *Server) selectGenerationContentSnapshots(ctx context.Context, policy agents.FeaturePolicy) ([]store.ContentSnapshotRecord, error) {
	selected := []store.ContentSnapshotRecord{}
	for _, requirement := range contentSnapshotFeatureRequirements {
		if policy[requirement.feature] != agents.FeaturePolicyRequired {
			continue
		}
		records, err := s.store.ListContentSnapshots(ctx, requirement.kind)
		if err != nil {
			return nil, err
		}
		switch len(records) {
		case 0:
			return nil, fmt.Errorf("content snapshot selection for required feature %s has no %s snapshot", requirement.feature, requirement.kind)
		case 1:
			selected = append(selected, records[0])
		default:
			return nil, fmt.Errorf("content snapshot selection for required feature %s is ambiguous: %d %s snapshots", requirement.feature, len(records), requirement.kind)
		}
	}
	return selected, nil
}

func (s *Server) generationContentSnapshotsForStart(ctx context.Context, session store.Session, details store.RuntimeGenerationDetails, isNew bool) ([]store.ContentSnapshotRecord, error) {
	if isNew {
		return s.generationContentSnapshots(ctx, session, details)
	}
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, strings.TrimSpace(details.GenerationID))
	if err != nil {
		return nil, err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return nil, err
	}
	return s.generationPlanContentSnapshotRecords(ctx, plan.CanonicalPayload)
}
