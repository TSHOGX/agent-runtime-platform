package server

import (
	"context"
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func (s *Server) verifyStoredGenerationPlanProjections(ctx context.Context, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, sandboxContractDigest string) (bool, error) {
	return s.store.VerifyGenerationPlanProjections(ctx, store.VerifyGenerationPlanProjectionsParams{
		GenerationID: details.GenerationID,
		Expected:     generationPlanProjectionExpectationsForDetails(details, artifacts, sandboxContractDigest),
		RequirePlan:  true,
	})
}

func generationPlanProjectionExpectations(artifacts runtime.GenerationArtifacts, sandboxContractDigest string) []store.GenerationPlanProjectionExpectation {
	return generationPlanProjectionExpectationsForDetails(store.RuntimeGenerationDetails{}, artifacts, sandboxContractDigest)
}

func generationPlanProjectionExpectationsForDetails(details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, sandboxContractDigest string) []store.GenerationPlanProjectionExpectation {
	expectations := planprojection.Expectations(artifacts)
	if strings.TrimSpace(details.GenerationID) != "" {
		expectations = planprojection.ExpectationsForDetails(details, artifacts)
	}
	sandboxContractDigest = strings.TrimSpace(sandboxContractDigest)
	if sandboxContractDigest == "" {
		return expectations
	}
	return append([]store.GenerationPlanProjectionExpectation{
		{
			ProjectionKind:    store.GenerationPlanProjectionSandboxContract,
			ProjectionVersion: store.GenerationPlanProjectionVersion,
			PayloadDigest:     sandboxContractDigest,
		},
	}, expectations...)
}

func (s *Server) storedGenerationPlanProjectionEvidence(ctx context.Context, generationID, planDigest string) (map[string]string, map[string]int, error) {
	records, err := s.store.ListGenerationPlanProjections(ctx, generationID)
	if err != nil {
		return nil, nil, err
	}
	digests := map[string]string{}
	versions := map[string]int{}
	for _, record := range records {
		kind := strings.TrimSpace(record.ProjectionKind)
		if kind == "" {
			return nil, nil, fmt.Errorf("generation plan projection kind is required")
		}
		if strings.TrimSpace(record.PlanDigest) != strings.TrimSpace(planDigest) {
			return nil, nil, fmt.Errorf("generation plan projection %s plan digest mismatch: got %s want %s", kind, record.PlanDigest, planDigest)
		}
		digests[kind] = record.PayloadDigest
		versions[kind] = record.ProjectionVersion
	}
	for _, kind := range store.GenerationPlanProjectionKinds() {
		if strings.TrimSpace(digests[kind]) == "" {
			return nil, nil, fmt.Errorf("generation plan projection %s is required", kind)
		}
		if versions[kind] <= 0 {
			return nil, nil, fmt.Errorf("generation plan projection %s version is required", kind)
		}
	}
	return digests, versions, nil
}
