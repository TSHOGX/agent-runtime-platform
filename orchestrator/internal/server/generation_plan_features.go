package server

import (
	"context"
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/generationplan"
)

func (s *Server) generationPlanFeatureRequired(ctx context.Context, generationID string, feature agents.FeatureID) (bool, error) {
	generationID = strings.TrimSpace(generationID)
	if generationID == "" {
		return false, fmt.Errorf("active generation is required")
	}
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, generationID)
	if err != nil {
		return false, err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return false, err
	}
	state, err := generationPlanFeaturePolicyState(plan.CanonicalPayload, feature)
	if err != nil {
		return false, err
	}
	return state == agents.FeaturePolicyRequired, nil
}

func generationPlanFeaturePolicyState(payload []byte, feature agents.FeatureID) (agents.FeaturePolicyState, error) {
	object, err := generationplan.PayloadObject(payload)
	if err != nil {
		return "", err
	}
	policy, ok := object["feature_policy"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("generation plan feature_policy is required")
	}
	value := strings.TrimSpace(fmt.Sprint(policy[string(feature)]))
	if value == "" {
		return "", fmt.Errorf("generation plan feature_policy.%s is required", feature)
	}
	state := agents.FeaturePolicyState(value)
	switch state {
	case agents.FeaturePolicyRequired, agents.FeaturePolicyDisabled, agents.FeaturePolicyUnsupported:
		return state, nil
	default:
		return "", fmt.Errorf("generation plan feature_policy.%s has invalid state %q", feature, value)
	}
}
