package server

import (
	"context"
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/store"
)

func (s *Server) destroyGenerationRuntime(ctx context.Context, details store.RuntimeGenerationDetails) error {
	runtimeID := strings.TrimSpace(details.RunscContainerID)
	if runtimeID == "" {
		return fmt.Errorf("generation %s has no runsc container id", details.GenerationID)
	}
	return s.runtime.Destroy(ctx, runtimeID)
}

func (s *Server) runtimeGenerationDetails(ctx context.Context, sessionID, generationID string) (store.RuntimeGenerationDetails, error) {
	if strings.TrimSpace(generationID) == "" {
		return store.RuntimeGenerationDetails{}, fmt.Errorf("generation id is required")
	}
	details, err := s.store.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		return store.RuntimeGenerationDetails{}, err
	}
	return details, nil
}
