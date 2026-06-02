package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

type sessionRuntimeDataVolumes struct {
	Workspace  store.SessionWorkspaceVolume
	DriverHome store.SessionDriverHomeVolume
}

func (s *Server) runtimeStartRequest(session store.Session, generationID string, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, volumes sessionRuntimeDataVolumes, contentSnapshots []store.ContentSnapshotRecord) runtime.StartRequest {
	driverID := strings.TrimSpace(details.DriverID)
	if driverID == "" {
		driverID = strings.TrimSpace(session.DriverID)
	}
	return runtime.StartRequest{
		SessionID:         session.ID,
		GenerationID:      generationID,
		DriverID:          driverID,
		Generation:        details,
		PreparedArtifacts: artifacts,
		WorkspaceHostPath: volumes.Workspace.HostPath,
		AgentHomeHostPath: volumes.DriverHome.HostPath,
		ContentSnapshots:  contentSnapshots,
	}
}

func validateDriverStateForRuntimeLaunch(details store.RuntimeGenerationDetails, volumes sessionRuntimeDataVolumes) error {
	return store.ValidateDriverStatePayloadForRuntimeLaunch(details.DriverID, details.DriverStatePayload, volumes.DriverHome.HostPath)
}

func (s *Server) ensureSessionRuntimeDataVolumes(ctx context.Context, session store.Session) (sessionRuntimeDataVolumes, error) {
	volumeConfig, err := s.dataVolumeProvisionerConfig()
	if err != nil {
		return sessionRuntimeDataVolumes{}, err
	}
	now := time.Now().UTC()
	workspace, err := s.store.ProvisionSessionWorkspace(ctx, store.ProvisionSessionWorkspaceParams{
		SessionID: session.ID,
		Config:    volumeConfig,
		Now:       now,
	})
	if err != nil {
		return sessionRuntimeDataVolumes{}, fmt.Errorf("provision session workspace volume: %w", err)
	}
	driverHome, err := s.store.ProvisionSessionDriverHome(ctx, store.ProvisionSessionDriverHomeParams{
		SessionID: session.ID,
		Driver:    session.DriverID,
		Config:    volumeConfig,
		Now:       now,
	})
	if err != nil {
		return sessionRuntimeDataVolumes{}, fmt.Errorf("provision session driver home volume: %w", err)
	}
	return sessionRuntimeDataVolumes{Workspace: workspace, DriverHome: driverHome}, nil
}
