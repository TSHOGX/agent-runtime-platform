package server

import (
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/store"
)

func (s *Server) dataVolumeProvisionerConfig() (store.DataVolumeProvisionerConfig, error) {
	roots, err := config.ValidateIsolationRoots(s.cfg.IsolationRoots())
	if err != nil {
		return store.DataVolumeProvisionerConfig{}, err
	}
	identity := s.cfg.Harness.SandboxIdentity
	return store.DataVolumeProvisionerConfig{
		SessionsRoot:   roots.SessionsRoot,
		AgentHomesRoot: roots.AgentHomesRoot,
		EvidenceRoot:   roots.DataVolumeEvidenceRoot,
		LayoutVersion:  store.DataVolumeLayoutVersion,
		RuntimeIdentity: store.RuntimeIdentity{
			UID:              identity.UID,
			GID:              identity.GID,
			SupplementalGIDs: identity.SupplementalGIDs,
		},
	}, nil
}
