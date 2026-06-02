package server

import (
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func sandboxContractID(generationID string) string {
	return "contract_" + strings.TrimSpace(generationID)
}

func (s *Server) sandboxContractPayload(session store.Session, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, resourceIdentityDigest string, volumes sessionRuntimeDataVolumes, contentSnapshots []store.ContentSnapshotRecord) (map[string]any, error) {
	driverID := strings.TrimSpace(details.DriverID)
	mode := strings.TrimSpace(session.Mode)
	if mode == "" {
		return nil, fmt.Errorf("session mode is required")
	}
	deployment, capabilityErr := s.resolveDriverDeployment(mode, agents.ID(driverID))
	if capabilityErr != nil {
		return nil, capabilityErr
	}
	inputDigests, err := s.driverManifestInputDigests(deployment)
	if err != nil {
		return nil, err
	}
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		return nil, err
	}
	return planprojection.RenderSandboxContract(planprojection.SandboxContractParams{
		Session:                     session,
		Details:                     details,
		Artifacts:                   artifacts,
		ResourceIdentityDigest:      resourceIdentityDigest,
		NetworkIdentityNftTableName: nftTableName,
		Volumes: planprojection.DataVolumes{
			Workspace:  volumes.Workspace,
			DriverHome: volumes.DriverHome,
		},
		DriverSpec:       deployment.DriverSpec,
		ProviderSpec:     deployment.ProviderSpec,
		ContentSnapshots: contentSnapshots,
		InputDigests: planprojection.SandboxContractInputDigests{
			RuntimeConfigDigest: inputDigests.RuntimeConfigDigest,
			AgentManifestDigest: inputDigests.AgentManifestDigest,
		},
	})
}

type driverManifestInputDigests struct {
	RuntimeConfigDigest string
	AgentManifestDigest string
}

func (s *Server) driverManifestInputDigests(deployment deploymentResolution) (driverManifestInputDigests, error) {
	defaultAgent, err := s.explicitDefaultAgent()
	if err != nil {
		return driverManifestInputDigests{}, err
	}
	runtimeConfigDigest, err := runtimeConfigDigest(deployment.runtimeConfigPreimage(defaultAgent))
	if err != nil {
		return driverManifestInputDigests{}, err
	}
	return driverManifestInputDigests{
		RuntimeConfigDigest: runtimeConfigDigest,
		AgentManifestDigest: deployment.AgentManifest.Digest,
	}, nil
}

func sandboxContractDigestForPayload(value any) (string, error) {
	return planprojection.SandboxContractDigestForPayload(value)
}
