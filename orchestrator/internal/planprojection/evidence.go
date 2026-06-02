package planprojection

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func Rows(details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, sandboxContractPayload map[string]any, planDigest string) []store.StoreGenerationPlanProjectionParams {
	generationID := strings.TrimSpace(details.GenerationID)
	return []store.StoreGenerationPlanProjectionParams{
		{
			GenerationID:      generationID,
			PlanDigest:        planDigest,
			ProjectionKind:    store.GenerationPlanProjectionSandboxContract,
			ProjectionVersion: projectionVersion(store.GenerationPlanProjectionSandboxContract),
			PayloadDigest:     SandboxContractPayloadDigest(sandboxContractPayload),
		},
		{
			GenerationID:      generationID,
			PlanDigest:        planDigest,
			ProjectionKind:    store.GenerationPlanProjectionControlManifest,
			ProjectionVersion: projectionVersion(store.GenerationPlanProjectionControlManifest),
			PayloadDigest:     PayloadDigest(store.GenerationPlanProjectionControlManifest, artifacts.ManifestDigest),
			MaterializedPath:  details.ControlManifestPath,
		},
		{
			GenerationID:      generationID,
			PlanDigest:        planDigest,
			ProjectionKind:    store.GenerationPlanProjectionControlManifestProjected,
			ProjectionVersion: projectionVersion(store.GenerationPlanProjectionControlManifestProjected),
			PayloadDigest:     PayloadDigest(store.GenerationPlanProjectionControlManifestProjected, artifacts.ProjectedManifestDigest),
			MaterializedPath:  details.ControlManifestPath,
		},
		{
			GenerationID:      generationID,
			PlanDigest:        planDigest,
			ProjectionKind:    store.GenerationPlanProjectionOCISpec,
			ProjectionVersion: projectionVersion(store.GenerationPlanProjectionOCISpec),
			PayloadDigest:     PayloadDigest(store.GenerationPlanProjectionOCISpec, artifacts.SpecDigest),
			MaterializedPath:  details.SpecPath,
		},
		{
			GenerationID:      generationID,
			PlanDigest:        planDigest,
			ProjectionKind:    store.GenerationPlanProjectionBundle,
			ProjectionVersion: projectionVersion(store.GenerationPlanProjectionBundle),
			PayloadDigest:     PayloadDigest(store.GenerationPlanProjectionBundle, artifacts.BundleDigest),
			MaterializedPath:  details.BundleDirPath,
		},
		{
			GenerationID:      generationID,
			PlanDigest:        planDigest,
			ProjectionKind:    store.GenerationPlanProjectionRuntimeConfig,
			ProjectionVersion: projectionVersion(store.GenerationPlanProjectionRuntimeConfig),
			PayloadDigest:     PayloadDigest(store.GenerationPlanProjectionRuntimeConfig, artifacts.RuntimeConfigDigest),
		},
	}
}

func Expectations(artifacts runtime.GenerationArtifacts) []store.GenerationPlanProjectionExpectation {
	return []store.GenerationPlanProjectionExpectation{
		{ProjectionKind: store.GenerationPlanProjectionControlManifest, ProjectionVersion: projectionVersion(store.GenerationPlanProjectionControlManifest), PayloadDigest: PayloadDigest(store.GenerationPlanProjectionControlManifest, artifacts.ManifestDigest)},
		{ProjectionKind: store.GenerationPlanProjectionControlManifestProjected, ProjectionVersion: projectionVersion(store.GenerationPlanProjectionControlManifestProjected), PayloadDigest: PayloadDigest(store.GenerationPlanProjectionControlManifestProjected, artifacts.ProjectedManifestDigest)},
		{ProjectionKind: store.GenerationPlanProjectionOCISpec, ProjectionVersion: projectionVersion(store.GenerationPlanProjectionOCISpec), PayloadDigest: PayloadDigest(store.GenerationPlanProjectionOCISpec, artifacts.SpecDigest)},
		{ProjectionKind: store.GenerationPlanProjectionBundle, ProjectionVersion: projectionVersion(store.GenerationPlanProjectionBundle), PayloadDigest: PayloadDigest(store.GenerationPlanProjectionBundle, artifacts.BundleDigest)},
		{ProjectionKind: store.GenerationPlanProjectionRuntimeConfig, ProjectionVersion: projectionVersion(store.GenerationPlanProjectionRuntimeConfig), PayloadDigest: PayloadDigest(store.GenerationPlanProjectionRuntimeConfig, artifacts.RuntimeConfigDigest)},
	}
}

func ExpectationsForDetails(details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts) []store.GenerationPlanProjectionExpectation {
	expectations := Expectations(artifacts)
	for i := range expectations {
		switch expectations[i].ProjectionKind {
		case store.GenerationPlanProjectionControlManifest, store.GenerationPlanProjectionControlManifestProjected:
			expectations[i].MaterializedPath = strings.TrimSpace(details.ControlManifestPath)
		case store.GenerationPlanProjectionOCISpec:
			expectations[i].MaterializedPath = strings.TrimSpace(details.SpecPath)
		case store.GenerationPlanProjectionBundle:
			expectations[i].MaterializedPath = strings.TrimSpace(details.BundleDirPath)
		}
	}
	return expectations
}

func DigestMap(artifacts runtime.GenerationArtifacts) map[string]string {
	out := map[string]string{}
	for _, expectation := range Expectations(artifacts) {
		out[expectation.ProjectionKind] = expectation.PayloadDigest
	}
	return out
}

func VersionMap(artifacts runtime.GenerationArtifacts) map[string]int {
	out := map[string]int{}
	for _, expectation := range Expectations(artifacts) {
		out[expectation.ProjectionKind] = expectation.ProjectionVersion
	}
	return out
}

func PayloadDigest(kind string, digest any) string {
	value := strings.TrimSpace(fmt.Sprint(digest))
	if strings.HasPrefix(value, "sha256:") {
		return value
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(kind) + "\n" + value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func SandboxContractPayloadDigest(payload map[string]any) string {
	canonical, err := store.CanonicalSandboxContractPayload(payload)
	if err != nil {
		return PayloadDigest(store.GenerationPlanProjectionSandboxContract, payload["sandbox_contract_digest"])
	}
	return store.SandboxContractDigest(canonical)
}

func projectionVersion(kind string) int {
	version, _ := store.GenerationPlanProjectionVersionFor(kind)
	return version
}
