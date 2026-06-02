package planprojection

import (
	"testing"

	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func TestRowsBuildStoreProjectionParams(t *testing.T) {
	details := testRuntimeDetails()
	artifacts := testArtifacts()
	rows := Rows(details, artifacts, map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"generation_id":            details.GenerationID,
	}, "sha256:plan")

	if len(rows) != len(store.GenerationPlanProjectionKinds()) {
		t.Fatalf("projection row count = %d want %d", len(rows), len(store.GenerationPlanProjectionKinds()))
	}
	byKind := map[string]store.StoreGenerationPlanProjectionParams{}
	for _, row := range rows {
		byKind[row.ProjectionKind] = row
		if row.GenerationID != details.GenerationID {
			t.Fatalf("%s generation id = %q want %q", row.ProjectionKind, row.GenerationID, details.GenerationID)
		}
		if row.PlanDigest != "sha256:plan" {
			t.Fatalf("%s plan digest = %q want sha256:plan", row.ProjectionKind, row.PlanDigest)
		}
		if row.ProjectionVersion != store.GenerationPlanProjectionVersion {
			t.Fatalf("%s version = %d want %d", row.ProjectionKind, row.ProjectionVersion, store.GenerationPlanProjectionVersion)
		}
	}
	if byKind[store.GenerationPlanProjectionControlManifest].PayloadDigest != PayloadDigest(store.GenerationPlanProjectionControlManifest, artifacts.ManifestDigest) {
		t.Fatalf("control manifest digest mismatch")
	}
	if byKind[store.GenerationPlanProjectionControlManifest].MaterializedPath != details.ControlManifestPath {
		t.Fatalf("control manifest path = %q want %q", byKind[store.GenerationPlanProjectionControlManifest].MaterializedPath, details.ControlManifestPath)
	}
	if byKind[store.GenerationPlanProjectionOCISpec].MaterializedPath != details.SpecPath {
		t.Fatalf("oci spec path = %q want %q", byKind[store.GenerationPlanProjectionOCISpec].MaterializedPath, details.SpecPath)
	}
	if byKind[store.GenerationPlanProjectionBundle].MaterializedPath != details.BundleDirPath {
		t.Fatalf("bundle path = %q want %q", byKind[store.GenerationPlanProjectionBundle].MaterializedPath, details.BundleDirPath)
	}
	if byKind[store.GenerationPlanProjectionRuntimeConfig].MaterializedPath != "" {
		t.Fatalf("runtime config path = %q want empty", byKind[store.GenerationPlanProjectionRuntimeConfig].MaterializedPath)
	}
}

func TestExpectationDigestAndVersionMapsUseExpectations(t *testing.T) {
	artifacts := testArtifacts()
	digests := DigestMap(artifacts)
	versions := VersionMap(artifacts)

	for _, expectation := range Expectations(artifacts) {
		if digests[expectation.ProjectionKind] != expectation.PayloadDigest {
			t.Fatalf("%s digest = %q want %q", expectation.ProjectionKind, digests[expectation.ProjectionKind], expectation.PayloadDigest)
		}
		if versions[expectation.ProjectionKind] != expectation.ProjectionVersion {
			t.Fatalf("%s version = %d want %d", expectation.ProjectionKind, versions[expectation.ProjectionKind], expectation.ProjectionVersion)
		}
	}
	if versions[store.GenerationPlanProjectionOCISpec] != store.GenerationPlanProjectionVersion {
		t.Fatalf("oci spec projection version = %d want %d", versions[store.GenerationPlanProjectionOCISpec], store.GenerationPlanProjectionVersion)
	}
}

func TestPayloadDigestPassesThroughPrefixedDigest(t *testing.T) {
	if got := PayloadDigest(store.GenerationPlanProjectionBundle, "sha256:bundle"); got != "sha256:bundle" {
		t.Fatalf("payload digest = %q want sha256:bundle", got)
	}
	if got := PayloadDigest(store.GenerationPlanProjectionBundle, "bundle_digest"); got != "sha256:1d101de303b0f0c6c69013008285b8f2bbce4627b9ac8975083b0b0032945f13" {
		t.Fatalf("payload digest fallback = %q", got)
	}
}

func TestSandboxContractPayloadDigestFallsBackToEmbeddedDigest(t *testing.T) {
	payload := map[string]any{
		"sandbox_contract_digest": "contract_digest",
	}
	if got := SandboxContractPayloadDigest(payload); got != PayloadDigest(store.GenerationPlanProjectionSandboxContract, "contract_digest") {
		t.Fatalf("sandbox contract digest fallback = %q", got)
	}
}

func testRuntimeDetails() store.RuntimeGenerationDetails {
	return store.RuntimeGenerationDetails{
		GenerationID:        "gen_projection",
		ControlManifestPath: "/var/lib/harness/run/control/gen_projection/session.json",
		SpecPath:            "/var/lib/harness/run/runtime/gen_projection/config.json",
		BundleDirPath:       "/var/lib/harness/run/runtime/gen_projection",
	}
}

func testArtifacts() runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		ManifestDigest:          "manifest_digest",
		ProjectedManifestDigest: "projected_manifest_digest",
		BundleDigest:            "bundle_digest",
		RuntimeConfigDigest:     "runtime_config_digest",
		SpecDigest:              "spec_digest",
	}
}
