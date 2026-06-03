package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/driveradapter"
)

func TestRenderDriverConfigProjectionRejectsNonCanonicalControlDir(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{})
	details := testGenerationDetails(dir, "gen_pi_config_path")
	details.DriverID = "pi"
	details.OutputFormat = "pi_rpc_events_v1.0"
	details.Model = "sonnet"
	details.ManifestAnthropicBaseURL = "http://harness-model-proxy.internal:8082"
	details.ControlDirPath = filepath.Dir(details.ControlDirPath) + string(filepath.Separator) + "same" + string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(details.ControlDirPath)

	_, err := rt.renderDriverConfigProjection(StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     "pi",
		Generation:   details,
	})
	if err == nil || !strings.Contains(err.Error(), "driver config control dir path must be canonical absolute") {
		t.Fatalf("expected non-canonical driver config control dir error, got %v", err)
	}
}

func TestWriteDriverConfigProjectionReturnsNilWithoutSpecsOrRenderer(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{})
	details := testGenerationDetails(dir, "gen_shell_no_driver_config")
	details.DriverID = "sh"

	entries, err := rt.writeDriverConfigProjection(StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     "sh",
		Generation:   details,
	})
	if err != nil {
		t.Fatalf("write shell driver config projection: %v", err)
	}
	if entries != nil {
		t.Fatalf("shell driver config projection = %+v, want nil", entries)
	}
}

func TestRenderDriverConfigProjectionIsPure(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{})
	details := testGenerationDetails(dir, "gen_pi_render_driver_config")
	details.DriverID = string(agents.Pi)
	details.OutputFormat = agents.PiEventSchemaVersion

	req := StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     string(agents.Pi),
		Generation:   details,
	}
	rendered, err := rt.renderDriverConfigProjection(req)
	if err != nil {
		t.Fatalf("render pi driver config projection: %v", err)
	}
	if len(rendered.Entries) != 2 || len(rendered.Payloads) != 2 {
		t.Fatalf("unexpected rendered driver config projection: %+v", rendered)
	}
	for _, entry := range rendered.Entries {
		if !strings.HasPrefix(entry.SourceDigest, "sha256:") {
			t.Fatalf("entry %s missing source digest: %+v", entry.Name, entry)
		}
		if len(rendered.Payloads[entry.Name]) == 0 {
			t.Fatalf("entry %s missing rendered payload", entry.Name)
		}
		if _, err := os.Stat(entry.HostSourcePath); !os.IsNotExist(err) {
			t.Fatalf("render should not write %s, stat err=%v", entry.HostSourcePath, err)
		}
	}

	written, err := rt.writeDriverConfigProjection(req)
	if err != nil {
		t.Fatalf("write pi driver config projection: %v", err)
	}
	if len(written) != len(rendered.Entries) {
		t.Fatalf("written entries = %d want %d", len(written), len(rendered.Entries))
	}
	for _, entry := range written {
		if _, err := os.Stat(entry.HostSourcePath); err != nil {
			t.Fatalf("write should materialize %s: %v", entry.HostSourcePath, err)
		}
	}
}

func TestDriverConfigProjectionRenderersMatchMaterializationSpecs(t *testing.T) {
	driversWithSpecs := map[agents.ID]struct{}{}
	for _, driver := range agents.AllDriverSpecs() {
		specs := agents.DriverConfigMaterializationSpecsFor(driver.ID)
		if len(specs) == 0 {
			continue
		}
		driversWithSpecs[driver.ID] = struct{}{}

		renderer, ok := driveradapter.ConfigProjectionRendererFor(driver.ID)
		if !ok {
			t.Errorf("%s driver has config materialization specs but no renderer", driver.ID)
			continue
		}

		details := testGenerationDetails(t.TempDir(), "gen_"+string(driver.ID)+"_config_renderer")
		details.DriverID = string(driver.ID)
		payloads, err := renderer(details)
		if err != nil {
			t.Errorf("%s driver config renderer failed for baseline generation details: %v", driver.ID, err)
			continue
		}
		for _, spec := range specs {
			if _, ok := payloads[spec.Name]; !ok {
				t.Errorf("%s driver config renderer missing %q payload for materialization spec", driver.ID, spec.Name)
			}
		}
	}

	for _, driver := range agents.AllDriverSpecs() {
		if _, ok := driveradapter.ConfigProjectionRendererFor(driver.ID); ok {
			if _, ok := driversWithSpecs[driver.ID]; !ok {
				t.Errorf("%s driver has config renderer but no materialization specs", driver.ID)
			}
		}
	}
}
