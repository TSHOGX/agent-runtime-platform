package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

const checkpointImageManifestDigestForTest = "sha256:checkpoint-image-manifest"

func TestCreateSessionRejectsRemovedAgentInput(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := &Server{
		cfg: config.Config{
			SessionsRoot:     dir,
			SessionRetention: time.Hour,
			MaxSessions:      10,
			DefaultAgent:     "claude_code",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"agent":"opencode"}`))
	rec := httptest.NewRecorder()

	srv.createSession(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "agent input is no longer supported") {
		t.Fatalf("expected removed agent input error, got %s", rec.Body.String())
	}
}

func TestCreateSessionModeMapping(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantMode   string
		wantDriver string
		wantError  string
	}{
		{name: "omitted body", wantStatus: http.StatusBadRequest, wantError: "mode is required"},
		{name: "empty object", body: `{}`, wantStatus: http.StatusBadRequest, wantError: "mode is required"},
		{name: "empty mode", body: `{"mode":" "}`, wantStatus: http.StatusBadRequest, wantError: "mode is required"},
		{name: "agent mode", body: `{"mode":"agent"}`, wantStatus: http.StatusCreated, wantMode: "agent", wantDriver: "claude_code"},
		{name: "shell mode", body: `{"mode":"shell"}`, wantStatus: http.StatusCreated, wantMode: "shell", wantDriver: "sh"},
		{name: "unknown mode", body: `{"mode":"pi"}`, wantStatus: http.StatusBadRequest, wantError: "unsupported mode"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })
			cfg := testServerConfig(dir)
			srv := &Server{
				cfg:     cfg,
				store:   st,
				runtime: runtime.New(runtime.Config{}),
				watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
				hub:     events.NewHub(),
				log:     slog.Default(),
			}
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(http.MethodPost, "/api/sessions", body)
			rec := httptest.NewRecorder()
			srv.createSession(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status=%d want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus != http.StatusCreated {
				if !strings.Contains(rec.Body.String(), tc.wantError) {
					t.Fatalf("expected error %q, got %s", tc.wantError, rec.Body.String())
				}
				var count int
				if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions`).Scan(&count); err != nil {
					t.Fatalf("count sessions: %v", err)
				}
				if count != 0 {
					t.Fatalf("failed create should not persist sessions, got %d", count)
				}
				return
			}
			var created struct {
				ID        string `json:"id"`
				Mode      string `json:"mode"`
				ModeLabel string `json:"mode_label"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if created.Mode != tc.wantMode || created.ModeLabel != modeLabel(tc.wantMode) {
				t.Fatalf("public mode=%s label=%s want %s/%s", created.Mode, created.ModeLabel, tc.wantMode, modeLabel(tc.wantMode))
			}
			var driverID, storedMode string
			if err := st.DBForTest().QueryRowContext(ctx, `SELECT driver_id, mode FROM sessions WHERE id = ?`, created.ID).Scan(&driverID, &storedMode); err != nil {
				t.Fatalf("read stored session selector: %v", err)
			}
			if driverID != tc.wantDriver || storedMode != tc.wantMode {
				t.Fatalf("stored driver/mode=%s/%s want %s/%s", driverID, storedMode, tc.wantDriver, tc.wantMode)
			}
		})
	}
}

func TestValidateDriverStateForRuntimeLaunchPiHostState(t *testing.T) {
	agentHome := t.TempDir()
	volumes := sessionRuntimeDataVolumes{
		DriverHome: store.SessionDriverHomeVolume{HostPath: agentHome},
	}
	uninitialized := []byte(`{"driver_id":"pi","schema_version":1,"session_dir":"/agent-home/.pi/agent/sessions","state_kind":"pi_uninitialized"}`)
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{
		DriverID:           "pi",
		DriverStatePayload: uninitialized,
	}, volumes); err != nil {
		t.Fatalf("pi uninitialized launch state rejected: %v", err)
	}
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{DriverID: "pi"}, volumes); err == nil || !strings.Contains(err.Error(), "requires driver state payload") {
		t.Fatalf("expected missing pi driver state rejection, got %v", err)
	}

	sessionPayload := []byte(fmt.Sprintf(
		`{"driver_id":"pi","last_completed_turn_id":"9","schema_version":1,"selected_session_file":%q,"selected_session_id":"pi-session-1","selected_session_relpath":"session-1.jsonl","session_dir":%q,"state_kind":"pi_session"}`,
		agents.PiSessionDir+"/session-1.jsonl",
		agents.PiSessionDir,
	))
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{
		DriverID:           "pi",
		DriverStatePayload: sessionPayload,
	}, volumes); err == nil || !strings.Contains(err.Error(), "host file missing") {
		t.Fatalf("expected missing pi session file rejection, got %v", err)
	}

	sessionRoot := filepath.Join(agentHome, ".pi", "agent", "sessions")
	if err := os.MkdirAll(sessionRoot, 0o755); err != nil {
		t.Fatalf("create pi session root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionRoot, "session-1.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write pi session file: %v", err)
	}
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{
		DriverID:           "pi",
		DriverStatePayload: sessionPayload,
	}, volumes); err != nil {
		t.Fatalf("pi session launch state rejected: %v", err)
	}
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{DriverID: "sh"}, sessionRuntimeDataVolumes{}); err != nil {
		t.Fatalf("non-pi runtime launch should not require driver state: %v", err)
	}
}

func TestInterruptSessionUsesFrozenPlanFeaturePolicy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	session, _ := createServerPlannedActiveGeneration(t, ctx, st, cfg, owner.UUID, dir, "sess_interrupt_shell", agents.Shell)
	rt := &recordingRuntime{}
	srv := &Server{store: st, runtime: rt}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/interrupt", nil)
	rec := httptest.NewRecorder()
	srv.interruptSession(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected interrupt status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	if len(rt.interruptSessionIDs) != 1 || rt.interruptSessionIDs[0] != session.ID {
		t.Fatalf("runtime interrupt calls = %+v want %s", rt.interruptSessionIDs, session.ID)
	}
}

func TestInterruptSessionRejectsFrozenUnsupportedFeature(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	session, _ := createServerPlannedActiveGeneration(t, ctx, st, cfg, owner.UUID, dir, "sess_interrupt_agent", agents.ClaudeCode)
	rt := &recordingRuntime{}
	srv := &Server{store: st, runtime: rt}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/interrupt", nil)
	rec := httptest.NewRecorder()
	srv.interruptSession(rec, req, session.ID)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "interrupt is not supported") {
		t.Fatalf("expected unsupported interrupt conflict, got %d body %s", rec.Code, rec.Body.String())
	}
	if len(rt.interruptSessionIDs) != 0 {
		t.Fatalf("runtime interrupt should not be called: %+v", rt.interruptSessionIDs)
	}
}

func TestCreateSessionAgentModeRejectsShellDefault(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := &Server{
		cfg: config.Config{
			SessionsRoot:     dir,
			SessionRetention: time.Hour,
			MaxSessions:      10,
			DefaultAgent:     "sh",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "agent mode unavailable") {
		t.Fatalf("expected unavailable agent mode, got status=%d body=%s", rec.Code, rec.Body.String())
	}
	var count int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions`).Scan(&count); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if count != 0 {
		t.Fatalf("failed create should not persist sessions, got %d", count)
	}
}

func TestResolveAgentModeRequiresExplicitDefaultAgent(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	cfg.DefaultAgent = ""
	srv := &Server{cfg: cfg}

	if _, err := srv.resolveModeDeployment("agent"); err == nil || err.code != "default_unavailable" {
		t.Fatalf("expected missing default agent to make agent mode unavailable, got %v", err)
	}
}

func TestResolveModeDeploymentRejectsEmptyMode(t *testing.T) {
	srv := &Server{cfg: testServerConfig(t.TempDir())}

	if _, err := srv.resolveModeDeployment(""); err == nil || err.code != "unsupported_mode" || err.message != "unsupported mode" {
		t.Fatalf("expected empty mode to be unsupported, got %v", err)
	}
}

func TestRuntimeResourceHostIDFailsClosed(t *testing.T) {
	if _, err := runtimeResourceHostIDFrom(func() (string, error) { return " ", nil }); err == nil || !strings.Contains(err.Error(), "host id is required") {
		t.Fatalf("expected empty hostname error, got %v", err)
	}

	boom := errors.New("hostname failed")
	if _, err := runtimeResourceHostIDFrom(func() (string, error) { return "", boom }); !errors.Is(err, boom) {
		t.Fatalf("expected hostname error, got %v", err)
	}
}

func TestRuntimeResourceNftTableNameRequiresIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "whitespace only", value: " \t\n"},
		{name: "all invalid", value: "!!!"},
		{name: "underscore only", value: "___"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runtimeResourceNftTableName(tc.value); err == nil || !strings.Contains(err.Error(), "identifier is required") {
				t.Fatalf("expected generation id error, got %v", err)
			}
		})
	}
}

func TestRuntimeResourcePostStartProofValidatesRuntimeIdentity(t *testing.T) {
	instance := store.RuntimeResourceInstance{
		GenerationID:           "gen_post_start",
		HostID:                 "host-post-start",
		ContractID:             "contract_gen_post_start",
		SandboxContractVersion: store.SandboxContractVersion,
		RunscContainerID:       "harness-gen-post-start",
		RunscPlatform:          "systrap",
		RunscVersion:           "runsc test",
		RunscBinaryPath:        "/usr/local/bin/runsc-test",
		RunscBinaryDigest:      "sha256:runsc-test",
	}
	proof := serverPostStartProofForTest(instance)
	proof.HostID = ""
	proof.ContractID = ""
	proof.SandboxContractVersion = ""

	verified, err := runtimeResourcePostStartProof(instance, runtime.Result{PostStartProof: proof}, "bridge_startup_probe:passed; check=test")
	if err != nil {
		t.Fatalf("validate post-start proof: %v", err)
	}
	if verified.HostID != instance.HostID ||
		verified.ContractID != instance.ContractID ||
		verified.SandboxContractVersion != instance.SandboxContractVersion {
		t.Fatalf("server-owned proof fields were not filled from instance: %+v", verified)
	}

	mismatch := *serverPostStartProofForTest(instance)
	mismatch.RunscBinaryDigest = "sha256:changed"
	if _, err := runtimeResourcePostStartProof(instance, runtime.Result{PostStartProof: &mismatch}, "bridge_startup_probe:passed; check=test"); err == nil ||
		!strings.Contains(err.Error(), "runtime post-start proof runsc_binary_digest") {
		t.Fatalf("expected runsc digest mismatch, got %v", err)
	}
}

func TestPublicSessionDoesNotInferMissingMode(t *testing.T) {
	now := time.Now().UTC()
	got := publicSession(store.Session{
		ID:        "sess_missing_mode",
		UserID:    labUserID,
		Status:    string(sessionstate.Created),
		DriverID:  "claude_code",
		CreatedAt: now,
		UpdatedAt: now,
	})

	if got.Mode != "" || got.ModeLabel != "" {
		t.Fatalf("public session inferred missing mode as %q/%q", got.Mode, got.ModeLabel)
	}
}

func TestResolveDeploymentRequiresExplicitRuntimeProviderID(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	runtimeProvider := cfg.Harness.RuntimeProviders["local_runsc"]
	runtimeProvider.ProviderID = ""
	cfg.Harness.RuntimeProviders["local_runsc"] = runtimeProvider
	srv := &Server{cfg: cfg}

	if _, err := srv.resolveModeDeployment("agent"); err == nil || err.code != "provider_unsupported" {
		t.Fatalf("expected missing provider_id to make deployment unavailable, got %v", err)
	}
}

func TestDeploymentCapabilitiesAreProductSafe(t *testing.T) {
	dir := t.TempDir()
	srv := &Server{
		cfg: testServerConfig(dir),
	}
	req := httptest.NewRequest(http.MethodGet, "/api/deployment-capabilities", nil)
	rec := httptest.NewRecorder()
	srv.deploymentCapabilities(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, forbidden := range []string{"driver_id", "claude_code", "provider_id", "local_runsc", "host_path", "agent_manifest_digest"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("capability response exposed %q: %s", forbidden, body)
		}
	}
	var payload struct {
		SchemaVersion int    `json:"schema_version"`
		DefaultMode   string `json:"default_mode"`
		SessionModes  []struct {
			Mode           string  `json:"mode"`
			Label          string  `json:"label"`
			Visible        bool    `json:"visible"`
			CreateEnabled  bool    `json:"create_enabled"`
			DisabledReason *string `json:"disabled_reason"`
		} `json:"session_modes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	if payload.SchemaVersion != 1 || payload.DefaultMode != "agent" || len(payload.SessionModes) != 2 {
		t.Fatalf("unexpected capabilities: %+v", payload)
	}
	for _, mode := range payload.SessionModes {
		if mode.Mode != "agent" && mode.Mode != "shell" {
			t.Fatalf("capabilities exposed non-product mode: %+v", mode)
		}
		if !mode.CreateEnabled || mode.DisabledReason != nil {
			t.Fatalf("expected test deployment mode enabled, got %+v", mode)
		}
	}
}

func TestDeploymentCapabilitiesUseImageManifestGate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rootfs := filepath.Join(dir, "rootfs")
	writeServerTestAgentImageManifest(t, rootfs, agents.ClaudeCode)
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cfg := config.Config{
		SessionsRoot:     dir,
		SessionRetention: time.Hour,
		MaxSessions:      10,
		DefaultAgent:     "claude_code",
		RootFSPath:       rootfs,
	}
	applyServerTestDeploymentConfig(&cfg)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodGet, "/api/deployment-capabilities", nil)
	rec := httptest.NewRecorder()
	srv.deploymentCapabilities(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var capabilities struct {
		SessionModes []struct {
			Mode           string  `json:"mode"`
			Visible        bool    `json:"visible"`
			CreateEnabled  bool    `json:"create_enabled"`
			DisabledReason *string `json:"disabled_reason"`
		} `json:"session_modes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &capabilities); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	var sawAgent, sawShell bool
	for _, mode := range capabilities.SessionModes {
		switch mode.Mode {
		case "agent":
			sawAgent = true
			if !mode.Visible || !mode.CreateEnabled || mode.DisabledReason != nil {
				t.Fatalf("agent should remain available without sh in image: %+v", mode)
			}
		case "shell":
			sawShell = true
			if mode.Visible || mode.CreateEnabled || mode.DisabledReason == nil || *mode.DisabledReason != "missing_from_image" {
				t.Fatalf("shell should be hidden when sh is absent from image: %+v", mode)
			}
		}
	}
	if !sawAgent || !sawShell {
		t.Fatalf("expected agent and shell modes: %+v", capabilities.SessionModes)
	}

	shellReq := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"shell"}`))
	shellRec := httptest.NewRecorder()
	srv.createSession(shellRec, shellReq)
	if shellRec.Code != http.StatusBadRequest || !strings.Contains(shellRec.Body.String(), "shell mode unavailable") {
		t.Fatalf("expected shell rejection before persistence, got status=%d body=%s", shellRec.Code, shellRec.Body.String())
	}
	var count int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions`).Scan(&count); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if count != 0 {
		t.Fatalf("shell rejection should not persist a session, got %d", count)
	}

	agentReq := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	agentRec := httptest.NewRecorder()
	srv.createSession(agentRec, agentReq)
	if agentRec.Code != http.StatusCreated {
		t.Fatalf("expected agent session creation, got status=%d body=%s", agentRec.Code, agentRec.Body.String())
	}
}

func TestCreateSessionFailsClosedWhenRequiredManifestMissing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := &Server{
		cfg: config.Config{
			SessionsRoot:     dir,
			SessionRetention: time.Hour,
			MaxSessions:      10,
			DefaultAgent:     "claude_code",
			RootFSPath:       filepath.Join(dir, "missing-rootfs"),
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "agent mode unavailable") {
		t.Fatalf("expected fail-closed agent rejection, got status=%d body=%s", rec.Code, rec.Body.String())
	}
	var count int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions`).Scan(&count); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if count != 0 {
		t.Fatalf("manifest rejection should not persist a session, got %d", count)
	}
}

func TestDriverManifestInputDigestsUseSourceConfigAndImageManifest(t *testing.T) {
	dir := t.TempDir()
	rootfs := filepath.Join(dir, "rootfs")
	writeServerTestAgentImageManifest(t, rootfs, agents.ClaudeCode, agents.Shell)
	cfg := config.Config{
		DefaultAgent: "claude_code",
		RootFSPath:   rootfs,
	}
	applyServerTestDeploymentConfig(&cfg)
	srv := &Server{cfg: cfg}
	deployment, capabilityErr := srv.resolveModeDeployment("shell")
	if capabilityErr != nil {
		t.Fatalf("resolve shell deployment: %v", capabilityErr)
	}
	manifest, err := srv.loadAgentImageManifest()
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	digests, err := srv.driverManifestInputDigests(deployment)
	if err != nil {
		t.Fatalf("driver manifest input digests: %v", err)
	}
	if digests.AgentManifestDigest != manifest.Digest {
		t.Fatalf("agent manifest digest = %s want %s", digests.AgentManifestDigest, manifest.Digest)
	}
	want, err := runtimeConfigDigest(deployment.runtimeConfigPreimage("claude_code"))
	if err != nil {
		t.Fatalf("runtime config digest: %v", err)
	}
	if digests.RuntimeConfigDigest != want {
		t.Fatalf("runtime config digest = %s want %s", digests.RuntimeConfigDigest, want)
	}

	cfg.DefaultAgent = "pi"
	srv = &Server{cfg: cfg}
	deploymentWithPiDefault, capabilityErr := srv.resolveModeDeployment("shell")
	if capabilityErr != nil {
		t.Fatalf("resolve shell deployment with pi default: %v", capabilityErr)
	}
	changed, err := srv.driverManifestInputDigests(deploymentWithPiDefault)
	if err != nil {
		t.Fatalf("driver manifest input digests with pi default: %v", err)
	}
	if changed.RuntimeConfigDigest == digests.RuntimeConfigDigest {
		t.Fatalf("shell runtime config digest should change when deployment default agent changes")
	}
	if changed.AgentManifestDigest != digests.AgentManifestDigest {
		t.Fatalf("agent manifest digest should not change when manifest is unchanged: %s vs %s", changed.AgentManifestDigest, digests.AgentManifestDigest)
	}
}

func TestDriverManifestInputDigestsRequireExplicitDefaultAgent(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	srv := &Server{cfg: cfg}
	deployment, capabilityErr := srv.resolveModeDeployment("shell")
	if capabilityErr != nil {
		t.Fatalf("resolve shell deployment: %v", capabilityErr)
	}

	cfg.DefaultAgent = ""
	srv = &Server{cfg: cfg}
	if _, err := srv.driverManifestInputDigests(deployment); err == nil || !strings.Contains(err.Error(), "default agent is required") {
		t.Fatalf("expected missing default agent error, got %v", err)
	}

	cfg.DefaultAgent = "not-a-driver"
	srv = &Server{cfg: cfg}
	if _, err := srv.driverManifestInputDigests(deployment); err == nil || !strings.Contains(err.Error(), "default agent") {
		t.Fatalf("expected invalid default agent error, got %v", err)
	}
}

func TestSandboxContractDigestForPayloadFailsClosedOnCanonicalizationError(t *testing.T) {
	if got, err := sandboxContractDigestForPayload(map[string]any{"invalid": func() {}}); err == nil {
		t.Fatalf("expected canonicalization error, got digest %q", got)
	}
}

func TestSandboxContractInputEvidenceRequiresExplicitDefaultAgentAndSessionMode(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	srv := &Server{cfg: cfg}
	session := store.Session{
		ID:       "sess_contract_evidence",
		DriverID: "claude_code",
		Mode:     "agent",
	}

	cfg.DefaultAgent = ""
	srv = &Server{cfg: cfg}
	if _, err := srv.sandboxContractInputEvidenceFor(session, "claude_code"); err == nil || !strings.Contains(err.Error(), "default agent is required") {
		t.Fatalf("expected missing default agent error, got %v", err)
	}

	cfg.DefaultAgent = "claude_code"
	srv = &Server{cfg: cfg}
	session.Mode = ""
	if _, err := srv.sandboxContractInputEvidenceFor(session, "claude_code"); err == nil || !strings.Contains(err.Error(), "session mode is required") {
		t.Fatalf("expected missing session mode error, got %v", err)
	}
}

func TestSandboxContractPayloadRequiresRunscPlatformAndSessionMode(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	srv := &Server{cfg: cfg}
	session := store.Session{
		ID:       "sess_contract_payload",
		DriverID: "claude_code",
		Mode:     "agent",
	}
	details := store.RuntimeGenerationDetails{
		SessionID:      session.ID,
		GenerationID:   "gen_contract_payload",
		DriverID:       "claude_code",
		RunscPlatform:  "systrap",
		SandboxIPCIDR:  "10.241.0.2/29",
		RunscOverlay2:  "true",
		SandboxUID:     serverTestSandboxUID(),
		SandboxGID:     serverTestSandboxGID(),
		ControlDirPath: filepath.Join(dir, "control"),
		BridgeDirPath:  filepath.Join(dir, "bridge"),
	}

	missingPlatform := details
	missingPlatform.RunscPlatform = ""
	if _, err := srv.sandboxContractPayload(session, missingPlatform, runtime.GenerationArtifacts{}, "sha256:resource-identity", sessionRuntimeDataVolumes{}, nil); err == nil || !strings.Contains(err.Error(), "runsc platform is required") {
		t.Fatalf("expected missing runsc platform error, got %v", err)
	}
	if _, err := srv.runtimeResourceInstanceParams(missingPlatform, runtime.GenerationArtifacts{}, "host-a"); err == nil || !strings.Contains(err.Error(), "runsc platform is required") {
		t.Fatalf("expected missing runtime resource runsc platform error, got %v", err)
	}

	missingDriverStateDigest := details
	missingDriverStateDigest.DriverStateDigest = ""
	if _, err := srv.sandboxContractPayload(session, missingDriverStateDigest, runtime.GenerationArtifacts{}, "sha256:resource-identity", sessionRuntimeDataVolumes{}, nil); err == nil || !strings.Contains(err.Error(), "initial driver state digest is required") {
		t.Fatalf("expected missing driver state digest error, got %v", err)
	}

	details.DriverStateDigest = "sha256:driver-state"
	session.Mode = ""
	if _, err := srv.sandboxContractPayload(session, details, runtime.GenerationArtifacts{}, "sha256:resource-identity", sessionRuntimeDataVolumes{}, nil); err == nil || !strings.Contains(err.Error(), "session mode is required") {
		t.Fatalf("expected missing session mode error, got %v", err)
	}
}

func TestRuntimeConfigDigestFailsClosedOnCanonicalizationError(t *testing.T) {
	if got, err := runtimeConfigDigest(map[string]any{"invalid": func() {}}); err == nil {
		t.Fatalf("expected canonicalization error, got digest %q", got)
	}
}

func TestAgentsCatalogIsOperatorOnly(t *testing.T) {
	srv := &Server{cfg: config.Config{CookieName: "sid"}, runtime: runtime.New(runtime.Config{})}
	productReq := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	productReq.AddCookie(&http.Cookie{Name: "sid", Value: srv.signCookie(labUserID)})
	productRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(productRec, productReq)
	if productRec.Code != http.StatusNotFound {
		t.Fatalf("product /api/agents status=%d body=%s", productRec.Code, productRec.Body.String())
	}

	operatorReq := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	operatorRec := httptest.NewRecorder()
	srv.OperatorRoutes().ServeHTTP(operatorRec, operatorReq)
	if operatorRec.Code != http.StatusOK {
		t.Fatalf("operator /api/agents status=%d body=%s", operatorRec.Code, operatorRec.Body.String())
	}
	var payload struct {
		Drivers []struct {
			DriverID string `json:"driver_id"`
		} `json:"drivers"`
	}
	if err := json.Unmarshal(operatorRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode operator catalog: %v", err)
	}
	if len(payload.Drivers) != 3 ||
		payload.Drivers[0].DriverID != "claude_code" ||
		payload.Drivers[1].DriverID != "pi" ||
		payload.Drivers[2].DriverID != "sh" {
		t.Fatalf("unexpected operator catalog: %+v", payload.Drivers)
	}
}

func TestCreateSessionSoftLimitUsesPoolExhaustedEnvelope(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createServerTestSession(t, ctx, st, dir, "sess_existing", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.MaxSessions = 1
	cfg.Harness.MaxSessions = 1

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "pool_exhausted" {
		t.Fatalf("expected pool_exhausted, got %v", body)
	}
}

func TestEnsureActiveGenerationRequiresPersistedSessionMode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_missing_mode_start", string(sessionstate.Created), time.Now().UTC(), nil)
	session.Mode = ""
	srv := &Server{
		cfg:   cfg,
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}

	if _, err := srv.ensureActiveGeneration(ctx, session, store.GenerationLeaseOwner(owner.UUID)); err == nil || !strings.Contains(err.Error(), "session mode is required") {
		t.Fatalf("expected missing session mode error, got %v", err)
	}
	var count int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&count); err != nil {
		t.Fatalf("count runtime generations: %v", err)
	}
	if count != 0 {
		t.Fatalf("missing mode session should not allocate generation, got %d", count)
	}
}

func TestCloseSessionReleasesSoftLimitWithoutDeletingHistory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := testServerConfig(dir)
	cfg.SessionRetention = 0
	cfg.MaxSessions = 1
	cfg.Harness.MaxSessions = 1
	volumeConfig, err := serverDataVolumeConfigForTest(cfg)
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	now := time.Now().UTC()
	oldSession := store.Session{
		ID:        "sess_retained",
		UserID:    labUserID,
		Status:    string(sessionstate.Created),
		DriverID:  "claude_code",
		Mode:      store.ModeForDriver("claude_code"),
		CreatedAt: now,
		UpdatedAt: now,
	}
	oldWorkspacePath := filepath.Join(cfg.SessionsRoot, oldSession.ID)
	if err := os.MkdirAll(oldWorkspacePath, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, oldSession); err != nil {
		t.Fatalf("create retained session: %v", err)
	}
	oldDriverHome, err := st.ProvisionSessionDriverHome(ctx, store.ProvisionSessionDriverHomeParams{
		SessionID: oldSession.ID,
		Driver:    oldSession.DriverID,
		Config:    volumeConfig,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision driver home: %v", err)
	}
	if _, err := st.AddMessage(ctx, oldSession.ID, "user", "keep this"); err != nil {
		t.Fatalf("add message: %v", err)
	}
	if err := st.UpsertArtifact(ctx, store.Artifact{
		SessionID: oldSession.ID,
		Path:      "report.txt",
		Size:      12,
		ModTime:   now,
	}); err != nil {
		t.Fatalf("upsert artifact: %v", err)
	}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected quota rejection before close, got %d body %s", rec.Code, rec.Body.String())
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/sessions/"+oldSession.ID, nil)
	deleteRec := httptest.NewRecorder()
	srv.destroySession(deleteRec, deleteReq, oldSession.ID)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected close status 200, got %d body %s", deleteRec.Code, deleteRec.Body.String())
	}

	createReq := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	createRec := httptest.NewRecorder()
	srv.createSession(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected create after close, got %d body %s", createRec.Code, createRec.Body.String())
	}

	closed, err := st.GetSession(ctx, oldSession.ID)
	if err != nil {
		t.Fatalf("get closed session: %v", err)
	}
	if closed.Status != string(sessionstate.Destroyed) {
		t.Fatalf("closed session should preserve terminal state: %+v", closed)
	}
	if _, err := os.Stat(oldWorkspacePath); err != nil {
		t.Fatalf("workspace should remain after close: %v", err)
	}
	if _, err := os.Stat(oldDriverHome.HostPath); err != nil {
		t.Fatalf("agent home should remain after close: %v", err)
	}
	retainedDriverHome, err := st.GetSessionDriverHomeVolume(ctx, oldSession.ID, oldSession.DriverID)
	if err != nil {
		t.Fatalf("get retained driver home: %v", err)
	}
	if retainedDriverHome.HostPath != oldDriverHome.HostPath {
		t.Fatalf("driver home row should be retained: got=%+v want=%+v", retainedDriverHome, oldDriverHome)
	}
	messages, err := st.ListMessages(ctx, oldSession.ID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Content != "keep this" {
		t.Fatalf("expected retained message, got %+v", messages)
	}
	artifacts, err := st.ListArtifacts(ctx, oldSession.ID)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].Path != "report.txt" {
		t.Fatalf("expected retained artifact, got %+v", artifacts)
	}
}

func TestCreateSessionUsesNullExpiryWhenSessionRetentionDisabled(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cfg := testServerConfig(dir)
	cfg.SessionRetention = 0
	cfg.Harness.SessionRetention = config.Duration{Duration: 0}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d body %s", rec.Code, rec.Body.String())
	}
	var created store.Session
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode session: %v", err)
	}
	if created.ExpiresAt != nil {
		t.Fatalf("expected nil expires_at in response, got %v", created.ExpiresAt)
	}
	got, err := st.GetSession(ctx, created.ID)
	if err != nil {
		t.Fatalf("get created session: %v", err)
	}
	if got.ExpiresAt != nil {
		t.Fatalf("expected nil stored expires_at, got %v", got.ExpiresAt)
	}
	changed, err := st.SweepExpiredSessions(ctx, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatalf("sweep sessions: %v", err)
	}
	if changed != 0 {
		t.Fatalf("expected NULL expires_at session to be preserved, swept %d", changed)
	}
	got, err = st.GetSession(ctx, created.ID)
	if err != nil {
		t.Fatalf("get session after sweep: %v", err)
	}
	if got.Status != string(sessionstate.Created) {
		t.Fatalf("expected session to remain created, got %s", got.Status)
	}
}

func TestCreateSessionDefersWorkspaceProvisioning(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d body %s", rec.Code, rec.Body.String())
	}

	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	session, err := st.GetSession(ctx, created.ID)
	if err != nil {
		t.Fatalf("get created session: %v", err)
	}
	workspacePath := filepath.Join(cfg.SessionsRoot, session.ID)
	if _, err := os.Stat(workspacePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("session create should defer workspace provisioning, stat err=%v path=%s", err, workspacePath)
	}
	var workspaceRows int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM session_workspaces WHERE session_id = ?`, session.ID).Scan(&workspaceRows); err != nil {
		t.Fatalf("count workspace evidence rows: %v", err)
	}
	if workspaceRows != 0 {
		t.Fatalf("session create should not synthesize workspace evidence rows, got %d", workspaceRows)
	}
}

func TestCreateSessionUsesPublicSessionDTO(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe("")
	defer cancelEvents()
	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     hub,
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d body %s", rec.Code, rec.Body.String())
	}
	assertPublicSessionJSONOmitsHostFields(t, rec.Body.Bytes())
	var created map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created["id"] == "" || created["mode"] != "agent" || created["mode_label"] != "Agent" {
		t.Fatalf("unexpected create response: %v", created)
	}

	select {
	case event := <-eventsCh:
		if event.Type != "session.created" {
			t.Fatalf("event type=%s want session.created", event.Type)
		}
		payload, err := json.Marshal(event.Payload)
		if err != nil {
			t.Fatalf("marshal event payload: %v", err)
		}
		assertPublicSessionJSONOmitsHostFields(t, payload)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for session.created event")
	}
}

func TestSessionReadResponsesUsePublicSessionDTO(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_public", string(sessionstate.RunningIdle), now, nil)
	restoreMS := int64(123)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = ?
WHERE id = ?`, filepath.Join(dir, "checkpoints", session.ID), restoreMS, session.ID); err != nil {
		t.Fatalf("seed host-only fields: %v", err)
	}

	srv := &Server{
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/sessions/"+session.ID, nil)
	getRec := httptest.NewRecorder()
	srv.getSession(getRec, getReq, session.ID)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected get status 200, got %d body %s", getRec.Code, getRec.Body.String())
	}
	assertPublicSessionJSONOmitsHostFields(t, getRec.Body.Bytes())
	assertContains(t, getRec.Body.String(), `"restore_ms":123`)

	listReq := httptest.NewRequest(http.MethodGet, "/api/sessions", nil)
	listRec := httptest.NewRecorder()
	srv.listSessions(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected list status 200, got %d body %s", listRec.Code, listRec.Body.String())
	}
	assertPublicSessionJSONOmitsHostFields(t, listRec.Body.Bytes())
	assertContains(t, listRec.Body.String(), `"id":"sess_public"`)
}

func TestPublicEventSanitizerStripsRuntimePrivateFields(t *testing.T) {
	event := publicEvent(events.Event{
		EventID:      12,
		Type:         "session.checkpoint_retired",
		SessionID:    "sess_public_event",
		GenerationID: "gen_private",
		Payload: map[string]any{
			"session_status":           "running_idle",
			"session_updated_at":       "2026-05-26T01:02:00Z",
			"session_last_activity_at": "2026-05-26T00:30:00Z",
			"generation_id":            "gen_private",
			"active_generation_id":     "gen_private",
			"driver_id":                "claude_code",
			"agent":                    "claude",
			"restore_id":               "restore_private",
			"host_path":                filepath.Join(t.TempDir(), "private"),
			"driver_state": map[string]any{
				"state_digest": "sha256:private",
			},
			"data_volumes": map[string]any{
				"workspace": map[string]any{"host_path": "/host/workspace"},
			},
		},
	})
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal public event: %v", err)
	}
	body := string(data)
	for _, forbidden := range []string{
		`"generation_id"`,
		`"active_generation_id"`,
		`"driver_id"`,
		`"agent"`,
		`"restore_id"`,
		`"host_path"`,
		`"driver_state"`,
		`"data_volumes"`,
		"gen_private",
		"claude_code",
		"restore_private",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("public event exposed %q: %s", forbidden, body)
		}
	}
	assertContains(t, body, `"event_id":12`)
	assertContains(t, body, `"session_id":"sess_public_event"`)
	assertContains(t, body, `"session_status":"running_idle"`)
	assertContains(t, body, `"session_updated_at":"2026-05-26T01:02:00Z"`)
}

func TestMonitorIdleSessionsSkipsHostNetwork(t *testing.T) {
	srv := &Server{
		cfg: config.Config{
			RunscNetwork: "host",
		},
		log: slog.Default(),
	}

	if err := srv.MonitorIdleSessions(context.Background()); err != nil {
		t.Fatalf("expected idle monitor to exit cleanly in host mode, got %v", err)
	}
}

func TestMonitorIdleSessionsReEnablesCheckpointedSessionsWhenCheckpointDisabled(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := store.Session{
		ID:        "sess_checkpointed",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointed),
		DriverID:  "claude_code",
		Mode:      store.ModeForDriver("claude_code"),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	srv := &Server{
		cfg: config.Config{
			RunscNetwork: "sandbox",
		},
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}
	if err := srv.MonitorIdleSessions(context.Background()); err != nil {
		t.Fatalf("monitor idle sessions: %v", err)
	}
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.Checkpointed) {
		t.Fatalf("disabled monitor should leave checkpointed session alone, got %s", got.Status)
	}
}

func TestBridgeCheckpointReadyRequiresFreshHeartbeatAndMarker(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()

	if bridgeCheckpointReady(dir, now, time.Second) {
		t.Fatal("empty bridge dir should not be checkpoint-ready")
	}
	if err := bridge.TouchHeartbeat(dir, bridge.BridgeHeartbeatFile, now); err != nil {
		t.Fatalf("touch heartbeat: %v", err)
	}
	if bridgeCheckpointReady(dir, now, time.Second) {
		t.Fatal("missing ready marker should not be checkpoint-ready")
	}
	if err := bridge.TouchCheckpointReady(dir, now); err != nil {
		t.Fatalf("touch ready: %v", err)
	}
	if !bridgeCheckpointReady(dir, now, time.Second) {
		t.Fatal("fresh heartbeat and ready marker should be checkpoint-ready")
	}
	if bridgeCheckpointReady(dir, now, 0) {
		t.Fatal("non-positive heartbeat interval should not be checkpoint-ready")
	}
	if bridgeCheckpointReady(dir, now.Add(10*time.Second), time.Second) {
		t.Fatal("stale bridge control files should not be checkpoint-ready")
	}
}

func TestMonitorIdleSessionsRequiresPositiveTimingConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)

	tests := []struct {
		name string
		edit func(*config.Config)
		want string
	}{
		{
			name: "monitor interval",
			edit: func(cfg *config.Config) {
				cfg.Harness.Checkpoint.MonitorInterval = config.Duration{}
			},
			want: "checkpoint monitor interval must be > 0",
		},
		{
			name: "idle threshold",
			edit: func(cfg *config.Config) {
				cfg.Harness.Checkpoint.IdleThreshold = config.Duration{}
			},
			want: "checkpoint idle threshold must be > 0",
		},
		{
			name: "bridge heartbeat interval",
			edit: func(cfg *config.Config) {
				cfg.Harness.Bridge.HeartbeatInterval = config.Duration{}
			},
			want: "bridge heartbeat interval must be > 0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testServerConfig(filepath.Join(dir, tc.name))
			cfg.Harness.Checkpoint.AutoEnabled = true
			cfg.Harness.Checkpoint.MonitorInterval = config.Duration{Duration: time.Minute}
			cfg.Harness.Checkpoint.IdleThreshold = config.Duration{Duration: time.Minute}
			tc.edit(&cfg)
			srv := &Server{
				cfg:   cfg,
				store: st,
				hub:   events.NewHub(),
				log:   slog.Default(),
			}
			srv.SetOwnerUUID(owner.UUID)
			err := srv.MonitorIdleSessions(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("monitor err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestMonitorIdleSessionsCheckpointsEligibleGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_auto_checkpoint", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	enableSessionAutoCheckpoint(t, ctx, st, session.ID)
	cfg := testServerConfig(dir)
	cfg.Harness.Checkpoint.AutoEnabled = true
	cfg.Harness.Checkpoint.IdleThreshold = config.Duration{Duration: time.Nanosecond}
	cfg.Harness.Checkpoint.MonitorInterval = config.Duration{Duration: time.Hour}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if err := bridge.TouchHeartbeat(details.BridgeDirPath, bridge.BridgeHeartbeatFile, time.Now().UTC()); err != nil {
		t.Fatalf("touch heartbeat: %v", err)
	}
	if err := bridge.TouchCheckpointReady(details.BridgeDirPath, time.Now().UTC()); err != nil {
		t.Fatalf("touch ready: %v", err)
	}
	mutateServerRuntimeArtifactDigestMirrors(t, ctx, st, allocation.GenerationID)
	rt := &recordingRuntime{}
	runCtx, cancel := context.WithCancel(ctx)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.MonitorIdleSessions(runCtx)
	}()
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.Checkpointed))
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("monitor exit err=%v, want context canceled", err)
	}

	checkpoints := rt.checkpointRequests()
	if len(checkpoints) != 1 {
		t.Fatalf("checkpoint requests=%d want 1: %+v", len(checkpoints), checkpoints)
	}
	if checkpoints[0].SessionID != session.ID ||
		checkpoints[0].GenerationID != allocation.GenerationID ||
		checkpoints[0].CheckpointPath != details.CheckpointPath ||
		checkpoints[0].Generation.GenerationID != allocation.GenerationID ||
		checkpoints[0].Generation.RunscContainerID != details.RunscContainerID ||
		checkpoints[0].Generation.RunscOverlay2 != details.RunscOverlay2 {
		t.Fatalf("unexpected checkpoint request: %+v details=%+v", checkpoints[0], details)
	}
	plan, err := st.GetGenerationPlan(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get checkpointed plan: %v", err)
	}
	var generationStatus, networkState, resourceState, checkpointPath, checkpointBundle, checkpointRuntimeConfig, checkpointManifest, checkpointPlan, checkpointImageManifest string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, COALESCE(r.checkpoint_path, ''),
       COALESCE(g.checkpoint_bundle_digest, ''), COALESCE(g.checkpoint_runtime_config_digest, ''), COALESCE(g.checkpoint_control_manifest_digest, ''),
       COALESCE(g.checkpoint_plan_digest, ''), COALESCE(g.checkpoint_image_manifest_digest, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(
		&generationStatus, &networkState, &resourceState, &checkpointPath,
		&checkpointBundle, &checkpointRuntimeConfig, &checkpointManifest, &checkpointPlan, &checkpointImageManifest,
	); err != nil {
		t.Fatalf("query checkpointed generation: %v", err)
	}
	if generationStatus != "checkpointed" ||
		networkState != "reserved_checkpointed" ||
		resourceState != "reserved_checkpointed" ||
		checkpointPath != details.CheckpointPath ||
		checkpointBundle != "bundle_digest" ||
		checkpointRuntimeConfig != "runtime_config_digest" ||
		checkpointManifest != "projected_manifest_digest" ||
		checkpointPlan != plan.PlanDigest ||
		checkpointImageManifest != checkpointImageManifestDigestForTest {
		t.Fatalf("unexpected checkpoint metadata: generation=%s network=%s resource=%s path=%s bundle=%s runtime=%s manifest=%s plan=%s image_manifest=%s want_plan=%s",
			generationStatus, networkState, resourceState, checkpointPath, checkpointBundle, checkpointRuntimeConfig, checkpointManifest, checkpointPlan, checkpointImageManifest, plan.PlanDigest)
	}
}

func TestMonitorIdleSessionsAbortsFailedCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_auto_checkpoint_fail", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	enableSessionAutoCheckpoint(t, ctx, st, session.ID)
	cfg := testServerConfig(dir)
	cfg.Harness.Checkpoint.AutoEnabled = true
	cfg.Harness.Checkpoint.IdleThreshold = config.Duration{Duration: time.Nanosecond}
	cfg.Harness.Checkpoint.MonitorInterval = config.Duration{Duration: time.Hour}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if err := bridge.TouchHeartbeat(details.BridgeDirPath, bridge.BridgeHeartbeatFile, time.Now().UTC()); err != nil {
		t.Fatalf("touch heartbeat: %v", err)
	}
	if err := bridge.TouchCheckpointReady(details.BridgeDirPath, time.Now().UTC()); err != nil {
		t.Fatalf("touch ready: %v", err)
	}
	rt := &recordingRuntime{checkpointErr: errors.New("checkpoint boom")}
	runCtx, cancel := context.WithCancel(ctx)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.MonitorIdleSessions(runCtx)
	}()
	waitForCheckpointRequests(t, ctx, rt, 1)
	waitForGenerationStatus(t, ctx, st, allocation.GenerationID, "idle")
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("monitor exit err=%v, want context canceled", err)
	}
	var generationStatus, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query aborted generation: %v", err)
	}
	if generationStatus != "idle" || networkState != "live" || resourceState != "live" {
		t.Fatalf("checkpoint failure should return generation live idle, got generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
}

func TestSendMessageAllocatesGenerationAndQueuesBridgeTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_turn", string(sessionstate.Created), time.Now().UTC(), nil)

	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningActive))

	var generations, networkRows, resourceRows, queuedTurns, userMessages int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&generations); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM network_profiles WHERE session_id = ? AND allocation_state = 'live'`, session.ID).Scan(&networkRows); err != nil {
		t.Fatalf("count network rows: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM runtime_generation_resources r
JOIN runtime_generations g ON g.generation_id = r.generation_id
WHERE g.session_id = ? AND r.resource_state = 'live'`, session.ID).Scan(&resourceRows); err != nil {
		t.Fatalf("count resource rows: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'queued' AND generation_id IS NULL`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE session_id = ? AND role = 'user' AND content = 'hello'`, session.ID).Scan(&userMessages); err != nil {
		t.Fatalf("count user messages: %v", err)
	}
	if generations != 1 || networkRows != 1 || resourceRows != 1 || queuedTurns != 1 || userMessages != 1 {
		t.Fatalf("unexpected bridge enqueue rows: generations=%d network=%d resources=%d queued_turns=%d user_messages=%d", generations, networkRows, resourceRows, queuedTurns, userMessages)
	}
	var generationID string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT generation_id FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&generationID); err != nil {
		t.Fatalf("query generation id: %v", err)
	}
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if err != nil {
		t.Fatalf("fresh start should persist generation plan: %v", err)
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		t.Fatalf("fresh start persisted invalid generation plan: %v\n%s", err, plan.CanonicalPayload)
	}
	var planPayload map[string]any
	if err := json.Unmarshal(plan.CanonicalPayload, &planPayload); err != nil {
		t.Fatalf("decode generation plan: %v", err)
	}
	identity := planPayload["identity"].(map[string]any)
	runscPin := planPayload["runsc_pin"].(map[string]any)
	if identity["session_id"] != session.ID || identity["generation_id"] != generationID || runscPin["binary_digest"] != "sha256:runsc-test" {
		t.Fatalf("generation plan did not capture launch identity/runsc pin: %s", plan.CanonicalPayload)
	}
	if _, ok := planPayload["projection_digests"]; ok {
		t.Fatalf("generation plan must not embed projection digests: %s", plan.CanonicalPayload)
	}
	driverPlan := planPayload["driver"].(map[string]any)
	driverCapabilities := driverPlan["capability_snapshot"].(map[string]any)
	driverFeatures := driverCapabilities["features"].(map[string]any)
	featurePolicy := planPayload["feature_policy"].(map[string]any)
	providerPlan := planPayload["runtime_provider"].(map[string]any)
	providerCapabilities := providerPlan["capability_snapshot"].(map[string]any)
	if driverFeatures["compaction"] != "supported" ||
		driverFeatures["interrupt"] != "unsupported" ||
		featurePolicy["compaction"] != "required" ||
		featurePolicy["interrupt"] != "unsupported" ||
		featurePolicy["legacy_supports_compaction"] != true ||
		featurePolicy["legacy_supports_interrupt"] != false ||
		providerCapabilities["vocabulary_version"] != "1" {
		t.Fatalf("generation plan did not freeze typed capability policy: %s", plan.CanonicalPayload)
	}
	projections, err := st.ListGenerationPlanProjections(ctx, generationID)
	if err != nil {
		t.Fatalf("list generation plan projections: %v", err)
	}
	if len(projections) != 6 {
		t.Fatalf("projection count=%d want 6: %+v", len(projections), projections)
	}
	projectionKinds := map[string]string{}
	for _, projection := range projections {
		if projection.PlanDigest != plan.PlanDigest {
			t.Fatalf("projection %s plan digest=%s want %s", projection.ProjectionKind, projection.PlanDigest, plan.PlanDigest)
		}
		if !strings.HasPrefix(projection.PayloadDigest, "sha256:") {
			t.Fatalf("projection %s payload digest is not sha256: %s", projection.ProjectionKind, projection.PayloadDigest)
		}
		projectionKinds[projection.ProjectionKind] = projection.PayloadDigest
	}
	contract, err := st.GetSandboxContractForGeneration(ctx, session.ID, generationID)
	if err != nil {
		t.Fatalf("load sandbox contract: %v", err)
	}
	if projectionKinds["sandbox_contract"] != contract.SandboxContractDigest ||
		projectionKinds["control_manifest"] == "" ||
		projectionKinds["oci_spec"] == "" {
		t.Fatalf("unexpected projection digests: %+v", projectionKinds)
	}
}

func TestExistingStartVerifiesStoredGenerationPlanEvidence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_plan_verify_start", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	if err := srv.startEnsuredGeneration(ctx, session, ensuredGeneration{Allocation: allocation, IsNew: true}, startFailureInputAcceptable); err != nil {
		t.Fatalf("initial start: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, session.ID, string(sessionstate.RunningIdle), nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'idle'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("mark generation idle: %v", err)
	}
	plan, err := st.GetGenerationPlan(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation plan: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(plan.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan: %v", err)
	}
	payload["runsc_pin"].(map[string]any)["binary_digest"] = "sha256:changed-runsc"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical corrupt plan: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), store.GenerationPlanDigest(canonical), allocation.GenerationID); err != nil {
		t.Fatalf("corrupt stored plan: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET plan_digest = ?
WHERE generation_id = ?`, store.GenerationPlanDigest(canonical), allocation.GenerationID); err != nil {
		t.Fatalf("align corrupt plan projection digests: %v", err)
	}
	rt = &recordingRuntime{}
	srv = &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError ||
		!strings.Contains(rec.Body.String(), "generation plan runsc pin mismatch") {
		t.Fatalf("expected frozen evidence failure, got status %d body %s", rec.Code, rec.Body.String())
	}
	_, starts := rt.requests()
	if len(starts) != 0 {
		t.Fatalf("runtime start should not run after frozen evidence mismatch: %+v", starts)
	}
}

func TestFreshStartStoresGenerationPlanBeforeMaterializationAndNetworkPrepare(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_plan_before_network", string(sessionstate.Created), time.Now().UTC(), nil)
	rt := &planOrderRuntime{store: st, t: t}
	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	if !rt.planSeenBeforeMaterializeRender {
		t.Fatalf("runtime artifact materialization render ran before generation plan rows were stored")
	}
	if !rt.planSeenBeforeMaterialize {
		t.Fatalf("runtime artifact materialization ran before generation plan rows were stored")
	}
	if !rt.projectionVerificationBeforeMaterialize {
		t.Fatalf("runtime artifact materialization ran before generation plan projections were verified")
	}
	if !rt.runtimeResourceClaimedBeforeMaterialize {
		t.Fatalf("runtime artifact materialization ran before claiming the runtime resource")
	}
	if !rt.planSeenBeforeNetwork {
		t.Fatalf("network preparation ran before generation plan rows were stored")
	}
	if !rt.projectionVerificationObserved {
		t.Fatalf("network preparation ran before generation plan projections were verified")
	}
	if !rt.runtimeResourceClaimedBeforeNetwork {
		t.Fatalf("network preparation ran before claiming the runtime resource")
	}
}

func TestFreshStartReverifiesStoredGenerationPlanProjectionsBeforeMaterialization(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_projection_reverify_materialize", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &corruptProjectionBeforeMaterializeRuntime{store: st, t: t}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	err = srv.startEnsuredGeneration(ctx, session, ensuredGeneration{Allocation: allocation, IsNew: true}, startFailureInputAcceptable)
	if err == nil || !strings.Contains(err.Error(), "generation plan projection bundle digest mismatch") {
		t.Fatalf("expected pre-materialization projection mismatch, got %v", err)
	}
	if !rt.corrupted {
		t.Fatalf("test runtime did not corrupt stored projection row")
	}
	if rt.materialized {
		t.Fatalf("materialization should not run after stored projection mismatch")
	}
}

func TestSendMessageReusesActiveGenerationArtifacts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_reuse", string(sessionstate.Created), time.Now().UTC(), nil)
	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	atomic.StoreInt64(&instantRuntimePrepareCalls, 0)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"first"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected first status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningActive))
	if got := atomic.LoadInt64(&instantRuntimePrepareCalls); got != 2 {
		t.Fatalf("first turn prepare calls=%d want 2", got)
	}
	var generationID string
	var firstTurnID int64
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.generation_id, t.id
FROM runtime_generations g
JOIN turns t ON t.session_id = g.session_id
WHERE g.session_id = ?
  AND t.status = 'queued'
  AND t.content = 'first'`, session.ID).Scan(&generationID, &firstTurnID); err != nil {
		t.Fatalf("query first queued turn: %v", err)
	}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	if grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    session.ID,
		GenerationID: generationID,
		Owner:        leaseOwner,
		RequestID:    "req_first",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	}); err != nil || !ok || grant.TurnID != firstTurnID {
		t.Fatalf("claim first turn: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, store.AckStartedParams{
		SessionID:       session.ID,
		GenerationID:    generationID,
		TurnID:          firstTurnID,
		Owner:           leaseOwner,
		SandboxSourceIP: serverSandboxSourceIPForGeneration(t, ctx, st, generationID),
		LeaseTTL:        time.Minute,
		Now:             time.Now().UTC(),
	}); err != nil {
		t.Fatalf("ack first turn started: %v", err)
	}
	if _, err := st.CompleteTurn(ctx, store.CompleteTurnParams{
		SessionID:      session.ID,
		GenerationID:   generationID,
		TurnID:         firstTurnID,
		Owner:          leaseOwner,
		TerminalStatus: "completed",
		Now:            time.Now().UTC(),
	}); err != nil {
		t.Fatalf("complete first turn: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, session.ID, string(sessionstate.RunningIdle), nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"second"}`))
	rec = httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected second status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningActive))
	if got := atomic.LoadInt64(&instantRuntimePrepareCalls); got != 2 {
		t.Fatalf("active generation should reuse prepared artifacts, prepare calls=%d", got)
	}
	var completedTurns, queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'completed'`, session.ID).Scan(&completedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'queued' AND content = 'second'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count queued turns: %v", err)
	}
	if completedTurns != 1 || queuedTurns != 1 {
		t.Fatalf("unexpected turn statuses after reuse: completed=%d queued=%d", completedTurns, queuedTurns)
	}
}

func TestSendMessageAllocatesReplacementGenerationForFailedActiveGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_send_failed_generation", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate old generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark old generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, old, owner.UUID, mustRuntimeResourceHostID(t), time.Now().UTC())
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    session.ID,
		GenerationID: old.GenerationID,
		Owner:        old.Owner,
		ErrorClass:   "orchestrator_restart_reconnect_grace_expired",
		Reason:       "orchestrator_restart_reconnect_grace_expired",
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("fail old generation: %v", err)
	}

	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after failed generation"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.ActiveGenerationID == "" || gotSession.ActiveGenerationID == old.GenerationID {
		t.Fatalf("active generation was not replaced: %q old=%q", gotSession.ActiveGenerationID, old.GenerationID)
	}
	volumeConfig, err := srv.dataVolumeProvisionerConfig()
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	driverHome, err := st.VerifySessionDriverHomeVolume(ctx, store.VerifySessionDriverHomeVolumeParams{
		SessionID: session.ID,
		Driver:    session.DriverID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify replacement driver home: %v", err)
	}
	var oldStatus, oldNetwork, oldResources, newStatus, newNetwork, newResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("old generation not fenced/reclaimable: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, gotSession.ActiveGenerationID).Scan(&newStatus, &newNetwork, &newResources); err != nil {
		t.Fatalf("query replacement generation: %v", err)
	}
	if newStatus != "idle" || newNetwork != "live" || newResources != "live" {
		t.Fatalf("replacement generation not live idle: status=%s network=%s resources=%s", newStatus, newNetwork, newResources)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'after failed generation'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count queued turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("queued replacement turn count=%d want 1", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 2 || len(startRequests) != 1 {
		t.Fatalf("runtime calls prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	if startRequests[0].GenerationID != gotSession.ActiveGenerationID {
		t.Fatalf("unexpected replacement start request: %+v", startRequests[0])
	}
	if startRequests[0].AgentHomeHostPath != driverHome.HostPath {
		t.Fatalf("replacement start did not use driver home volume: start=%+v home=%+v", startRequests[0], driverHome)
	}
}

func TestSendMessageRestoresCheckpointedGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, allocation.GenerationID, "restore_manifest_digest", "runsc restore-test")
	snapshot := addServerGenerationPlanSkillsSnapshot(t, ctx, st, allocation.GenerationID)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, allocation.GenerationID, time.Now().UTC())
	mutateServerRuntimeArtifactDigestMirrors(t, ctx, st, allocation.GenerationID)

	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restored session: %v", err)
	}
	if gotSession.ActiveGenerationID != allocation.GenerationID || gotSession.Status != string(sessionstate.RunningActive) {
		t.Fatalf("unexpected restored session: %+v allocation=%+v", gotSession, allocation)
	}
	var generationStatus, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query restored generation: %v", err)
	}
	if generationStatus != "idle" || networkState != "live" || resourceState != "live" {
		t.Fatalf("restored generation not live idle: status=%s network=%s resources=%s", generationStatus, networkState, resourceState)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'after restore'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count restored queued turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("queued restored turn count=%d want 1", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 0 || len(startRequests) != 1 {
		t.Fatalf("restore should skip prepare and start once: prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	start := startRequests[0]
	if start.GenerationID != allocation.GenerationID ||
		!start.RestoreFromCheckpoint ||
		start.PreparedArtifacts.ManifestDigest != "restore_manifest_digest" ||
		start.PreparedArtifacts.ProjectedManifestDigest != "restore_manifest_digest" ||
		start.PreparedArtifacts.BundleDigest != "bundle_digest" ||
		start.PreparedArtifacts.RuntimeConfigDigest != "runtime_config_digest" ||
		start.PreparedArtifacts.SpecDigest != "spec_digest" ||
		start.PreparedArtifacts.RunscVersion != "runsc restore-test" ||
		start.PreparedArtifacts.RunscBinaryPath != "/usr/local/bin/runsc-test" ||
		start.PreparedArtifacts.RunscBinaryDigest != "sha256:runsc-test" ||
		start.Generation.NetworkAllocationState != "recreating" {
		t.Fatalf("unexpected restore start request: %+v", start)
	}
	if len(start.ContentSnapshots) != 1 ||
		start.ContentSnapshots[0].Kind != store.ContentSnapshotKindSkills ||
		start.ContentSnapshots[0].Digest != snapshot.Digest ||
		start.ContentSnapshots[0].ImmutableHostPath != snapshot.ImmutableHostPath ||
		start.ContentSnapshots[0].MountDestination != store.ContentSnapshotSkillsMount {
		t.Fatalf("restore start content snapshots = %+v want %+v", start.ContentSnapshots, snapshot)
	}
}

func TestSendMessageCheckpointRestoreFailureFailsExplicitly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_fail", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc restore-test")
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 123
WHERE id = ?`, filepath.Join(dir, "checkpoints", session.ID), session.ID); err != nil {
		t.Fatalf("seed checkpoint metadata: %v", err)
	}

	rt := &restoreFailingRuntime{err: errors.New("checkpoint_runsc_version mismatch")}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore failure"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "runtime_failed" || body["error"] != "checkpoint_runsc_version mismatch" {
		t.Fatalf("unexpected response body: %v", body)
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restore-failed session: %v", err)
	}
	if gotSession.ActiveGenerationID != old.GenerationID || gotSession.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("restore failure should keep active generation and retryable session: %+v old=%s", gotSession, old.GenerationID)
	}
	if gotSession.CheckpointPath != "" || gotSession.RestoreMS != nil {
		t.Fatalf("restore failure should clear checkpoint metadata: checkpoint=%q restore=%v", gotSession.CheckpointPath, gotSession.RestoreMS)
	}
	var oldStatus, oldNetwork, oldResources, oldErrorClass string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, COALESCE(g.error_class, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources, &oldErrorClass); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" || oldErrorClass != "runtime_failed" {
		t.Fatalf("generation not failed explicitly after restore failure: status=%s network=%s resources=%s class=%s", oldStatus, oldNetwork, oldResources, oldErrorClass)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if queuedTurns != 0 {
		t.Fatalf("restore failure should not enqueue a turn, got %d", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 0 || len(startRequests) != 1 {
		t.Fatalf("runtime calls prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	if !startRequests[0].RestoreFromCheckpoint || startRequests[0].GenerationID != old.GenerationID {
		t.Fatalf("start was not restore: %+v", startRequests[0])
	}
	var runtimeEvents, terminalEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(CASE WHEN type = 'generation.error' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN type = 'session.error' THEN 1 ELSE 0 END), 0)
FROM events
WHERE session_id = ?`, session.ID).Scan(&runtimeEvents, &terminalEvents); err != nil {
		t.Fatalf("count restore failure events: %v", err)
	}
	if runtimeEvents != 1 || terminalEvents != 0 {
		t.Fatalf("unexpected restore failure events: runtime=%d terminal=%d", runtimeEvents, terminalEvents)
	}
}

func TestSendMessageCheckpointRestoreFailureCanBeRetriedColdOnNextInput(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_fail_retry", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc restore-test")
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 456
WHERE id = ?`, filepath.Join(dir, "checkpoints", session.ID), session.ID); err != nil {
		t.Fatalf("seed checkpoint metadata: %v", err)
	}

	rt := &restoreFailingRuntime{err: errors.New("checkpoint_runsc_version mismatch")}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore failure"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restore-failed session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningIdle) || gotSession.ActiveGenerationID != old.GenerationID {
		t.Fatalf("session should stay retryable on failed checkpoint generation: %+v old=%s", gotSession, old.GenerationID)
	}
	var oldStatus, oldNetwork, oldResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("generation not reclaimable after restore failure: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
	rt.err = nil
	req = httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"retry after restore failure"}`))
	rec = httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected retry status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	gotSession, err = st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get retried session: %v", err)
	}
	if gotSession.ActiveGenerationID == "" || gotSession.ActiveGenerationID == old.GenerationID || gotSession.Status != string(sessionstate.RunningActive) {
		t.Fatalf("retry should allocate a replacement after explicit failure: %+v old=%s", gotSession, old.GenerationID)
	}
	var newStatus, newNetwork, newResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, gotSession.ActiveGenerationID).Scan(&newStatus, &newNetwork, &newResources); err != nil {
		t.Fatalf("query retry generation: %v", err)
	}
	if newStatus != "idle" || newNetwork != "live" || newResources != "live" {
		t.Fatalf("retry generation not live idle: status=%s network=%s resources=%s", newStatus, newNetwork, newResources)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'retry after restore failure'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("retry should enqueue exactly one turn, got %d", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 2 || len(startRequests) != 2 {
		t.Fatalf("runtime calls prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	if !startRequests[0].RestoreFromCheckpoint || startRequests[0].GenerationID != old.GenerationID {
		t.Fatalf("first start was not restore: %+v", startRequests[0])
	}
	if startRequests[1].RestoreFromCheckpoint || startRequests[1].GenerationID != gotSession.ActiveGenerationID {
		t.Fatalf("second start was not cold retry generation: %+v", startRequests[1])
	}
}

func TestSendMessageRestoreLiveCASFailureDestroysRunscContainerIDBeforeFailing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_live_cas", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc restore-test")
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())

	rt := &restoreStartHookRuntime{
		onRestoreStart: func() {
			if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?`, old.GenerationID); err != nil {
				t.Fatalf("force restore live CAS failure: %v", err)
			}
		},
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore live cas"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	destroyIDs := rt.runtimeDestroyRequests()
	oldRunscID := serverRunscContainerID(t, ctx, st, session.ID, old.GenerationID)
	if len(destroyIDs) != 1 || destroyIDs[0] != oldRunscID {
		t.Fatalf("restore live CAS cleanup should destroy runsc container id %q, got %+v", oldRunscID, destroyIDs)
	}
	if destroyIDs[0] == session.ID {
		t.Fatalf("restore live CAS cleanup used bare session id %q", session.ID)
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restore-failed session: %v", err)
	}
	if gotSession.ActiveGenerationID != old.GenerationID || gotSession.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("restore live CAS failure should not allocate replacement: %+v old=%s", gotSession, old.GenerationID)
	}
	var oldStatus, oldNetwork, oldResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("generation not reclaimable after restore live CAS failure: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
}

func TestSendMessageRestoreLiveCASFailureDoesNotRetireWhenDestroyFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_destroy_fail", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc restore-test")
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())

	rt := &restoreStartHookRuntime{
		recordingRuntime: recordingRuntime{destroyRuntimeErr: errors.New("destroy failed")},
		onRestoreStart: func() {
			if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?`, old.GenerationID); err != nil {
				t.Fatalf("force restore live CAS failure: %v", err)
			}
		},
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore destroy fail"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	destroyIDs := rt.runtimeDestroyRequests()
	oldRunscID := serverRunscContainerID(t, ctx, st, session.ID, old.GenerationID)
	if len(destroyIDs) != 1 || destroyIDs[0] != oldRunscID {
		t.Fatalf("restore cleanup should target runsc container id %q before failing, got %+v", oldRunscID, destroyIDs)
	}
	var oldStatus, oldNetwork, oldResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus == "failed" || oldNetwork == "reclaimable" || oldResources == "reclaimable" {
		t.Fatalf("restore generation should not be retired when runtime destroy fails: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
	var retirementEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type IN ('session.checkpoint_retired', 'generation.error')`, session.ID).Scan(&retirementEvents); err != nil {
		t.Fatalf("count restore failure events: %v", err)
	}
	if retirementEvents != 0 {
		t.Fatalf("restore failure events should not be committed when destroy fails, got %d", retirementEvents)
	}
}

func TestSendMessageCheckpointImageManifestInvalidFailsExplicitly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_manifest_invalid", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE sessions SET driver_id = 'sh', mode = 'shell' WHERE id = ?`, session.ID); err != nil {
		t.Fatalf("set shell session agent: %v", err)
	}
	session.DriverID = "sh"
	session.Mode = "shell"
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "sh"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}

	checkpointPath := filepath.Join(dir, "checkpoints", session.ID)
	writeServerCheckpointFilesWithoutManifest(t, checkpointPath)
	manifest, err := buildServerCheckpointImageManifest(checkpointPath)
	if err != nil {
		t.Fatalf("build checkpoint image manifest: %v", err)
	}
	if err := writeServerJSONFile(filepath.Join(checkpointPath, "harness-checkpoint-manifest.json"), manifest); err != nil {
		t.Fatalf("write checkpoint image manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkpointPath, "pages.img"), []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("corrupt checkpoint image file: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET checkpoint_path = ?
WHERE generation_id = ?`, checkpointPath, old.GenerationID); err != nil {
		t.Fatalf("record checkpoint path: %v", err)
	}
	runscPath, runscDigest := currentRunscBinaryMetadataForServerTest(t)
	recordServerRuntimeArtifactsWithRunsc(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc test", runscPath, runscDigest)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())

	realRuntime := runtime.New(runtime.Config{
		SessionsRoot:    cfg.SessionsRoot,
		AgentHomesRoot:  filepath.Join(dir, "agent-homes"),
		RootFSPath:      filepath.Join(dir, "rootfs"),
		BundleRoot:      filepath.Join(dir, "run", "runtime"),
		RunscNetwork:    "host",
		RunscOverlay2:   "none",
		RunDir:          cfg.Harness.RunDir,
		CommandRunner:   serverCommandRunner{outputs: map[string][]byte{"runsc --version": []byte("runsc test")}},
		BridgeMode:      "claim-loop",
		BridgeHeartbeat: time.Second,
		SandboxUID:      cfg.Harness.SandboxIdentity.UID,
		SandboxGID:      cfg.Harness.SandboxIdentity.GID,
	})
	rt := &restoreValidationRuntime{restore: realRuntime}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after corrupt checkpoint"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restore-failed session: %v", err)
	}
	if gotSession.ActiveGenerationID != old.GenerationID || gotSession.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("invalid checkpoint should not allocate replacement: %+v old=%s", gotSession, old.GenerationID)
	}
	var oldStatus, oldNetwork, oldResources, oldReason string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, COALESCE(g.failure_reason, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources, &oldReason); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("generation not failed after invalid checkpoint manifest: status=%s network=%s resources=%s reason=%s", oldStatus, oldNetwork, oldResources, oldReason)
	}
	if !strings.Contains(oldReason, "checkpoint image manifest") || !strings.Contains(oldReason, "pages.img") {
		t.Fatalf("old generation failure reason did not include checkpoint manifest mismatch: %q", oldReason)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if queuedTurns != 0 {
		t.Fatalf("invalid checkpoint should not enqueue a turn, got %d", queuedTurns)
	}
	if got := len(rt.startRequests); got != 1 {
		t.Fatalf("runtime calls start=%d want 1", got)
	}
	if !rt.startRequests[0].RestoreFromCheckpoint || rt.startRequests[0].GenerationID != old.GenerationID {
		t.Fatalf("start was not restore: %+v", rt.startRequests[0])
	}
}

func TestRunMaintenanceDoesNotColdStartFailedActiveGenerationWithQueuedTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_maintenance_no_restart", string(sessionstate.RunningActive), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	cfg.Harness.Bridge.HeartbeatInterval = config.Duration{Duration: time.Hour}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate old generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark old generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, old, owner.UUID, mustRuntimeResourceHostID(t), time.Now().UTC())
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    session.ID,
		GenerationID: old.GenerationID,
		Owner:        old.Owner,
		ErrorClass:   "orchestrator_restart_reconnect_grace_expired",
		Reason:       "orchestrator_restart_reconnect_grace_expired",
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("fail old generation: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, session.ID, "protected queued turn", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue queued turn: %v", err)
	}

	rt := &recordingRuntime{}
	runCtx, cancel := context.WithCancel(ctx)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()
	waitForGenerationResourceStates(t, runCtx, st, old.GenerationID, "destroyed", "destroyed")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningActive) ||
		sessionstate.CanAcceptInput(gotSession.Status) ||
		gotSession.ActiveGenerationID != old.GenerationID {
		t.Fatalf("maintenance should not replace failed active generation: %+v old=%s", gotSession, old.GenerationID)
	}
	var generationCount int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM runtime_generations
WHERE session_id = ?`, session.ID).Scan(&generationCount); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if generationCount != 1 {
		t.Fatalf("maintenance allocated replacement generations, count=%d", generationCount)
	}
	var generationStatus, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query failed generation: %v", err)
	}
	if generationStatus != "failed" ||
		networkState != "destroyed" ||
		resourceState != "destroyed" {
		t.Fatalf("unexpected failed generation after maintenance: status=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
	var queuedStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT status
FROM turns
WHERE id = ?`, turnID).Scan(&queuedStatus); err != nil {
		t.Fatalf("query queued turn: %v", err)
	}
	if queuedStatus != "queued" {
		t.Fatalf("queued turn status=%s want queued", queuedStatus)
	}
	if _, starts := rt.requests(); len(starts) != 0 {
		t.Fatalf("maintenance should not cold-start failed active generation with queued turn: %+v", starts)
	}
}

func TestRunMaintenanceRetiresExpiredCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_retire_checkpoint", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Reaper.CheckpointImageRetention = config.Duration{Duration: 0}
	cfg.Harness.Reaper.FailedRetention = config.Duration{Duration: time.Hour}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	checkpointedAt := time.Now().UTC().Add(-2 * time.Hour)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, allocation.GenerationID, checkpointedAt)
	checkpointPath := filepath.Join(dir, "checkpoint", session.ID)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 99,
    last_activity_at = ?
WHERE id = ?`, checkpointPath, checkpointedAt.Format(time.RFC3339Nano), session.ID); err != nil {
		t.Fatalf("seed checkpoint session metadata: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET checkpoint_path = ?
WHERE generation_id = ?`, checkpointPath, allocation.GenerationID); err != nil {
		t.Fatalf("seed checkpoint resource path: %v", err)
	}

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe(session.ID)
	defer cancelEvents()
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, hub),
		hub:     hub,
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()
	event := waitForHubEvent(t, eventsCh, "session.checkpoint_retired")
	waitForGenerationResourceStates(t, runCtx, st, allocation.GenerationID, "destroyed", "destroyed")
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}
	payload, ok := event.Payload.(json.RawMessage)
	if !ok || strings.Contains(string(payload), `"checkpoint_path"`) || !strings.Contains(string(payload), `"restore_ms":null`) {
		t.Fatalf("unexpected checkpoint retirement event payload: %#v", event.Payload)
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get retired session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningIdle) || gotSession.CheckpointPath != "" || gotSession.RestoreMS != nil {
		t.Fatalf("unexpected retired session: %+v", gotSession)
	}
	var generationStatus, generationError, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &generationError, &networkState, &resourceState); err != nil {
		t.Fatalf("query retired generation: %v", err)
	}
	if generationStatus != "failed" || generationError != "checkpoint_retired" || networkState != "destroyed" || resourceState != "destroyed" {
		t.Fatalf("unexpected retired generation: status=%s error=%s network=%s resource=%s", generationStatus, generationError, networkState, resourceState)
	}
	destroyRequests := rt.destroyGenerationRequests()
	if len(destroyRequests) != 1 || destroyRequests[0].GenerationID != allocation.GenerationID {
		t.Fatalf("unexpected destroy generation requests: %+v", destroyRequests)
	}
}

func TestEnsureActiveGenerationColdStartsAfterCheckpointRetirement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_retire_then_send", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	checkpointedAt := time.Now().UTC().Add(-2 * time.Hour)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, allocation.GenerationID, checkpointedAt)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    last_activity_at = ?
WHERE id = ?`, filepath.Join(dir, "checkpoint", session.ID), checkpointedAt.Format(time.RFC3339Nano), session.ID); err != nil {
		t.Fatalf("seed checkpoint session metadata: %v", err)
	}
	staleCheckpointedSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get checkpointed session: %v", err)
	}
	if _, err := st.RetireExpiredCheckpoints(ctx, store.RetireExpiredCheckpointsParams{
		OwnerUUID:                owner.UUID,
		Now:                      time.Now().UTC(),
		CheckpointImageRetention: time.Hour,
	}); err != nil {
		t.Fatalf("retire checkpoint: %v", err)
	}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: &recordingRuntime{},
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	ensured, err := srv.ensureActiveGeneration(ctx, staleCheckpointedSession, store.GenerationLeaseOwner(owner.UUID))
	if err != nil {
		t.Fatalf("ensure active generation after retirement: %v", err)
	}
	if !ensured.IsNew || ensured.RestoreFromCheckpoint || ensured.Allocation.GenerationID == allocation.GenerationID {
		t.Fatalf("ensure should cold-start replacement after checkpoint retirement: %+v old=%s", ensured, allocation.GenerationID)
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get replacement session: %v", err)
	}
	if gotSession.ActiveGenerationID != ensured.Allocation.GenerationID {
		t.Fatalf("session active generation=%s want replacement %s", gotSession.ActiveGenerationID, ensured.Allocation.GenerationID)
	}
}

func TestGetQuotaReportsSessionAndPoolCeilings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	createServerTestSession(t, ctx, st, dir, "sess_quota", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.MaxSessions = 3
	cfg.Harness.MaxSessions = 3
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.242.0.0/29")}
	modelAccessAllowed := true
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_quota",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config: store.ResourceAllocatorConfig{
			RunDir:                      cfg.Harness.RunDir,
			CIDRPool:                    cfg.Harness.Network.CIDRPool.Prefix,
			EgressDorisFEHosts:          cfg.Harness.Network.Egress.DorisFEHosts,
			EgressDorisBEHosts:          cfg.Harness.Network.Egress.DorisBEHosts,
			EgressDorisPorts:            cfg.Harness.Network.Egress.DorisPorts,
			EgressDNSPolicy:             string(cfg.Harness.Network.Egress.DNSPolicy),
			HostProxyBindURL:            cfg.ModelProxy.BindURL,
			ProxyPort:                   cfg.ModelProxy.BindPort,
			DriverID:                    "claude_code",
			Model:                       "sonnet",
			OutputFormat:                "stream-json",
			SandboxUID:                  cfg.Harness.SandboxIdentity.UID,
			SandboxGID:                  cfg.Harness.SandboxIdentity.GID,
			ModelAccessAllowed:          &modelAccessAllowed,
			ProviderCredentialsHostOnly: true,
			SandboxModelProxyBaseURL:    cfg.ModelProxy.SandboxBaseURL,
		},
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	req := httptest.NewRequest(http.MethodGet, "/api/quota", nil)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode quota: %v", err)
	}
	if body["soft_session_ceiling"] != 3 ||
		body["active_sessions"] != 1 ||
		body["live_pool_ceiling"] != 2 ||
		body["allocated_pool_slots"] != 1 ||
		body["remaining_pool_slots"] != 1 {
		t.Fatalf("unexpected quota body for allocation %s: %+v", allocation.GenerationID, body)
	}
	if _, ok := body["effective_ceiling"]; ok {
		t.Fatalf("quota should report session and pool ceilings separately without effective_ceiling: %+v", body)
	}
}

func TestSendMessagePoolExhaustionDoesNotQueueTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.242.0.0/30")}
	createServerTestSession(t, ctx, st, dir, "sess_pool_used", string(sessionstate.Created), time.Now().UTC(), nil)
	target := createServerTestSession(t, ctx, st, dir, "sess_pool_target", string(sessionstate.Created), time.Now().UTC(), nil)
	if _, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_pool_used",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	}); err != nil {
		t.Fatalf("allocate pool slot: %v", err)
	}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+target.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, target.ID)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "pool_exhausted" {
		t.Fatalf("expected pool_exhausted, got %v", body)
	}
	var targetGenerations, targetTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, target.ID).Scan(&targetGenerations); err != nil {
		t.Fatalf("count target generations: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, target.ID).Scan(&targetTurns); err != nil {
		t.Fatalf("count target turns: %v", err)
	}
	if targetGenerations != 0 || targetTurns != 0 {
		t.Fatalf("pool exhaustion leaked target state: generations=%d turns=%d", targetGenerations, targetTurns)
	}
}

func TestSendMessageRuntimeStartFailureMarksGenerationFailedAndReclaimable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_runtime_fail", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:   cfg,
		store: st,
		runtime: failingRuntime{
			err: errors.New("pre-start sandbox network probe failed"),
		},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "probe_failed_pre_start" ||
		body["error"] != "sandbox network probe failed before start" {
		t.Fatalf("unexpected response body: %v", body)
	}
	var generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state, s.status,
       COALESCE(s.error_class, ''), COALESCE(s.failure_reason, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.session_id = ?`, session.ID).Scan(
		&generationStatus,
		&errorClass,
		&networkState,
		&resourceState,
		&sessionStatus,
		&sessionErrorClass,
		&sessionFailureReason,
	); err != nil {
		t.Fatalf("query generation state: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" ||
		sessionStatus != string(sessionstate.Created) ||
		sessionErrorClass != "" ||
		sessionFailureReason != "" {
		t.Fatalf("unexpected failed generation state: generation=%s class=%s network=%s resource=%s session=%s session_class=%s session_reason=%s", generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason)
	}
	if !sessionstate.CanAcceptInput(sessionStatus) {
		t.Fatalf("session should remain input-acceptable after start failure, got %s", sessionStatus)
	}
	var runtimeResourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT state
FROM runtime_resource_instances
WHERE generation_id = (
  SELECT generation_id FROM runtime_generations WHERE session_id = ?
)`, session.ID).Scan(&runtimeResourceState); err != nil {
		t.Fatalf("query runtime resource instance after start failure: %v", err)
	}
	if runtimeResourceState != string(store.RuntimeResourceRetiring) {
		t.Fatalf("runtime resource state after start failure=%s want %s", runtimeResourceState, store.RuntimeResourceRetiring)
	}
	var runtimeEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'generation.error'`, session.ID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count generation error events: %v", err)
	}
	if runtimeEvents != 1 {
		t.Fatalf("expected one generation.error event, got %d", runtimeEvents)
	}
	var turns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 0 {
		t.Fatalf("runtime start failure should happen before turn creation, got %d turns", turns)
	}
	var failedGenerationID string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT generation_id FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&failedGenerationID); err != nil {
		t.Fatalf("query failed generation id: %v", err)
	}
	if err := srv.cleanupGenerationResources(ctx, session.ID, failedGenerationID, time.Now().UTC()); err != nil {
		t.Fatalf("cleanup failed generation resources: %v", err)
	}
	instance, err := st.GetRuntimeResourceInstance(ctx, failedGenerationID)
	if err != nil {
		t.Fatalf("get cleaned runtime resource instance: %v", err)
	}
	if instance.State != store.RuntimeResourceDestroyed ||
		instance.EvidenceDigest == "" ||
		len(instance.EvidenceJSON) == 0 ||
		instance.VerifiedAt == nil {
		t.Fatalf("runtime resource cleanup did not record destroyed evidence: %+v", instance)
	}
	var evidence store.ResourceReconciliationEvidence
	if err := json.Unmarshal(instance.EvidenceJSON, &evidence); err != nil {
		t.Fatalf("decode runtime resource cleanup evidence: %v", err)
	}
	if !strings.HasPrefix(evidence.RunscState, "runsc_container:absent") {
		t.Fatalf("runtime resource cleanup did not record runsc absence: %+v", evidence)
	}

	srv.runtime = instantRuntime{}
	retryReq := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"retry"}`))
	retryRec := httptest.NewRecorder()
	srv.sendMessage(retryRec, retryReq, session.ID)
	if retryRec.Code != http.StatusAccepted {
		t.Fatalf("expected retry status 202, got %d body %s", retryRec.Code, retryRec.Body.String())
	}
	var generationCount int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&generationCount); err != nil {
		t.Fatalf("count generations after retry: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns after retry: %v", err)
	}
	if generationCount != 2 || turns != 1 {
		t.Fatalf("retry should allocate generation N+1 and enqueue one turn, generations=%d turns=%d", generationCount, turns)
	}
}

func TestStartEnsuredGenerationRenewsLeaseDuringSlowPrepare(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_slow_start", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Bridge.LeaseTTL = config.Duration{Duration: 200 * time.Millisecond}
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  cfg.Harness.Bridge.LeaseTTL.Duration,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := newBlockingPrepareRuntime()
	t.Cleanup(rt.release)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.startEnsuredGeneration(ctx, session, ensuredGeneration{
			Allocation: allocation,
			IsNew:      true,
		}, startFailureInputAcceptable)
	}()

	select {
	case <-rt.prepareStarted:
	case <-time.After(time.Second):
		t.Fatalf("prepare did not start")
	}
	waitForGenerationLeaseAfter(t, ctx, st, allocation.GenerationID, allocation.LeaseExpiresAt)
	rt.release()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("start ensured generation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("start ensured generation did not finish")
	}
	waitForGenerationStatus(t, ctx, st, allocation.GenerationID, "idle")
	volumeConfig, err := srv.dataVolumeProvisionerConfig()
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	workspaceVolume, err := st.VerifySessionWorkspaceVolume(ctx, store.VerifySessionWorkspaceVolumeParams{
		SessionID: session.ID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify workspace volume: %v", err)
	}
	driverHomeVolume, err := st.VerifySessionDriverHomeVolume(ctx, store.VerifySessionDriverHomeVolumeParams{
		SessionID: session.ID,
		Driver:    session.DriverID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify driver home volume: %v", err)
	}
	prepares, starts := rt.requests()
	if len(prepares) != 2 || len(starts) != 1 {
		t.Fatalf("runtime requests prepare=%d start=%d", len(prepares), len(starts))
	}
	for _, prepare := range prepares {
		if prepare.WorkspaceHostPath != workspaceVolume.HostPath ||
			prepare.AgentHomeHostPath != driverHomeVolume.HostPath {
			t.Fatalf("runtime render did not receive data volume paths: prepare=%+v workspace=%+v home=%+v",
				prepare, workspaceVolume, driverHomeVolume)
		}
	}
	if starts[0].WorkspaceHostPath != workspaceVolume.HostPath ||
		starts[0].AgentHomeHostPath != driverHomeVolume.HostPath {
		t.Fatalf("runtime start did not receive data volume paths: start=%+v workspace=%+v home=%+v",
			starts[0], workspaceVolume, driverHomeVolume)
	}
	instance, err := st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime resource instance: %v", err)
	}
	if instance.State != store.RuntimeResourceLive ||
		instance.WorkerID != owner.UUID ||
		instance.RunscContainerID != serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID) ||
		instance.RunscBinaryPath != "/usr/local/bin/runsc-test" ||
		instance.RunscBinaryDigest != "sha256:runsc-test" ||
		instance.NftTableName != mustRuntimeResourceNftTableName(t, allocation.GenerationID) {
		t.Fatalf("unexpected runtime resource instance: %+v", instance)
	}
	contract, err := st.GetSandboxContractForGeneration(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get sandbox contract: %v", err)
	}
	if contract.SandboxContractVersion != store.SandboxContractVersion ||
		contract.ContractID != sandboxContractID(allocation.GenerationID) ||
		contract.ContractGateVersion != store.SandboxContractGateDriverManifest {
		t.Fatalf("unexpected sandbox contract: %+v", contract)
	}
	var payload map[string]any
	if err := json.Unmarshal(contract.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode sandbox contract payload: %v", err)
	}
	if payload["contract_gate_version"] != store.SandboxContractGateDriverManifest {
		t.Fatalf("sandbox contract gate version should be driver_manifest_v1: %s", contract.CanonicalPayload)
	}
	inputDigests, ok := payload["input_digests"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing input digests: %s", contract.CanonicalPayload)
	}
	for _, key := range []string{"runtime_config_digest", "agent_manifest_digest"} {
		value, _ := inputDigests[key].(string)
		if !strings.HasPrefix(value, "sha256:") {
			t.Fatalf("sandbox contract missing %s: %s", key, contract.CanonicalPayload)
		}
	}
	evidence, err := st.GetSandboxContractInputEvidence(ctx, contract.ContractID)
	if err != nil {
		t.Fatalf("get sandbox contract input evidence: %v", err)
	}
	if evidence.RuntimeConfigDigest != inputDigests["runtime_config_digest"] ||
		evidence.AgentManifestDigest != inputDigests["agent_manifest_digest"] ||
		!json.Valid(evidence.RuntimeConfigPreimage) ||
		!json.Valid(evidence.AgentManifestPayload) {
		t.Fatalf("sandbox contract input evidence mismatch: evidence=%+v input=%+v", evidence, inputDigests)
	}
	if inputDigests["rootfs_image_digest"] != nil {
		t.Fatalf("rootfs digest should remain null until rootfs evidence is available: %s", contract.CanonicalPayload)
	}
	adapter, ok := payload["runtime_adapter"].(map[string]any)
	if !ok || adapter["runsc_container_id"] != serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID) {
		t.Fatalf("sandbox contract missing runsc identity: %s", contract.CanonicalPayload)
	}
	if adapter["runsc_binary_path"] != "/usr/local/bin/runsc-test" ||
		adapter["runsc_binary_digest"] != "sha256:runsc-test" {
		t.Fatalf("sandbox contract missing runsc binary metadata: %s", contract.CanonicalPayload)
	}
	ambientCaps, ok := adapter["ambient_capabilities"].([]any)
	if adapter["no_new_privileges"] != true || !ok || len(ambientCaps) != 0 {
		t.Fatalf("sandbox contract missing runtime capability policy: %s", contract.CanonicalPayload)
	}
	forbiddenCaps, ok := adapter["forbidden_capabilities"].([]any)
	if !ok || !jsonArrayContainsAll(forbiddenCaps, "CAP_NET_ADMIN", "CAP_NET_RAW", "CAP_SYS_ADMIN") {
		t.Fatalf("sandbox contract missing forbidden capability policy: %s", contract.CanonicalPayload)
	}
	requiredAnnotations, ok := adapter["required_annotations"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing required annotations: %s", contract.CanonicalPayload)
	}
	bridgeAnnotations, ok := requiredAnnotations[bridge.BridgeMountDestination].(map[string]any)
	if !ok ||
		bridgeAnnotations["dev.gvisor.spec.mount./harness-control/bridge.type"] != "bind" ||
		bridgeAnnotations["dev.gvisor.spec.mount./harness-control/bridge.share"] != "exclusive" {
		t.Fatalf("sandbox contract missing bridge required annotation policy: %s", contract.CanonicalPayload)
	}
	networkIdentity, ok := payload["network_identity"].(map[string]any)
	if !ok ||
		networkIdentity["sandbox_ip"] != instance.SandboxIP ||
		networkIdentity["nft_table_name"] != instance.NftTableName {
		t.Fatalf("sandbox contract missing runtime network identity: %s instance=%+v", contract.CanonicalPayload, instance)
	}
	resourceIdentity, ok := payload["resource_identity"].(map[string]any)
	if !ok || resourceIdentity["resource_identity_digest"] != instance.ResourceIdentityDigest {
		t.Fatalf("sandbox contract missing resource identity digest: %s instance=%+v", contract.CanonicalPayload, instance)
	}
	mountPlan, ok := payload["mount_plan"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing mount plan: %s", contract.CanonicalPayload)
	}
	workspaceMount, ok := mountPlan["workspace"].(map[string]any)
	if !ok || workspaceMount["source"] != workspaceVolume.HostPath {
		t.Fatalf("sandbox contract workspace mount does not use data volume: %s", contract.CanonicalPayload)
	}
	agentHomeMount, ok := mountPlan["agent_home"].(map[string]any)
	if !ok || agentHomeMount["source"] != driverHomeVolume.HostPath {
		t.Fatalf("sandbox contract agent home mount does not use data volume: %s", contract.CanonicalPayload)
	}
	dataVolumes, ok := payload["data_volumes"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing data volume ownership: %s", contract.CanonicalPayload)
	}
	workspacePayload, ok := dataVolumes["workspace"].(map[string]any)
	if !ok || workspacePayload["provisioning_marker_digest"] != workspaceVolume.ProvisioningMarkerDigest {
		t.Fatalf("sandbox contract workspace data volume evidence mismatch: %s", contract.CanonicalPayload)
	}
	driverHomePayload, ok := dataVolumes["agent_home"].(map[string]any)
	if !ok || driverHomePayload["provisioning_marker_digest"] != driverHomeVolume.ProvisioningMarkerDigest {
		t.Fatalf("sandbox contract driver home data volume evidence mismatch: %s", contract.CanonicalPayload)
	}
	var manifestDigest, specDigest, bundleDigest string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT control_manifest_digest, oci_spec_digest, bundle_digest
FROM sandbox_contract_artifacts
WHERE contract_id = ?`, contract.ContractID).Scan(&manifestDigest, &specDigest, &bundleDigest); err != nil {
		t.Fatalf("query sandbox contract artifacts: %v", err)
	}
	if manifestDigest != "manifest_digest" || specDigest != "spec_digest" || bundleDigest != "bundle_digest" {
		t.Fatalf("unexpected sandbox contract artifact digests: manifest=%s spec=%s bundle=%s", manifestDigest, specDigest, bundleDigest)
	}
}

func TestSandboxContractPayloadRecordsPiMaterializedConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	session := store.Session{
		ID:        "sess_pi_contract",
		UserID:    labUserID,
		Status:    string(sessionstate.Created),
		DriverID:  "pi",
		Mode:      "agent",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create pi session: %v", err)
	}
	modelAccessAllowed := true
	allocatorConfig := store.ResourceAllocatorConfig{
		RunDir:                      filepath.Join(dir, "run"),
		CIDRPool:                    netip.MustParsePrefix("10.240.0.0/28"),
		EgressDNSPolicy:             "hostnames_only",
		HostProxyBindURL:            "http://0.0.0.0:8082",
		ProxyPort:                   8082,
		DriverID:                    "pi",
		Model:                       "sonnet",
		OutputFormat:                "pi_rpc_events_v1.0",
		DisableNonessentialTraffic:  true,
		SandboxUID:                  65534,
		SandboxGID:                  65534,
		ModelAccessAllowed:          &modelAccessAllowed,
		ProviderCredentialsHostOnly: true,
		SandboxModelProxyBaseURL:    "http://harness-model-proxy.internal:8082",
	}
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    allocatorConfig,
	})
	if err != nil {
		t.Fatalf("allocate pi generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	rt := runtime.New(runtime.Config{
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
		CommandRunner: serverCommandRunner{outputs: map[string][]byte{
			"runsc --version": []byte("runsc test"),
			"ip netns exec " + details.NetnsName + " curl -sS --max-time 2 -o /dev/null -w %{http_code} " + strings.TrimRight(details.ProbeURL, "/") + "/healthz": []byte("200"),
		}},
	})
	workspaceHostPath := filepath.Join(dir, "volumes", "workspaces", session.ID)
	agentHomeHostPath := filepath.Join(dir, "volumes", "driver-homes", session.ID, "pi")
	artifacts, err := rt.PrepareGeneration(ctx, runtime.StartRequest{
		SessionID:         session.ID,
		GenerationID:      allocation.GenerationID,
		DriverID:          "pi",
		Generation:        details,
		WorkspaceHostPath: workspaceHostPath,
		AgentHomeHostPath: agentHomeHostPath,
	})
	if err != nil {
		t.Fatalf("prepare pi runtime artifacts: %v", err)
	}
	cfg := testServerConfig(dir)
	cfg.DefaultAgent = "pi"
	srv := &Server{cfg: cfg, store: st, runtime: rt}
	payload, err := srv.sandboxContractPayload(session, details, artifacts, "sha256:resource-identity", sessionRuntimeDataVolumes{
		Workspace: store.SessionWorkspaceVolume{
			SessionID:                session.ID,
			HostPath:                 workspaceHostPath,
			LayoutVersion:            store.DataVolumeLayoutVersion,
			RuntimeIdentityDigest:    "sha256:workspace-identity",
			ProvisioningMarkerPath:   filepath.Join(dir, "evidence", "workspaces", session.ID+".json"),
			ProvisioningMarkerDigest: "sha256:workspace-marker",
		},
		DriverHome: store.SessionDriverHomeVolume{
			SessionID:                session.ID,
			Driver:                   "pi",
			HostPath:                 agentHomeHostPath,
			LayoutVersion:            store.DataVolumeLayoutVersion,
			RuntimeIdentityDigest:    "sha256:driver-home-identity",
			ProvisioningMarkerPath:   filepath.Join(dir, "evidence", "driver-homes", session.ID, "pi.json"),
			ProvisioningMarkerDigest: "sha256:driver-home-marker",
		},
	}, nil)
	if err != nil {
		t.Fatalf("build pi sandbox contract payload: %v", err)
	}
	mountPlan := payload["mount_plan"].(map[string]any)
	materializedMounts := mountPlan["driver_config_materializations"].(map[string]any)
	driverRuntime := payload["driver_runtime"].(map[string]any)
	materializedRuntime := driverRuntime["materialized_driver_config"].(map[string]any)
	for _, name := range []string{"models", "settings"} {
		runtimeEntry := materializedRuntime[name].(map[string]any)
		mountEntry := materializedMounts[name].(map[string]any)
		if runtimeEntry["destination_mutable_by_sandbox"] != false ||
			mountEntry["destination_mutable_by_sandbox"] != false ||
			mountEntry["mode"] != "ro" ||
			mountEntry["type"] != "bind" ||
			mountEntry["exact"] != true {
			t.Fatalf("pi materialization %s is not read-only/exact: runtime=%+v mount=%+v", name, runtimeEntry, mountEntry)
		}
		if digest, _ := runtimeEntry["source_digest"].(string); !strings.HasPrefix(digest, "sha256:") {
			t.Fatalf("pi materialization %s missing digest: %+v", name, runtimeEntry)
		}
	}
	if materializedRuntime["models"].(map[string]any)["source_projection_path"] != agents.PiModelsConfigPath ||
		materializedRuntime["settings"].(map[string]any)["source_projection_path"] != agents.PiSettingsConfigPath {
		t.Fatalf("pi materialization source projections wrong: %+v", materializedRuntime)
	}
	if _, err := st.StoreSandboxContract(ctx, store.StoreSandboxContractParams{
		ContractID:             sandboxContractID(allocation.GenerationID),
		SessionID:              session.ID,
		GenerationID:           allocation.GenerationID,
		Owner:                  store.GenerationLeaseOwner(owner.UUID),
		SandboxContractVersion: store.SandboxContractVersion,
		ContractSchemaVersion:  store.SandboxContractSchemaVersion,
		ContractGateVersion:    store.SandboxContractGateDriverManifest,
		DriverState:            allocation.DriverState,
		Payload:                payload,
		Now:                    now,
	}); err != nil {
		t.Fatalf("store pi sandbox contract: %v", err)
	}
}

func TestServerRenderersThreadContentSnapshots(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_content_thread", string(sessionstate.Created), now, nil)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	volumes := sessionRuntimeDataVolumes{
		Workspace: store.SessionWorkspaceVolume{
			SessionID:                session.ID,
			HostPath:                 filepath.Join(dir, "volumes", "workspaces", session.ID),
			LayoutVersion:            store.DataVolumeLayoutVersion,
			RuntimeIdentityDigest:    "sha256:workspace-identity",
			ProvisioningMarkerPath:   filepath.Join(dir, "evidence", "workspaces", session.ID+".json"),
			ProvisioningMarkerDigest: "sha256:workspace-marker",
		},
		DriverHome: store.SessionDriverHomeVolume{
			SessionID:                session.ID,
			Driver:                   session.DriverID,
			HostPath:                 filepath.Join(dir, "volumes", "driver-homes", session.ID, session.DriverID),
			LayoutVersion:            store.DataVolumeLayoutVersion,
			RuntimeIdentityDigest:    "sha256:driver-home-identity",
			ProvisioningMarkerPath:   filepath.Join(dir, "evidence", "driver-homes", session.ID, session.DriverID+".json"),
			ProvisioningMarkerDigest: "sha256:driver-home-marker",
		},
	}
	snapshots := []store.ContentSnapshotRecord{{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    filepath.Join(dir, "content", "skills", "sha256-skills"),
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
	}}
	artifacts := testGenerationArtifacts()
	srv := &Server{cfg: cfg, store: st}

	req := srv.runtimeStartRequest(session, allocation.GenerationID, details, artifacts, volumes, snapshots)
	if len(req.ContentSnapshots) != 1 || req.ContentSnapshots[0].Digest != "sha256:skills" {
		t.Fatalf("runtime start request content snapshots = %+v", req.ContentSnapshots)
	}
	driftedSession := session
	driftedSession.DriverID = string(agents.Shell)
	req = srv.runtimeStartRequest(driftedSession, allocation.GenerationID, details, artifacts, volumes, snapshots)
	if req.DriverID != details.DriverID {
		t.Fatalf("runtime start request driver id = %q want generation driver %q", req.DriverID, details.DriverID)
	}

	contractPayload, err := srv.sandboxContractPayload(session, details, artifacts, "sha256:resource-identity", volumes, snapshots)
	if err != nil {
		t.Fatalf("render sandbox contract with content snapshot: %v", err)
	}
	contractMounts := contractPayload["mount_plan"].(map[string]any)["content_snapshots"].(map[string]any)
	contractSkills := contractMounts[store.ContentSnapshotKindSkills].(map[string]any)
	if contractSkills["source"] != snapshots[0].ImmutableHostPath ||
		contractSkills["destination"] != store.ContentSnapshotSkillsMount ||
		contractSkills["digest"] != "sha256:skills" ||
		contractSkills["mode"] != "ro" ||
		contractSkills["exact"] != true {
		t.Fatalf("sandbox contract skills snapshot mount = %+v", contractSkills)
	}

	inputEvidence, err := srv.sandboxContractInputEvidenceFor(session, details.DriverID)
	if err != nil {
		t.Fatalf("input evidence: %v", err)
	}
	planPayload, err := srv.shadowGenerationPlanPayload(session, details, artifacts, contractPayload, "sha256:resource-identity", volumes, snapshots, inputEvidence)
	if err != nil {
		t.Fatalf("render generation plan with content snapshot: %v", err)
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: planPayload}); err != nil {
		t.Fatalf("validate content snapshot plan: %v", err)
	}
	planSnapshots := planPayload["content_snapshots"].(map[string]any)
	planSkills := planSnapshots[store.ContentSnapshotKindSkills].(map[string]any)
	if planSkills["digest"] != "sha256:skills" ||
		planSkills["immutable_host_path"] != snapshots[0].ImmutableHostPath ||
		planSkills["mount_destination"] != store.ContentSnapshotSkillsMount {
		t.Fatalf("generation plan skills snapshot = %+v", planSkills)
	}
	planMounts := planPayload["mounts"].(map[string]any)["content_snapshots"].(map[string]any)
	planMountSkills := planMounts[store.ContentSnapshotKindSkills].(map[string]any)
	if planMountSkills["digest"] != "sha256:skills" ||
		planMountSkills["destination"] != store.ContentSnapshotSkillsMount ||
		planMountSkills["exact"] != true {
		t.Fatalf("generation plan skills snapshot mount = %+v", planMountSkills)
	}
	workspace := planPayload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	if workspace["platform_content_mount_scope"] != "immutable_content_snapshots" {
		t.Fatalf("workspace platform content scope = %+v", workspace)
	}
}

func TestSelectGenerationContentSnapshotsRequiresSingleImmutableSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	policy := agents.FeaturePolicy{
		agents.FeatureSkillsSnapshot:  agents.FeaturePolicyRequired,
		agents.FeatureManagedSettings: agents.FeaturePolicyDisabled,
	}

	if _, err := srv.selectGenerationContentSnapshots(ctx, policy); err == nil ||
		!strings.Contains(err.Error(), "required feature skills_snapshot has no skills snapshot") {
		t.Fatalf("expected missing skills snapshot selection error, got %v", err)
	}

	skills, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
	})
	if err != nil {
		t.Fatalf("store skills snapshot: %v", err)
	}
	selected, err := srv.selectGenerationContentSnapshots(ctx, policy)
	if err != nil {
		t.Fatalf("select single skills snapshot: %v", err)
	}
	if len(selected) != 1 || selected[0].Digest != skills.Digest {
		t.Fatalf("selected snapshots = %+v want %+v", selected, skills)
	}

	if _, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               "sha256:skills-other",
		ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills-other",
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source-other",
		RetentionClass:       "generation_plan",
	}); err != nil {
		t.Fatalf("store second skills snapshot: %v", err)
	}
	if _, err := srv.selectGenerationContentSnapshots(ctx, policy); err == nil ||
		!strings.Contains(err.Error(), "required feature skills_snapshot is ambiguous") {
		t.Fatalf("expected ambiguous skills snapshot selection error, got %v", err)
	}

	policy[agents.FeatureSkillsSnapshot] = agents.FeaturePolicyDisabled
	policy[agents.FeatureManagedSettings] = agents.FeaturePolicyRequired
	managed, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindManagedSettings,
		Digest:               "sha256:settings",
		ImmutableHostPath:    "/var/lib/harness/content/managed-settings/sha256-settings",
		MountDestination:     store.ContentSnapshotManagedSettingsMount,
		SourceEvidenceDigest: "sha256:settings-source",
		RetentionClass:       "generation_plan",
	})
	if err != nil {
		t.Fatalf("store managed settings snapshot: %v", err)
	}
	selected, err = srv.selectGenerationContentSnapshots(ctx, policy)
	if err != nil {
		t.Fatalf("select managed settings snapshot: %v", err)
	}
	if len(selected) != 1 || selected[0].Digest != managed.Digest {
		t.Fatalf("selected managed settings snapshots = %+v want %+v", selected, managed)
	}

	policy[agents.FeatureManagedSettings] = agents.FeaturePolicyUnsupported
	selected, err = srv.selectGenerationContentSnapshots(ctx, policy)
	if err != nil {
		t.Fatalf("unsupported snapshot feature should not select: %v", err)
	}
	if len(selected) != 0 {
		t.Fatalf("unsupported snapshot feature selected snapshots: %+v", selected)
	}
}

func TestStartEnsuredGenerationLeavesBridgeClaimsUntilLivePoll(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_start_claim_deferred", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &claimAfterProbeRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	if err := srv.startEnsuredGeneration(ctx, session, ensuredGeneration{
		Allocation: allocation,
		IsNew:      true,
	}, startFailureInputAcceptable); err != nil {
		t.Fatalf("start ensured generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("generation details: %v", err)
	}
	outbox, err := bridge.OpenQueue(details.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		t.Fatalf("open bridge outbox: %v", err)
	}
	files, err := outbox.ReadAll()
	if err != nil {
		t.Fatalf("read bridge outbox: %v", err)
	}
	if len(files) != 1 || files[0].Envelope.Type != bridge.TypeClaimNextTurn {
		t.Fatalf("startup probe should leave only claim for live poller, got %+v", files)
	}
}

func TestVerifyStoredGenerationPlanProjectionsChecksExistingRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_projection_verify", string(sessionstate.Created), time.Now().UTC(), nil)
	generationID := "gen_projection_verify"
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'allocating', 'owner', ?)`, generationID, session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: generationID,
		Payload:      map[string]any{"generation_id": generationID, "plan_version": 1},
	})
	if err != nil {
		t.Fatalf("store generation plan: %v", err)
	}
	artifacts := testGenerationArtifacts()
	details := store.RuntimeGenerationDetails{
		GenerationID:        generationID,
		ControlManifestPath: artifacts.ManifestPath,
		SpecPath:            artifacts.SpecPath,
		BundleDirPath:       artifacts.BundleDir,
	}
	for _, expectation := range planprojection.ExpectationsForDetails(details, artifacts) {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    expectation.ProjectionKind,
			ProjectionVersion: 1,
			PayloadDigest:     expectation.PayloadDigest,
			MaterializedPath:  expectation.MaterializedPath,
		}); err != nil {
			t.Fatalf("store projection %s: %v", expectation.ProjectionKind, err)
		}
	}
	if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
		GenerationID:      generationID,
		PlanDigest:        plan.PlanDigest,
		ProjectionKind:    store.GenerationPlanProjectionSandboxContract,
		ProjectionVersion: store.GenerationPlanProjectionVersion,
		PayloadDigest:     "sha256:sandbox-contract",
	}); err != nil {
		t.Fatalf("store sandbox contract projection: %v", err)
	}

	srv := &Server{store: st}
	verified, err := srv.verifyStoredGenerationPlanProjections(ctx, details, artifacts, "sha256:sandbox-contract")
	if err != nil {
		t.Fatalf("verify matching projections: %v", err)
	}
	if !verified {
		t.Fatalf("expected existing plan projections to verify")
	}
	mismatch := artifacts
	mismatch.SpecDigest = "changed_spec_digest"
	if _, err := srv.verifyStoredGenerationPlanProjections(ctx, details, mismatch, "sha256:sandbox-contract"); err == nil ||
		!strings.Contains(err.Error(), "oci_spec digest mismatch") {
		t.Fatalf("expected projection mismatch, got %v", err)
	}
	if _, err := srv.verifyStoredGenerationPlanProjections(ctx, details, artifacts, "sha256:changed-contract"); err == nil ||
		!strings.Contains(err.Error(), "sandbox_contract digest mismatch") {
		t.Fatalf("expected sandbox contract projection mismatch, got %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET materialized_path = ?
WHERE generation_id = ?
  AND projection_kind = ?`, "/tmp/changed-config.json", generationID, store.GenerationPlanProjectionOCISpec); err != nil {
		t.Fatalf("corrupt projection path: %v", err)
	}
	if _, err := srv.verifyStoredGenerationPlanProjections(ctx, details, artifacts, "sha256:sandbox-contract"); err == nil ||
		!strings.Contains(err.Error(), "oci_spec materialized path mismatch") {
		t.Fatalf("expected projection materialized path mismatch, got %v", err)
	}
}

func TestVerifyStoredGenerationPlanProjectionsChecksProjectionVersion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_projection_version", string(sessionstate.Created), time.Now().UTC(), nil)
	generationID := "gen_projection_version"
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'allocating', 'owner', ?)`, generationID, session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: generationID,
		Payload:      map[string]any{"generation_id": generationID, "plan_version": 1},
	})
	if err != nil {
		t.Fatalf("store generation plan: %v", err)
	}
	artifacts := testGenerationArtifacts()
	for _, expectation := range planprojection.Expectations(artifacts) {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    expectation.ProjectionKind,
			ProjectionVersion: expectation.ProjectionVersion,
			PayloadDigest:     expectation.PayloadDigest,
		}); err != nil {
			t.Fatalf("store projection %s: %v", expectation.ProjectionKind, err)
		}
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET projection_version = 2
WHERE generation_id = ?
  AND projection_kind = ?`, generationID, store.GenerationPlanProjectionOCISpec); err != nil {
		t.Fatalf("corrupt stored projection version: %v", err)
	}

	srv := &Server{store: st}
	if _, err := srv.verifyStoredGenerationPlanProjections(ctx, store.RuntimeGenerationDetails{GenerationID: generationID}, artifacts, ""); err == nil ||
		!strings.Contains(err.Error(), "generation plan projection oci_spec version = 2, want 1") {
		t.Fatalf("expected projection version mismatch, got %v", err)
	}
}

func TestGenerationPlanProjectionExpectationsIncludesSandboxContractWhenProvided(t *testing.T) {
	withoutContract := generationPlanProjectionExpectations(testGenerationArtifacts(), "")
	for _, expectation := range withoutContract {
		if expectation.ProjectionKind == store.GenerationPlanProjectionSandboxContract {
			t.Fatalf("empty contract digest should not add sandbox contract expectation: %+v", withoutContract)
		}
	}

	withContract := generationPlanProjectionExpectations(testGenerationArtifacts(), "sha256:sandbox-contract")
	if len(withContract) != len(withoutContract)+1 {
		t.Fatalf("expectation count = %d want %d", len(withContract), len(withoutContract)+1)
	}
	first := withContract[0]
	if first.ProjectionKind != store.GenerationPlanProjectionSandboxContract ||
		first.ProjectionVersion != store.GenerationPlanProjectionVersion ||
		first.PayloadDigest != "sha256:sandbox-contract" {
		t.Fatalf("sandbox contract expectation = %+v", first)
	}
}

func TestVerifyStoredGenerationPlanProjectionsRequiresPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	_, err := srv.verifyStoredGenerationPlanProjections(ctx, store.RuntimeGenerationDetails{GenerationID: "missing_plan_generation"}, testGenerationArtifacts(), "")
	if err == nil || !strings.Contains(err.Error(), "generation plan is required") {
		t.Fatalf("expected required missing plan error, got %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceChecksExistingPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	details := serverGenerationPlanFrozenEvidenceDetails()
	artifacts := serverGenerationPlanFrozenEvidenceArtifacts()
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err != nil {
		t.Fatalf("verify frozen evidence: %v", err)
	}
	artifacts.RunscBinaryDigest = "sha256:changed"
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "runsc pin mismatch") {
		t.Fatalf("expected runsc mismatch, got %v", err)
	}
	details = serverGenerationPlanFrozenEvidenceDetails()
	details.CheckpointPlanDigest = "sha256:changed"
	artifacts = serverGenerationPlanFrozenEvidenceArtifacts()
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "checkpoint plan digest mismatch") {
		t.Fatalf("expected checkpoint plan digest mismatch, got %v", err)
	}
	details = serverGenerationPlanFrozenEvidenceDetails()
	details.CheckpointDriverStatesDigest = ""
	details.CheckpointPlanDigest = store.GenerationPlanDigest(storeServerFrozenEvidenceCanonicalPayload(t))
	artifacts = serverGenerationPlanFrozenEvidenceArtifacts()
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "checkpoint driver-state digest is required") {
		t.Fatalf("expected checkpoint driver-state fence error, got %v", err)
	}
}

func TestVerifyGenerationPlanDataVolumesChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	volumes := sessionRuntimeDataVolumes{
		Workspace: store.SessionWorkspaceVolume{
			HostPath:              "/var/lib/harness/sessions/sess_frozen_evidence",
			RuntimeIdentityDigest: "sha256:identity",
		},
		DriverHome: store.SessionDriverHomeVolume{
			HostPath:              "/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code",
			RuntimeIdentityDigest: "sha256:identity",
		},
	}
	if err := srv.verifyGenerationPlanDataVolumes(ctx, "gen_frozen_evidence", volumes); err != nil {
		t.Fatalf("verify data volumes: %v", err)
	}

	volumes.Workspace.HostPath = "/var/lib/harness/sessions/changed"
	if err := srv.verifyGenerationPlanDataVolumes(ctx, "gen_frozen_evidence", volumes); err == nil ||
		!strings.Contains(err.Error(), "data_volumes.workspace.host_path mismatch") {
		t.Fatalf("expected workspace host path mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanNetworkEvidenceChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	payload := validServerGenerationPlanPayload()
	network := payload["network"].(map[string]any)
	network["proxy_port"] = 8080
	network["nft_table_name"] = mustRuntimeResourceNftTableName(t, "gen_frozen_evidence")
	storeServerFrozenEvidencePlan(t, ctx, st, dir, payload)

	details := store.RuntimeGenerationDetails{
		GenerationID:       "gen_frozen_evidence",
		NetworkProfileID:   "net_gen_frozen_evidence",
		RunscNetwork:       "sandbox",
		RunscOverlay2:      "none",
		HostProxyBindURL:   "http://127.0.0.1:8080",
		ProxyPort:          8080,
		HostGatewayIP:      "10.240.0.1",
		SandboxBaseURL:     "http://10.240.0.1:8080",
		NetnsName:          "harness-gen-frozen",
		NetnsPath:          "/var/run/netns/harness-gen-frozen",
		HostVeth:           "vh-frozen",
		SandboxVeth:        "vs-frozen",
		SandboxIPCIDR:      "10.240.0.2/30",
		HostSideCIDR:       "10.240.0.1/30",
		EgressPolicyID:     "egress_frozen",
		EgressPolicyDigest: "egress_digest",
		DNSPolicy:          "off",
	}
	if err := srv.verifyGenerationPlanNetworkEvidence(ctx, "gen_frozen_evidence", details); err != nil {
		t.Fatalf("verify network evidence: %v", err)
	}

	details.HostVeth = "changed-veth"
	if err := srv.verifyGenerationPlanNetworkEvidence(ctx, "gen_frozen_evidence", details); err == nil ||
		!strings.Contains(err.Error(), "network.host_veth mismatch") {
		t.Fatalf("expected host veth mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanRuntimeArtifactPathsChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	details := store.RuntimeGenerationDetails{
		ControlDirPath:      "/var/lib/harness/run/control/gen_frozen_evidence",
		ControlManifestPath: "/var/lib/harness/run/control/gen_frozen_evidence/session.json",
		BundleDirPath:       "/var/lib/harness/run/runtime/gen_frozen_evidence",
		SpecPath:            "/var/lib/harness/run/runtime/gen_frozen_evidence/config.json",
		BridgeDirPath:       "/var/lib/harness/run/bridge/gen_frozen_evidence",
		LogDirPath:          "/var/lib/harness/logs/gen_frozen_evidence",
	}
	if err := srv.verifyGenerationPlanRuntimeArtifactPaths(ctx, "gen_frozen_evidence", details); err != nil {
		t.Fatalf("verify runtime artifact paths: %v", err)
	}

	details.SpecPath = "/var/lib/harness/run/runtime/changed/config.json"
	if err := srv.verifyGenerationPlanRuntimeArtifactPaths(ctx, "gen_frozen_evidence", details); err == nil ||
		!strings.Contains(err.Error(), "runtime_artifacts.spec_path mismatch") {
		t.Fatalf("expected spec path mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanMountPlanEvidenceChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	details := store.RuntimeGenerationDetails{
		GenerationID:   "gen_frozen_evidence",
		DriverID:       "claude_code",
		ControlDirPath: "/var/lib/harness/run/control/gen_frozen_evidence",
		BridgeDirPath:  "/var/lib/harness/run/bridge/gen_frozen_evidence",
	}
	volumes := sessionRuntimeDataVolumes{
		Workspace: store.SessionWorkspaceVolume{
			HostPath: "/var/lib/harness/sessions/sess_frozen_evidence",
		},
		DriverHome: store.SessionDriverHomeVolume{
			HostPath: "/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code",
		},
	}
	if err := srv.verifyGenerationPlanMountPlanEvidence(ctx, "gen_frozen_evidence", details, volumes, nil); err != nil {
		t.Fatalf("verify mount plan evidence: %v", err)
	}

	volumes.Workspace.HostPath = "/var/lib/harness/sessions/changed"
	if err := srv.verifyGenerationPlanMountPlanEvidence(ctx, "gen_frozen_evidence", details, volumes, nil); err == nil ||
		!strings.Contains(err.Error(), "mounts.workspace.source mismatch") {
		t.Fatalf("expected workspace mount mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanRuntimeResourceEvidenceChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	if err := srv.verifyGenerationPlanRuntimeResourceEvidence(ctx, "gen_frozen_evidence", "sha256:resource"); err != nil {
		t.Fatalf("verify runtime resource evidence: %v", err)
	}
	if err := srv.verifyGenerationPlanRuntimeResourceEvidence(ctx, "gen_frozen_evidence", "sha256:changed"); err == nil ||
		!strings.Contains(err.Error(), "runtime_artifacts.resource_identity_digest mismatch") {
		t.Fatalf("expected resource identity mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanSourceDigestEvidenceChecksStoredInputEvidence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	plan := storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())
	storeServerSyntheticSandboxContractParentForPlan(t, ctx, st, plan)
	storeServerSandboxContractInputEvidenceFromPlan(t, ctx, st, plan)

	if err := srv.verifyGenerationPlanSourceDigestEvidence(ctx, "sess_frozen_evidence", "gen_frozen_evidence"); err != nil {
		t.Fatalf("verify source digest evidence: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sandbox_contract_input_evidence
SET agent_manifest_digest = 'sha256:changed'
WHERE contract_id = ?`, sandboxContractID("gen_frozen_evidence")); err != nil {
		t.Fatalf("mutate sandbox contract input evidence: %v", err)
	}
	if err := srv.verifyGenerationPlanSourceDigestEvidence(ctx, "sess_frozen_evidence", "gen_frozen_evidence"); err == nil ||
		!strings.Contains(err.Error(), "source_digests.agent_manifest_digest mismatch") {
		t.Fatalf("expected agent manifest source digest mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanSandboxContractEvidenceChecksStoredRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_sandbox_contract_verify", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	if err := srv.startEnsuredGeneration(ctx, session, ensuredGeneration{Allocation: allocation, IsNew: true}, startFailureInputAcceptable); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err := srv.verifyGenerationPlanSandboxContractEvidence(ctx, allocation.GenerationID, session.ID); err != nil {
		t.Fatalf("verify sandbox contract evidence: %v", err)
	}

	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET payload_digest = 'sha256:changed'
WHERE generation_id = ?
  AND projection_kind = ?`, allocation.GenerationID, store.GenerationPlanProjectionSandboxContract); err != nil {
		t.Fatalf("corrupt sandbox contract projection: %v", err)
	}
	if err := srv.verifyGenerationPlanSandboxContractEvidence(ctx, allocation.GenerationID, session.ID); err == nil ||
		!strings.Contains(err.Error(), "sandbox_contract projection digest mismatch") {
		t.Fatalf("expected sandbox contract projection mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceChecksStoredProjectionRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET payload_digest = 'sha256:changed'
WHERE generation_id = ?
  AND projection_kind = ?`, "gen_frozen_evidence", store.GenerationPlanProjectionBundle); err != nil {
		t.Fatalf("corrupt stored projection row: %v", err)
	}

	err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", serverGenerationPlanFrozenEvidenceDetails(), serverGenerationPlanFrozenEvidenceArtifacts())
	if err == nil || !strings.Contains(err.Error(), "generation plan checkpoint bundle digest mismatch") {
		t.Fatalf("expected stored projection row checkpoint mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceUsesStoredProjectionRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	artifacts := serverGenerationPlanFrozenEvidenceArtifacts()
	artifacts.ManifestDigest = "sha256:mutated-control-manifest-row"
	artifacts.ProjectedManifestDigest = "sha256:mutated-projected-manifest-row"
	artifacts.BundleDigest = "sha256:mutated-bundle-row"
	artifacts.RuntimeConfigDigest = "sha256:mutated-runtime-config-row"
	artifacts.SpecDigest = "sha256:mutated-spec-row"

	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", serverGenerationPlanFrozenEvidenceDetails(), artifacts); err != nil {
		t.Fatalf("verify frozen evidence from stored projection rows: %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceChecksPlanIdentity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())
	plan, err := st.GetGenerationPlan(ctx, "gen_frozen_evidence")
	if err != nil {
		t.Fatalf("get generation plan: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(plan.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan: %v", err)
	}
	payload["identity"].(map[string]any)["session_id"] = "sess_drifted"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical generation plan: %v", err)
	}
	planDigest := store.GenerationPlanDigest(canonical)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), planDigest, "gen_frozen_evidence"); err != nil {
		t.Fatalf("mutate generation plan identity: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET plan_digest = ?
WHERE generation_id = ?`, planDigest, "gen_frozen_evidence"); err != nil {
		t.Fatalf("align projection plan digests: %v", err)
	}

	err = srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", serverGenerationPlanFrozenEvidenceDetails(), serverGenerationPlanFrozenEvidenceArtifacts())
	if err == nil || !strings.Contains(err.Error(), "identity.session_id mismatch") {
		t.Fatalf("expected plan identity mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceChecksContentSnapshots(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	snapshotPath := filepath.Join(dir, "content", "skills", "sha256-skills")
	snapshotDigest := writeServerContentSnapshotFixture(t, snapshotPath)
	planPayload := validServerGenerationPlanPayload()
	contentSnapshots := planPayload["content_snapshots"].(map[string]any)
	contentSnapshots["skills"] = map[string]any{
		"kind":                   "skills",
		"digest":                 snapshotDigest,
		"immutable_host_path":    snapshotPath,
		"mount_destination":      "/harness-skills",
		"source_evidence_digest": "sha256:skills-source",
		"retention_class":        "generation_plan",
	}
	planPayload["mounts"].(map[string]any)["content_snapshots"] = map[string]any{
		"skills": map[string]any{
			"mount_name":  "skills_snapshot",
			"type":        "bind",
			"mode":        "ro",
			"exact":       true,
			"source":      snapshotPath,
			"destination": "/harness-skills",
			"digest":      snapshotDigest,
		},
	}
	workspaceVolume := planPayload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	workspaceVolume["platform_content_mount_scope"] = "immutable_content_snapshots"
	plan := storeServerFrozenEvidencePlan(t, ctx, st, dir, planPayload)
	if _, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               snapshotDigest,
		ImmutableHostPath:    snapshotPath,
		MountDestination:     "/harness-skills",
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
	}); err != nil {
		t.Fatalf("store content snapshot: %v", err)
	}

	details := serverGenerationPlanFrozenEvidenceDetails()
	details.CheckpointPlanDigest = plan.PlanDigest
	artifacts := serverGenerationPlanFrozenEvidenceArtifacts()
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err != nil {
		t.Fatalf("verify content snapshot frozen evidence: %v", err)
	}

	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE content_snapshots
SET mount_destination = '/harness-skills-drifted'
WHERE snapshot_kind = ?
  AND snapshot_digest = ?`, store.ContentSnapshotKindSkills, snapshotDigest); err != nil {
		t.Fatalf("mutate stored content snapshot: %v", err)
	}
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "skills content snapshot mount destination must be /harness-skills") {
		t.Fatalf("expected content snapshot metadata mismatch, got %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE content_snapshots
SET mount_destination = '/harness-skills'
WHERE snapshot_kind = ?
  AND snapshot_digest = ?`, store.ContentSnapshotKindSkills, snapshotDigest); err != nil {
		t.Fatalf("restore stored content snapshot: %v", err)
	}

	if err := os.WriteFile(filepath.Join(snapshotPath, "README.md"), []byte("mutated skills"), 0o644); err != nil {
		t.Fatalf("mutate content snapshot payload: %v", err)
	}
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "content snapshot skills digest mismatch") {
		t.Fatalf("expected content snapshot digest mismatch, got %v", err)
	}

	if _, err := st.DBForTest().ExecContext(ctx, `
DELETE FROM content_snapshots
WHERE snapshot_kind = ?
  AND snapshot_digest = ?`, store.ContentSnapshotKindSkills, snapshotDigest); err != nil {
		t.Fatalf("delete stored content snapshot: %v", err)
	}
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "generation plan content snapshot skills") {
		t.Fatalf("expected content snapshot mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceRequiresPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	session := createServerTestSession(t, ctx, st, dir, "sess_missing_plan", string(sessionstate.Created), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'starting', 'owner', ?)`, "gen_missing_plan", session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_missing_plan", serverGenerationPlanFrozenEvidenceDetails(), serverGenerationPlanFrozenEvidenceArtifacts()); err == nil ||
		!strings.Contains(err.Error(), "generation plan is required") {
		t.Fatalf("expected required missing plan error, got %v", err)
	}
}

func TestGenerationPlanContentSnapshotRefs(t *testing.T) {
	payload := validServerGenerationPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["skills"] = map[string]any{
		"kind":                   "skills",
		"digest":                 "sha256:skills",
		"immutable_host_path":    "/var/lib/harness/content/skills/sha256-skills",
		"mount_destination":      "/harness-skills",
		"source_evidence_digest": "sha256:skills-source",
		"retention_class":        "generation_plan",
	}
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical generation plan payload: %v", err)
	}
	digests := generationplan.ContentSnapshotRefs(canonical)
	if len(digests) != 1 || digests["skills"] != "sha256:skills" {
		t.Fatalf("content snapshot digests = %+v", digests)
	}
}

func TestGenerationPlanRuntimeArtifactsValidatesStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	payload := validServerGenerationPlanPayload()
	payload["runtime_artifacts"].(map[string]any)["materialized_driver_config"] = "invalid"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical invalid generation plan payload: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), store.GenerationPlanDigest(canonical), "gen_frozen_evidence"); err != nil {
		t.Fatalf("corrupt generation plan payload: %v", err)
	}

	if _, err := srv.generationPlanRuntimeArtifacts(ctx, "gen_frozen_evidence"); err == nil ||
		!strings.Contains(err.Error(), "runtime_artifacts.materialized_driver_config must be an array") {
		t.Fatalf("expected stored plan validation error, got %v", err)
	}
}

func TestGenerationContentSnapshotsForStartValidatesStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	payload := validServerGenerationPlanPayload()
	payload["runtime_artifacts"].(map[string]any)["materialized_driver_config"] = "invalid"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical invalid generation plan payload: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), store.GenerationPlanDigest(canonical), "gen_frozen_evidence"); err != nil {
		t.Fatalf("corrupt generation plan payload: %v", err)
	}

	_, err = srv.generationContentSnapshotsForStart(ctx, store.Session{}, store.RuntimeGenerationDetails{GenerationID: "gen_frozen_evidence"}, false)
	if err == nil || !strings.Contains(err.Error(), "runtime_artifacts.materialized_driver_config must be an array") {
		t.Fatalf("expected stored plan validation error, got %v", err)
	}
}

func TestRuntimeResourceInstanceCheckpointRestoreTransitions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_resource_checkpoint_restore", string(sessionstate.Created), time.Now().UTC(), nil)
	enableSessionAutoCheckpoint(t, ctx, st, session.ID)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	if err := srv.startEnsuredGeneration(ctx, session, ensuredGeneration{Allocation: allocation, IsNew: true}, startFailureInputAcceptable); err != nil {
		t.Fatalf("start ensured generation: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, session.ID, string(sessionstate.RunningIdle), nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}
	if err := srv.checkpointGeneration(ctx, store.CheckpointCandidate{
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
	}, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("checkpoint generation: %v", err)
	}
	instance, err := st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get checkpointed runtime resource: %v", err)
	}
	if instance.State != store.RuntimeResourceCheckpointReserved {
		t.Fatalf("runtime resource after checkpoint=%s want %s", instance.State, store.RuntimeResourceCheckpointReserved)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after checkpoint"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected restore status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	instance, err = st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get restored runtime resource: %v", err)
	}
	if instance.State != store.RuntimeResourceLive ||
		instance.WorkerID != owner.UUID ||
		instance.IdempotencyToken != "" ||
		instance.LeaseExpiresAt != nil {
		t.Fatalf("unexpected runtime resource after restore: %+v", instance)
	}
	_, starts := rt.requests()
	if len(starts) != 2 || !starts[1].RestoreFromCheckpoint {
		t.Fatalf("expected second start to restore checkpoint, got %+v", starts)
	}
	if starts[1].Generation.RunscContainerID != instance.RunscContainerID ||
		starts[1].Generation.RunscBinaryDigest != instance.RunscBinaryDigest ||
		starts[1].Generation.NetnsName != instance.NetnsName {
		t.Fatalf("restore start did not use runtime resource identity: start=%+v instance=%+v", starts[1].Generation, instance)
	}
	volumeConfig, err := srv.dataVolumeProvisionerConfig()
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	workspaceVolume, err := st.VerifySessionWorkspaceVolume(ctx, store.VerifySessionWorkspaceVolumeParams{
		SessionID: session.ID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify workspace volume: %v", err)
	}
	driverHomeVolume, err := st.VerifySessionDriverHomeVolume(ctx, store.VerifySessionDriverHomeVolumeParams{
		SessionID: session.ID,
		Driver:    session.DriverID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify driver home volume: %v", err)
	}
	if starts[1].WorkspaceHostPath != workspaceVolume.HostPath ||
		starts[1].AgentHomeHostPath != driverHomeVolume.HostPath {
		t.Fatalf("restore start did not use data volume paths: start=%+v workspace=%+v home=%+v", starts[1], workspaceVolume, driverHomeVolume)
	}
}

func TestReserveRuntimeResourceCheckpointRequiresInstance(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_checkpoint_missing_instance", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	srv := &Server{store: st}

	err = srv.reserveRuntimeResourceCheckpoint(ctx, allocation.GenerationID)
	if err == nil || !strings.Contains(err.Error(), "runtime resource instance is required for checkpoint reserve") {
		t.Fatalf("expected missing runtime resource invariant failure, got %v", err)
	}
}

func TestStartEnsuredGenerationDestroysRuntimeAfterOwnerLoss(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_start_owner_loss", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &startHookRuntime{
		onStart: func(req runtime.StartRequest) {
			if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = 'other_owner',
    lease_expires_at = ?
WHERE generation_id = ?`, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano), req.GenerationID); err != nil {
				t.Fatalf("steal generation lease: %v", err)
			}
		},
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	err = srv.startEnsuredGeneration(ctx, session, ensuredGeneration{
		Allocation: allocation,
		IsNew:      true,
	}, startFailureInputAcceptable)
	if !errors.Is(err, errGenerationStartLeaseLost) {
		t.Fatalf("expected start lease loss, got %v", err)
	}
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != runscID {
		t.Fatalf("owner loss should destroy started runtime %q, got %+v", runscID, got)
	}
	var status, ownerValue, errorClass, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), COALESCE(g.error_class, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&status, &ownerValue, &errorClass, &networkState, &resourceState); err != nil {
		t.Fatalf("query generation after owner loss: %v", err)
	}
	if status != "starting" ||
		ownerValue != "other_owner" ||
		errorClass != "" ||
		networkState != "allocating" ||
		resourceState != "allocating" {
		t.Fatalf("owner loss should not fail or reclaim the stolen generation: status=%s owner=%q class=%q network=%s resource=%s", status, ownerValue, errorClass, networkState, resourceState)
	}
	instance, err := st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime resource instance after owner loss: %v", err)
	}
	if instance.State != store.RuntimeResourceRetiring {
		t.Fatalf("runtime resource after owner loss=%s want %s", instance.State, store.RuntimeResourceRetiring)
	}
	var runtimeEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'generation.error'`, session.ID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count generation events: %v", err)
	}
	if runtimeEvents != 0 {
		t.Fatalf("owner loss should not publish generation error events, got %d", runtimeEvents)
	}
}

func TestRuntimeFailureClassDetectsPostStartProbeFailure(t *testing.T) {
	cases := []string{
		"harness-bridge-client probe exited with status 1",
		"bridge probe starting failed",
		"bridge startup probe did not complete: missing probe_network",
		"probe GET /healthz returned 503, want one of [200]",
		"probe POST /v1/messages returned 502, want one of [400]",
	}
	for _, message := range cases {
		if got := runtimeFailureClass(message); got != "probe_failed_post_start" {
			t.Fatalf("runtimeFailureClass(%q)=%s want probe_failed_post_start", message, got)
		}
	}
}

func TestRuntimeFailureClassDetectsManifestFailures(t *testing.T) {
	cases := []struct {
		message string
		want    string
	}{
		{"sandbox_secret_disallowed", "sandbox_secret_disallowed"},
		{"shell_secret_disallowed", "shell_secret_disallowed"},
		{"runsc run: exit status 1: control manifest digest mismatch", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected session_id=sess_a got sess_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected generation_id=gen_a got gen_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected network_profile_id=net_a got net_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected agent_runtime_profile_id=arp_a got arp_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected anthropic_api_key_secret_id=anthropic_api_key got other", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected anthropic_auth_token_secret_id=anthropic_auth_token got other", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected manifest_version=1 got 2", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected secret_version=local got rotated", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: secret mount /harness-secrets missing", "manifest_digest_mismatch"},
	}
	for _, tc := range cases {
		if got := runtimeFailureClass(tc.message); got != tc.want {
			t.Fatalf("runtimeFailureClass(%q)=%s want %s", tc.message, got, tc.want)
		}
	}
}

func TestSendMessagePrepareFailureMarksGenerationFailedAndReclaimable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_prepare_fail", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:   cfg,
		store: st,
		runtime: failingRuntime{
			prepareErr: errors.New("pre-start sandbox network probe failed"),
		},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "probe_failed_pre_start" ||
		body["error"] != "sandbox network probe failed before start" {
		t.Fatalf("unexpected response body: %v", body)
	}
	var generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state, s.status,
       COALESCE(s.error_class, ''), COALESCE(s.failure_reason, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.session_id = ?`, session.ID).Scan(
		&generationStatus,
		&errorClass,
		&networkState,
		&resourceState,
		&sessionStatus,
		&sessionErrorClass,
		&sessionFailureReason,
	); err != nil {
		t.Fatalf("query generation state: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" ||
		sessionStatus != string(sessionstate.Created) ||
		sessionErrorClass != "" ||
		sessionFailureReason != "" {
		t.Fatalf("unexpected failed generation state: generation=%s class=%s network=%s resource=%s session=%s session_class=%s session_reason=%s", generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason)
	}
	if !sessionstate.CanAcceptInput(sessionStatus) {
		t.Fatalf("session should remain input-acceptable after prepare failure, got %s", sessionStatus)
	}
	var runtimeEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'generation.error'`, session.ID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count generation error events: %v", err)
	}
	if runtimeEvents != 1 {
		t.Fatalf("expected one generation.error event, got %d", runtimeEvents)
	}
	var turns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 0 {
		t.Fatalf("prepare failure should happen before turn creation, got %d turns", turns)
	}
}

func TestDestroySessionCancelsPendingTurnAndReclaimsGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_destroy_pending", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, mustRuntimeResourceHostID(t), time.Now().UTC())
	enqueued, err := st.EnqueueTurnMessage(ctx, store.EnqueueTurnMessageParams{
		SessionID: session.ID,
		Content:   "hello",
		Now:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/sessions/"+session.ID, nil)
	rec := httptest.NewRecorder()
	srv.destroySession(rec, req, session.ID)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body %s", rec.Code, rec.Body.String())
	}
	destroyIDs := rt.runtimeDestroyRequests()
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if len(destroyIDs) != 1 || destroyIDs[0] != runscID {
		t.Fatalf("destroy session should tear down runsc container id %q, got %+v", runscID, destroyIDs)
	}
	destroyGenerationRequests := rt.destroyGenerationRequests()
	if len(destroyGenerationRequests) != 1 || destroyGenerationRequests[0].GenerationID != allocation.GenerationID {
		t.Fatalf("destroy session should clean generation resources, got %+v", destroyGenerationRequests)
	}
	var sessionStatus, turnStatus, turnErrorClass, generationStatus, generationErrorClass, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT s.status, t.status, COALESCE(t.error_class, ''), g.status, COALESCE(g.error_class, ''),
       n.allocation_state, r.resource_state
FROM sessions s
JOIN turns t ON t.session_id = s.id
JOIN runtime_generations g ON g.session_id = s.id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE s.id = ?
  AND t.id = ?`, session.ID, enqueued.TurnID).Scan(
		&sessionStatus,
		&turnStatus,
		&turnErrorClass,
		&generationStatus,
		&generationErrorClass,
		&networkState,
		&resourceState,
	); err != nil {
		t.Fatalf("query destroyed state: %v", err)
	}
	if sessionStatus != string(sessionstate.Destroyed) ||
		turnStatus != "canceled" ||
		turnErrorClass != "session_destroyed" ||
		generationStatus != "failed" ||
		generationErrorClass != "session_destroyed" ||
		networkState != "destroyed" ||
		resourceState != "destroyed" {
		t.Fatalf("unexpected destroyed state: session=%s turn=%s turn_error=%s generation=%s generation_error=%s network=%s resource=%s",
			sessionStatus, turnStatus, turnErrorClass, generationStatus, generationErrorClass, networkState, resourceState)
	}
	var destroyedEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'session.destroyed'`, session.ID).Scan(&destroyedEvents); err != nil {
		t.Fatalf("count destroyed events: %v", err)
	}
	if destroyedEvents != 1 {
		t.Fatalf("expected one durable destroyed event, got %d", destroyedEvents)
	}
}

func TestRunMaintenancePollsBridgeOutbox(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_bridge_poll", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Bridge.PollInterval = config.Duration{Duration: 10 * time.Millisecond}
	modelAccessAllowed := true
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config: store.ResourceAllocatorConfig{
			RunDir:                      cfg.Harness.RunDir,
			CIDRPool:                    cfg.Harness.Network.CIDRPool.Prefix,
			EgressDorisFEHosts:          cfg.Harness.Network.Egress.DorisFEHosts,
			EgressDorisBEHosts:          cfg.Harness.Network.Egress.DorisBEHosts,
			EgressDorisPorts:            cfg.Harness.Network.Egress.DorisPorts,
			EgressDNSPolicy:             string(cfg.Harness.Network.Egress.DNSPolicy),
			HostProxyBindURL:            cfg.ModelProxy.BindURL,
			ProxyPort:                   cfg.ModelProxy.BindPort,
			DriverID:                    "claude_code",
			Model:                       "sonnet",
			OutputFormat:                "stream-json",
			SandboxUID:                  cfg.Harness.SandboxIdentity.UID,
			SandboxGID:                  cfg.Harness.SandboxIdentity.GID,
			ModelAccessAllowed:          &modelAccessAllowed,
			ProviderCredentialsHostOnly: true,
			SandboxModelProxyBaseURL:    cfg.ModelProxy.SandboxBaseURL,
		},
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, "host-bridge-poll", time.Now().UTC())
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	outbox, err := bridge.OpenQueue(details.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	if _, err := outbox.Write(ctx, bridge.Envelope{
		MessageID:    "msg_hello",
		RequestID:    "req_hello",
		Type:         bridge.TypeHello,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		Payload:      serverBridgeHelloPayload(t, session.DriverID),
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()

	response := waitForBridgeInboxResponse(t, runCtx, details.BridgeDirPath, bridge.TypeHelloAck, "req_hello")
	if response.GenerationID != allocation.GenerationID || response.SessionID != session.ID {
		t.Fatalf("unexpected bridge response identity: %+v", response)
	}
	if _, err := os.Stat(bridge.HeartbeatPath(details.BridgeDirPath, bridge.HostHeartbeatFile)); err != nil {
		t.Fatalf("host heartbeat file missing after bridge poll: %v", err)
	}
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}
}

func TestRunMaintenanceRequiresPositiveBridgeIntervals(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)

	tests := []struct {
		name string
		edit func(*config.Config)
		want string
	}{
		{
			name: "heartbeat",
			edit: func(cfg *config.Config) {
				cfg.Harness.Bridge.HeartbeatInterval = config.Duration{}
			},
			want: "bridge heartbeat interval must be > 0",
		},
		{
			name: "poll",
			edit: func(cfg *config.Config) {
				cfg.Harness.Bridge.PollInterval = config.Duration{}
			},
			want: "bridge poll interval must be > 0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testServerConfig(filepath.Join(dir, tc.name))
			tc.edit(&cfg)
			srv := &Server{
				cfg:   cfg,
				store: st,
				hub:   events.NewHub(),
				log:   slog.Default(),
			}
			srv.SetOwnerUUID(owner.UUID)
			err := srv.RunMaintenance(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("maintenance err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestRunMaintenanceRecoversGenerationThatExpiresAfterStartup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_expiring_generation", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Bridge.HeartbeatInterval = config.Duration{Duration: 10 * time.Millisecond}
	cfg.Harness.Bridge.ReconnectGrace = config.Duration{Duration: 20 * time.Millisecond}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, mustRuntimeResourceHostID(t), time.Now().UTC())
	expiresAt := time.Now().UTC().Add(25 * time.Millisecond)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?,
    lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?`,
		store.GenerationLeaseOwner("previous-owner"),
		expiresAt.Format(time.RFC3339Nano),
		expiresAt.Add(-time.Minute).Format(time.RFC3339Nano),
		allocation.GenerationID,
	); err != nil {
		t.Fatalf("move generation to previous owner: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()

	waitForGenerationStatus(t, runCtx, st, allocation.GenerationID, "failed")
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}

	var errorClass, leaseOwnerAfter string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COALESCE(error_class, ''), COALESCE(lease_owner, '')
FROM runtime_generations
WHERE generation_id = ?`, allocation.GenerationID).Scan(&errorClass, &leaseOwnerAfter); err != nil {
		t.Fatalf("query recovered generation: %v", err)
	}
	if errorClass != "orchestrator_restart_reconnect_grace_expired" || leaseOwnerAfter != "" {
		t.Fatalf("unexpected recovered generation: error_class=%s lease_owner=%s", errorClass, leaseOwnerAfter)
	}
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != runscID {
		t.Fatalf("maintenance should destroy runtime before repair using runsc container id %q, got %+v", runscID, got)
	}
	if _, starts := rt.requests(); len(starts) != 0 {
		t.Fatalf("maintenance should not cold-start without a queued turn: %+v", starts)
	}
}

func TestRunMaintenanceRecoversCurrentOwnerExpiredLeasedTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_current_owner_expired", string(sessionstate.RunningActive), now.Add(-2*time.Minute), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Bridge.HeartbeatInterval = config.Duration{Duration: 10 * time.Millisecond}
	cfg.Harness.Bridge.ReconnectGrace = config.Duration{Duration: 20 * time.Millisecond}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now.Add(-2 * time.Minute),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, now.Add(-2*time.Minute+time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, mustRuntimeResourceHostID(t), now.Add(-2*time.Minute+2*time.Second))
	turnID, err := st.EnqueueTurn(ctx, session.ID, "hi", now.Add(-2*time.Minute+3*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	expiredAt := now.Add(-time.Minute)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'active',
    lease_owner = ?,
    lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?`,
		leaseOwner,
		expiredAt.Format(time.RFC3339Nano),
		expiredAt.Add(-time.Minute).Format(time.RFC3339Nano),
		allocation.GenerationID,
	); err != nil {
		t.Fatalf("expire generation: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE turns
SET status = 'leased',
    generation_id = ?,
    lease_owner = ?,
    lease_expires_at = ?,
    claim_request_id = 'claim-expired',
    claim_granted_at = ?
WHERE id = ?`,
		allocation.GenerationID,
		leaseOwner,
		expiredAt.Format(time.RFC3339Nano),
		expiredAt.Add(-time.Minute).Format(time.RFC3339Nano),
		turnID,
	); err != nil {
		t.Fatalf("expire leased turn: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()

	waitForGenerationStatus(t, runCtx, st, allocation.GenerationID, "failed")
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}

	var turnStatus string
	var turnGeneration sql.NullString
	var attempt int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT status, generation_id, attempt
FROM turns
WHERE id = ?`, turnID).Scan(&turnStatus, &turnGeneration, &attempt); err != nil {
		t.Fatalf("query recovered turn: %v", err)
	}
	if turnStatus != "queued" || turnGeneration.Valid || attempt != 1 {
		t.Fatalf("leased turn was not requeued: status=%s generation=%v attempt=%d", turnStatus, turnGeneration, attempt)
	}
	var sessionStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT status
FROM sessions
WHERE id = ?`, session.ID).Scan(&sessionStatus); err != nil {
		t.Fatalf("query recovered session: %v", err)
	}
	if sessionStatus != string(sessionstate.RunningIdle) {
		t.Fatalf("recovered session status=%s want %s", sessionStatus, sessionstate.RunningIdle)
	}
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != runscID {
		t.Fatalf("maintenance should destroy expired runtime %q before repair, got %+v", runscID, got)
	}
}

func TestExpiredRuntimeRecoverySkipsRepairWhenRuntimeCleanupFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_recovery_cleanup_fail", string(sessionstate.RunningIdle), now.Add(-2*time.Minute), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-3 * time.Minute),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, now.Add(-3*time.Minute+time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, mustRuntimeResourceHostID(t), now.Add(-3*time.Minute+2*time.Second))
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'idle',
    lease_owner = ?,
    lease_expires_at = ?
WHERE generation_id = ?`, store.GenerationLeaseOwner("previous-owner"), now.Add(-time.Minute).Format(time.RFC3339Nano), allocation.GenerationID); err != nil {
		t.Fatalf("expire generation: %v", err)
	}
	rt := &recordingRuntime{destroyRuntimeErr: errors.New("runsc delete failed")}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	recovered, err := srv.RecoverExpiredRuntimeResources(ctx, now)
	if err != nil {
		t.Fatalf("recover expired runtime resources: %v", err)
	}
	if recovered.RuntimeCleanupSkipped != 1 ||
		recovered.ReconnectGraceFailed != 0 ||
		recovered.ExpiredLifecycleFailed != 0 ||
		recovered.UnknownAfterAckStarted != 0 {
		t.Fatalf("cleanup failure should skip repair, got %+v", recovered)
	}
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != runscID {
		t.Fatalf("expected runtime cleanup attempt for %q, got %+v", runscID, got)
	}
	var generationStatus, ownerAfter, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &ownerAfter, &networkState, &resourceState); err != nil {
		t.Fatalf("query skipped recovery state: %v", err)
	}
	if generationStatus != "idle" ||
		ownerAfter != string(store.GenerationLeaseOwner("previous-owner")) ||
		networkState != "live" ||
		resourceState != "live" {
		t.Fatalf("cleanup failure should leave DB non-reclaimable: generation=%s owner=%s network=%s resource=%s", generationStatus, ownerAfter, networkState, resourceState)
	}
}

func TestDestroyReclaimableGenerationResourcesMarksDestroyedOnlyAfterRuntimeCleanup(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	for _, tc := range []struct {
		name       string
		destroyErr error
		wantState  string
	}{
		{name: "cleanup succeeds", wantState: "destroyed"},
		{name: "cleanup fails", destroyErr: errors.New("netns busy"), wantState: "reclaimable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st, owner := openServerOwnedStore(t, ctx, dir)
			cfg := testServerConfig(dir)
			createServerTestSession(t, ctx, st, dir, "sess_cleanup", string(sessionstate.Created), now.Add(-time.Minute), nil)
			allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
				SessionID: "sess_cleanup",
				Owner:     store.GenerationLeaseOwner(owner.UUID),
				LeaseTTL:  time.Minute,
				Now:       now.Add(-time.Minute),
				Config:    serverTestAllocatorConfig(cfg, "claude_code"),
			})
			if err != nil {
				t.Fatalf("allocate generation: %v", err)
			}
			if err := st.MarkGenerationResourcesLive(ctx, "sess_cleanup", allocation.GenerationID, allocation.Owner, now.Add(-59*time.Second)); err != nil {
				t.Fatalf("mark resources live: %v", err)
			}
			createServerRuntimeResourceLive(t, ctx, st, "sess_cleanup", allocation, owner.UUID, mustRuntimeResourceHostID(t), now.Add(-59*time.Second+time.Millisecond))
			if err := st.FailGeneration(ctx, store.FailGenerationParams{
				SessionID:    "sess_cleanup",
				GenerationID: allocation.GenerationID,
				Owner:        allocation.Owner,
				ErrorClass:   "probe_failed_pre_start",
				Reason:       "probe failed",
				Now:          now.Add(-58 * time.Second),
			}); err != nil {
				t.Fatalf("fail generation: %v", err)
			}

			rt := &recordingRuntime{destroyErr: tc.destroyErr}
			srv := &Server{
				cfg:     cfg,
				store:   st,
				runtime: rt,
				watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
				hub:     events.NewHub(),
				log:     slog.Default(),
			}
			srv.destroyReclaimableGenerationResources(ctx, now)

			calls := rt.destroyGenerationRequests()
			if len(calls) != 1 || calls[0].GenerationID != allocation.GenerationID {
				t.Fatalf("destroy generation calls=%+v", calls)
			}
			var networkState, resourceState string
			if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, allocation.GenerationID).Scan(&networkState, &resourceState); err != nil {
				t.Fatalf("query resource states: %v", err)
			}
			if networkState != tc.wantState || resourceState != tc.wantState {
				t.Fatalf("unexpected states after cleanup: network=%s resource=%s want %s", networkState, resourceState, tc.wantState)
			}
			if tc.destroyErr == nil {
				instance, err := st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
				if err != nil {
					t.Fatalf("get cleaned runtime resource instance: %v", err)
				}
				if instance.State != store.RuntimeResourceDestroyed || len(instance.EvidenceJSON) == 0 || instance.EvidenceDigest == "" || instance.VerifiedAt == nil {
					t.Fatalf("runtime resource cleanup evidence not completed: %+v", instance)
				}
			}
		})
	}
}

func TestCleanupGenerationResourcesRequiresRuntimeResourceInstance(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	createServerTestSession(t, ctx, st, dir, "sess_cleanup_missing_instance", string(sessionstate.Created), now.Add(-time.Minute), nil)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_cleanup_missing_instance",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-time.Minute),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_cleanup_missing_instance", allocation.GenerationID, allocation.Owner, now.Add(-59*time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    "sess_cleanup_missing_instance",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		ErrorClass:   "probe_failed_pre_start",
		Reason:       "probe failed",
		Now:          now.Add(-58 * time.Second),
	}); err != nil {
		t.Fatalf("fail generation: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	err = srv.cleanupGenerationResources(ctx, "sess_cleanup_missing_instance", allocation.GenerationID, now)
	if err == nil || !strings.Contains(err.Error(), "runtime resource instance is required for generation cleanup") {
		t.Fatalf("expected missing runtime resource invariant failure, got %v", err)
	}
	if calls := rt.destroyGenerationRequests(); len(calls) != 0 {
		t.Fatalf("cleanup should not fall back to legacy resource details, calls=%+v", calls)
	}
}

func TestDestroyReclaimableGenerationResourcesRemovesFilesystemWithRealRuntime(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	createServerTestSession(t, ctx, st, dir, "sess_cleanup_real", string(sessionstate.Created), now.Add(-time.Minute), nil)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_cleanup_real",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-time.Minute),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_cleanup_real", allocation.GenerationID, allocation.Owner, now.Add(-59*time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, "sess_cleanup_real", allocation, owner.UUID, mustRuntimeResourceHostID(t), now.Add(-59*time.Second+time.Millisecond))
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    "sess_cleanup_real",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		ErrorClass:   "probe_failed_pre_start",
		Reason:       "probe failed",
		Now:          now.Add(-58 * time.Second),
	}); err != nil {
		t.Fatalf("fail generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_cleanup_real", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details: %v", err)
	}
	createServerGenerationFilesystem(t, details)
	currentRunscBinary, _ := currentRunscBinaryMetadataForServerTest(t)

	realRuntime := runtime.New(runtime.Config{
		RunscNetwork:  "sandbox",
		RunscOverlay2: "none",
		RunscRoot:     filepath.Join(dir, "runsc-root"),
		RunDir:        cfg.Harness.RunDir,
		CommandRunner: serverCommandRunner{
			outputs: map[string][]byte{
				"runsc --version": []byte("runsc test"),
			},
			fail: map[string]error{
				currentRunscBinary + " -root " + filepath.Join(dir, "runsc-root") + " state " + details.RunscContainerID: errors.New("not found"),
				"ip link show " + details.HostVeth:                                                errors.New("does not exist"),
				"nft list table inet " + mustRuntimeResourceNftTableName(t, details.GenerationID): errors.New("No such table"),
			},
		},
	})
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: realRuntime,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.destroyReclaimableGenerationResources(ctx, now)

	for _, path := range []string{details.CheckpointPath, details.ControlDirPath, details.BundleDirPath, details.BridgeDirPath, details.LogDirPath} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("expected cleanup path %s to be removed, stat err=%v", path, err)
		}
	}
	var networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, allocation.GenerationID).Scan(&networkState, &resourceState); err != nil {
		t.Fatalf("query resource states: %v", err)
	}
	if networkState != "destroyed" || resourceState != "destroyed" {
		t.Fatalf("unexpected states after real runtime cleanup: network=%s resource=%s", networkState, resourceState)
	}
}

func TestRunMaintenancePublishesBridgeOutputAndCompletion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_bridge_events", string(sessionstate.RunningActive), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE sessions SET driver_id = 'sh', mode = 'shell' WHERE id = ?`, session.ID); err != nil {
		t.Fatalf("set shell agent: %v", err)
	}
	session.DriverID = "sh"
	session.Mode = "shell"
	cfg := testServerConfig(dir)
	cfg.Harness.Bridge.PollInterval = config.Duration{Duration: 10 * time.Millisecond}
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "sh"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, "host-bridge-events", time.Now().UTC())
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, session.ID, "run", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "req_claim",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	sandboxSourceIP := serverSandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID)
	outbox, err := bridge.OpenQueue(details.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	if _, err := outbox.Write(ctx, bridge.Envelope{
		MessageID:    "msg_started",
		Type:         bridge.TypeAckTurnStarted,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(fmt.Sprintf(`{"sandbox_source_ip":%q}`, sandboxSourceIP)),
	}); err != nil {
		t.Fatalf("write started: %v", err)
	}
	if _, err := outbox.Write(ctx, bridge.Envelope{
		MessageID:    "msg_output",
		Type:         bridge.TypeEmitOutput,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"output_sequence":1,"stream":"stdout","payload":{"line":"{\"type\":\"harness.shell_output\",\"stream\":\"stdout\",\"text\":\"ok\\n\"}"}}`),
	}); err != nil {
		t.Fatalf("write output: %v", err)
	}
	if _, err := outbox.Write(ctx, bridge.Envelope{
		MessageID:    "msg_done",
		Type:         bridge.TypeAckTurnCompleted,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"status":"completed"}`),
	}); err != nil {
		t.Fatalf("write done: %v", err)
	}

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe(session.ID)
	defer cancelEvents()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, hub),
		hub:     hub,
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()
	waitForSessionStatus(t, runCtx, st, session.ID, string(sessionstate.RunningIdle))
	waitForHubEvent(t, eventsCh, bridge.TypeAckTurnCompleted)
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}

	var assistantMessages int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM messages
WHERE session_id = ?
  AND role = 'assistant'
  AND content = 'ok
'`, session.ID).Scan(&assistantMessages); err != nil {
		t.Fatalf("assistant messages: %v", err)
	}
	if assistantMessages != 1 {
		t.Fatalf("assistant messages=%d want 1", assistantMessages)
	}
}

func TestBridgeFailedCompletionDoesNotFailSession(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_bridge_failed", string(sessionstate.RunningActive), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, "host-bridge-failed", time.Now().UTC())
	turnID, err := st.EnqueueTurn(ctx, session.ID, "run", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "req_failed",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, store.AckStartedParams{
		SessionID:       session.ID,
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: serverSandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID),
		LeaseTTL:        time.Minute,
		Now:             time.Now().UTC(),
	}); err != nil {
		t.Fatalf("ack started: %v", err)
	}
	completionPayload := map[string]string{
		"status":      "failed",
		"error_class": "agent_error",
		"error":       "agent exited 1",
	}
	eventID, err := st.CompleteTurn(ctx, store.CompleteTurnParams{
		SessionID:      session.ID,
		GenerationID:   allocation.GenerationID,
		TurnID:         turnID,
		Owner:          allocation.Owner,
		TerminalStatus: "failed",
		ErrorClass:     "agent_error",
		Error:          "agent exited 1",
		EventType:      bridge.TypeAckTurnCompleted,
		EventDedupeKey: "ack_completed:" + allocation.GenerationID,
		EventPayload:   completionPayload,
		Now:            time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("complete failed turn: %v", err)
	}

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe(session.ID)
	defer cancelEvents()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, hub),
		hub:     hub,
		log:     slog.Default(),
	}
	envelopePayload, err := json.Marshal(completionPayload)
	if err != nil {
		t.Fatalf("marshal completion payload: %v", err)
	}
	srv.handleBridgeCommittedEnvelope(ctx, bridge.Envelope{
		Type:         bridge.TypeAckTurnCompleted,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      envelopePayload,
	}, eventID)

	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.RunningIdle) || !sessionstate.CanAcceptInput(got.Status) {
		t.Fatalf("failed completion should leave session retryable, got %s", got.Status)
	}
	seenCompletion := false
	for {
		select {
		case event := <-eventsCh:
			switch event.Type {
			case bridge.TypeAckTurnCompleted:
				seenCompletion = true
			case "session." + string(sessionstate.Failed), "session.error":
				t.Fatalf("unexpected terminal event after failed completion: %+v", event)
			}
		default:
			if !seenCompletion {
				t.Fatalf("missing durable completion event")
			}
			return
		}
	}
}

func TestRunMaintenancePrunesRetainedEvents(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	createServerTestSession(t, ctx, st, dir, "sess_events_a", string(sessionstate.Created), now, nil)
	createServerTestSession(t, ctx, st, dir, "sess_events_b", string(sessionstate.Created), now, nil)
	firstID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_events_a",
		Type:      "test.event",
		Payload:   map[string]string{"name": "first"},
		Now:       now.Add(-3 * time.Second),
	})
	if err != nil {
		t.Fatalf("append first event: %v", err)
	}
	secondID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_events_b",
		Type:      "test.event",
		Payload:   map[string]string{"name": "second"},
		Now:       now.Add(-2 * time.Second),
	})
	if err != nil {
		t.Fatalf("append second event: %v", err)
	}
	thirdID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_events_a",
		Type:      "test.event",
		Payload:   map[string]string{"name": "third"},
		Now:       now.Add(-time.Second),
	})
	if err != nil {
		t.Fatalf("append third event: %v", err)
	}

	cfg := testServerConfig(dir)
	cfg.Harness.Events.RetentionWindow = config.Duration{Duration: time.Hour}
	cfg.Harness.Events.RetentionRows = 2
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()
	waitForEventIDs(t, runCtx, st, []int64{secondID, thirdID})
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}
	if _, ok, err := st.GetEvent(ctx, firstID); err != nil || ok {
		t.Fatalf("first event retained ok=%v err=%v", ok, err)
	}
}

func TestSendMessageRejectsExpiredSessionBeforeAllocation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	expired := time.Now().UTC().Add(-time.Second)
	session := createServerTestSession(t, ctx, st, dir, "sess_expired", string(sessionstate.Created), time.Now().UTC(), &expired)
	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d body %s", rec.Code, rec.Body.String())
	}
	var generations int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations`).Scan(&generations); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if generations != 0 {
		t.Fatalf("expired session should not allocate generation, got %d", generations)
	}
}

func TestProxyCorrelationUnixSocketPublishesDurableEvents(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "hp-proxy-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	now := time.Now().UTC()
	allocation, turnID, sandboxSourceIP := createServerRunningProxyTurn(t, ctx, st, cfg, owner.UUID, dir, "sess_proxy_http", now)

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe("sess_proxy_http")
	defer cancelEvents()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, hub),
		hub:     hub,
		log:     slog.Default(),
	}

	publicReq := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/start", strings.NewReader(fmt.Sprintf(`{"sandbox_source_ip":%q,"proxy_request_id":"proxy_public"}`, sandboxSourceIP)))
	publicRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(publicRec, publicReq)
	if publicRec.Code != http.StatusNotFound {
		t.Fatalf("public proxy route status=%d body=%s", publicRec.Code, publicRec.Body.String())
	}

	directReq := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/start", strings.NewReader(fmt.Sprintf(`{"sandbox_source_ip":%q,"proxy_request_id":"proxy_direct"}`, sandboxSourceIP)))
	directRec := httptest.NewRecorder()
	srv.ProxyCorrelationRoutes().ServeHTTP(directRec, directReq)
	if directRec.Code != http.StatusForbidden {
		t.Fatalf("proxy route without peer credentials status=%d body=%s", directRec.Code, directRec.Body.String())
	}

	listener, socketPath, err := srv.ListenProxyCorrelation()
	if err != nil {
		t.Fatalf("listen proxy correlation: %v", err)
	}
	assertProxyCorrelationSocketPermissions(t, socketPath, cfg.Harness.ProxyServiceIdentity.GID)
	proxyServer := srv.ProxyCorrelationServer()
	errCh := make(chan error, 1)
	go func() { errCh <- proxyServer.Serve(listener) }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := proxyServer.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown proxy server: %v", err)
		}
		_ = os.Remove(socketPath)
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("proxy server stopped: %v", err)
		}
	})

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}}
	clientPost := func(path, body string) (int, []byte) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://proxy.internal"+path, strings.NewReader(body))
		if err != nil {
			t.Fatalf("build proxy request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("proxy request %s: %v", path, err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read proxy response: %v", err)
		}
		return resp.StatusCode, data
	}

	startStatus, startBody := clientPost("/internal/proxy/requests/start", fmt.Sprintf(`{
		"sandbox_source_ip":%q,
		"proxy_request_id":"proxy_http_1",
		"upstream_model":"claude-sonnet",
		"upstream_base_url":"https://api.anthropic.test"
	}`, sandboxSourceIP))
	if startStatus != http.StatusOK {
		t.Fatalf("start status=%d body=%s", startStatus, string(startBody))
	}
	var startResp struct {
		SessionID       string `json:"session_id"`
		TurnID          int64  `json:"turn_id"`
		GenerationID    string `json:"generation_id"`
		RequestSequence int64  `json:"request_sequence"`
		EventID         int64  `json:"event_id"`
		Replayed        bool   `json:"replayed"`
	}
	if err := json.Unmarshal(startBody, &startResp); err != nil {
		t.Fatalf("decode start response: %v", err)
	}
	if startResp.SessionID != "sess_proxy_http" || startResp.GenerationID != allocation.GenerationID ||
		startResp.TurnID != turnID || startResp.RequestSequence != 1 || startResp.EventID == 0 || startResp.Replayed {
		t.Fatalf("unexpected start response: %+v allocation=%+v turn=%d", startResp, allocation, turnID)
	}
	startEvent := waitForHubEvent(t, eventsCh, "proxy.request.started")
	if startEvent.EventID != startResp.EventID || startEvent.ProxyRequestID != "proxy_http_1" ||
		startEvent.SessionID != "sess_proxy_http" {
		t.Fatalf("unexpected start hub event: %+v response=%+v", startEvent, startResp)
	}

	finishStatus, finishBody := clientPost("/internal/proxy/requests/finish", `{
		"proxy_request_id":"proxy_http_1",
		"http_status":200,
		"upstream_total_latency_ms":321,
		"retry_count":0
	}`)
	if finishStatus != http.StatusOK {
		t.Fatalf("finish status=%d body=%s", finishStatus, string(finishBody))
	}
	var finishResp struct {
		Status       string `json:"status"`
		EventID      int64  `json:"event_id"`
		EventType    string `json:"event_type"`
		SessionID    string `json:"session_id"`
		TurnID       int64  `json:"turn_id"`
		GenerationID string `json:"generation_id"`
		Replayed     bool   `json:"replayed"`
	}
	if err := json.Unmarshal(finishBody, &finishResp); err != nil {
		t.Fatalf("decode finish response: %v", err)
	}
	if finishResp.Status != "accepted" || finishResp.EventType != "proxy.request.completed" ||
		finishResp.SessionID != "sess_proxy_http" || finishResp.GenerationID != allocation.GenerationID ||
		finishResp.TurnID != turnID || finishResp.EventID <= startResp.EventID || finishResp.Replayed {
		t.Fatalf("unexpected finish response: %+v start=%+v", finishResp, startResp)
	}
	finishEvent := waitForHubEvent(t, eventsCh, "proxy.request.completed")
	if finishEvent.EventID != finishResp.EventID || finishEvent.ProxyRequestID != "proxy_http_1" {
		t.Fatalf("unexpected finish hub event: %+v response=%+v", finishEvent, finishResp)
	}

	unknownStatus, unknownBody := clientPost("/internal/proxy/requests/finish", `{"proxy_request_id":"proxy_missing"}`)
	if unknownStatus != http.StatusOK {
		t.Fatalf("unknown finish status=%d body=%s", unknownStatus, string(unknownBody))
	}
	var unknownResp map[string]string
	if err := json.Unmarshal(unknownBody, &unknownResp); err != nil {
		t.Fatalf("decode unknown finish response: %v", err)
	}
	if unknownResp["status"] != "stale_unknown_request" {
		t.Fatalf("unexpected unknown finish response: %v", unknownResp)
	}
}

func TestEventsStreamReplaysDurableEventsAfterLastEventID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	createServerTestSession(t, ctx, st, dir, "sess_a", string(sessionstate.RunningActive), now, nil)
	createServerTestSession(t, ctx, st, dir, "sess_b", string(sessionstate.RunningActive), now, nil)

	firstID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_a",
		Type:      bridge.TypeAckTurnStarted,
		Payload:   map[string]string{"step": "first"},
		Now:       now,
	})
	if err != nil {
		t.Fatalf("append first event: %v", err)
	}
	secondID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_b",
		Type:      bridge.TypeEmitOutput,
		Payload:   map[string]string{"line": "second"},
		Now:       now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("append second event: %v", err)
	}
	thirdID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_a",
		Type:      bridge.TypeAckTurnCompleted,
		Payload:   map[string]string{"status": "completed"},
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("append third event: %v", err)
	}

	srv := &Server{store: st, hub: events.NewHub(), log: slog.Default()}
	req := httptest.NewRequest(http.MethodGet, "/api/events/stream?last_event_id="+strconv.FormatInt(firstID, 10), nil)
	lastEventID, ok, err := parseLastEventID(req)
	if err != nil || !ok || lastEventID != firstID {
		t.Fatalf("parse last_event_id: id=%d ok=%v err=%v", lastEventID, ok, err)
	}
	rec := httptest.NewRecorder()
	replayedThrough, err := srv.writeSSEReplay(req.Context(), rec, rec, "", lastEventID)
	if err != nil {
		t.Fatalf("write replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("replayed through=%d want %d", replayedThrough, thirdID)
	}
	body := rec.Body.String()
	if strings.Contains(body, "id: "+strconv.FormatInt(firstID, 10)+"\n") {
		t.Fatalf("replay included already-seen event: %s", body)
	}
	assertContains(t, body, "id: "+strconv.FormatInt(secondID, 10)+"\n")
	assertContains(t, body, "event: "+bridge.TypeEmitOutput+"\n")
	assertContains(t, body, `"event_id":`+strconv.FormatInt(secondID, 10))
	assertContains(t, body, `"session_id":"sess_b"`)
	assertContains(t, body, `"payload":{"line":"second"}`)
	assertContains(t, body, "id: "+strconv.FormatInt(thirdID, 10)+"\n")
	assertContains(t, body, "event: "+bridge.TypeAckTurnCompleted+"\n")
	if strings.Index(body, "id: "+strconv.FormatInt(secondID, 10)+"\n") >
		strings.Index(body, "id: "+strconv.FormatInt(thirdID, 10)+"\n") {
		t.Fatalf("replayed events out of order: %s", body)
	}

	filtered := httptest.NewRecorder()
	replayedThrough, err = srv.writeSSEReplay(req.Context(), filtered, filtered, "sess_a", firstID)
	if err != nil {
		t.Fatalf("write filtered replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("filtered replayed through=%d want %d", replayedThrough, thirdID)
	}
	filteredBody := filtered.Body.String()
	if strings.Contains(filteredBody, "id: "+strconv.FormatInt(secondID, 10)+"\n") {
		t.Fatalf("filtered replay included another session: %s", filteredBody)
	}
	assertContains(t, filteredBody, "id: "+strconv.FormatInt(thirdID, 10)+"\n")

	headerReq := httptest.NewRequest(http.MethodGet, "/api/events/stream?last_event_id=1", nil)
	headerReq.Header.Set("Last-Event-ID", strconv.FormatInt(thirdID, 10))
	lastEventID, ok, err = parseLastEventID(headerReq)
	if err != nil || !ok || lastEventID != thirdID {
		t.Fatalf("header Last-Event-ID should win: id=%d ok=%v err=%v", lastEventID, ok, err)
	}

	deleted, err := st.PruneEvents(ctx, store.PruneEventsParams{
		RetentionRows: 2,
		Now:           now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("prune replay events: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d want 1", deleted)
	}

	gap := httptest.NewRecorder()
	replayedThrough, err = srv.writeSSEReplay(req.Context(), gap, gap, "", 0)
	if err != nil {
		t.Fatalf("write gap replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("gap replayed through=%d want %d", replayedThrough, thirdID)
	}
	gapBody := gap.Body.String()
	assertContains(t, gapBody, "id: "+strconv.FormatInt(secondID-1, 10)+"\n")
	assertContains(t, gapBody, "event: replay_gap\n")
	assertContains(t, gapBody, `"requested_last_event_id":0`)
	assertContains(t, gapBody, `"oldest_available":`+strconv.FormatInt(secondID, 10))
	assertContains(t, gapBody, `"session_id_filter":null`)
	assertContains(t, gapBody, `"reason":"retention_window_exceeded"`)
	assertContains(t, gapBody, "id: "+strconv.FormatInt(secondID, 10)+"\n")
	assertContains(t, gapBody, "id: "+strconv.FormatInt(thirdID, 10)+"\n")
	if strings.Contains(gapBody, `"payload":{"step":"first"}`) {
		t.Fatalf("gap replay included pruned event: %s", gapBody)
	}

	filteredGap := httptest.NewRecorder()
	replayedThrough, err = srv.writeSSEReplay(req.Context(), filteredGap, filteredGap, "sess_a", 0)
	if err != nil {
		t.Fatalf("write filtered gap replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("filtered gap replayed through=%d want %d", replayedThrough, thirdID)
	}
	filteredGapBody := filteredGap.Body.String()
	assertContains(t, filteredGapBody, "id: "+strconv.FormatInt(thirdID-1, 10)+"\n")
	assertContains(t, filteredGapBody, "event: replay_gap\n")
	assertContains(t, filteredGapBody, `"oldest_available":`+strconv.FormatInt(thirdID, 10))
	assertContains(t, filteredGapBody, `"session_id_filter":"sess_a"`)
	assertContains(t, filteredGapBody, "id: "+strconv.FormatInt(thirdID, 10)+"\n")
	if strings.Contains(filteredGapBody, `"payload":{"line":"second"}`) {
		t.Fatalf("filtered gap replay included another session: %s", filteredGapBody)
	}
}

func waitForBridgeInboxResponse(t *testing.T, ctx context.Context, root, responseType, requestID string) bridge.Envelope {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		queue, err := bridge.OpenQueue(root, bridge.InboxDir)
		if err != nil {
			t.Fatalf("open inbox: %v", err)
		}
		files, err := queue.ReadAll()
		if err != nil {
			t.Fatalf("read inbox: %v", err)
		}
		for _, file := range files {
			if file.Envelope.Type == responseType && file.Envelope.RequestID == requestID {
				if err := file.Unlink(); err != nil {
					t.Fatalf("unlink response: %v", err)
				}
				return file.Envelope
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before bridge response")
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for bridge response %s/%s", responseType, requestID)
	return bridge.Envelope{}
}

type instantRuntime struct{}

var instantRuntimePrepareCalls int64

func (instantRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	atomic.AddInt64(&instantRuntimePrepareCalls, 1)
	return testGenerationArtifacts(), nil
}

func (instantRuntime) RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	atomic.AddInt64(&instantRuntimePrepareCalls, 1)
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (instantRuntime) MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error {
	return nil
}

func (instantRuntime) PrepareGenerationNetwork(context.Context, runtime.StartRequest) error {
	return nil
}

func (instantRuntime) Start(ctx context.Context, req runtime.StartRequest, output func(runtime.Output)) runtime.Result {
	if output != nil {
		output(runtime.Output{Stream: "stdout", Line: `{"type":"result","subtype":"success","result":"ok"}`})
	}
	return serverRuntimeStartResult(req)
}

func (instantRuntime) Destroy(context.Context, string) error {
	return nil
}

func (instantRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtimeCleanupEvidenceForDetails(details), nil
}

func (instantRuntime) Interrupt(string) error {
	return nil
}

func (instantRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

type recordingRuntime struct {
	mu                  sync.Mutex
	prepareRequests     []runtime.StartRequest
	materializeRequests []runtime.StartRequest
	networkRequests     []runtime.StartRequest
	startRequests       []runtime.StartRequest
	destroyRuntimeIDs   []string
	destroyRuntimeErr   error
	destroyRequests     []store.RuntimeGenerationDetails
	destroyErr          error
	checkpointReqs      []runtime.CheckpointRequest
	checkpointErr       error
	interruptSessionIDs []string
}

type planOrderRuntime struct {
	recordingRuntime
	store                                   *store.Store
	t                                       *testing.T
	planSeenBeforeNetwork                   bool
	planSeenBeforeMaterializeRender         bool
	planSeenBeforeMaterialize               bool
	projectionVerificationObserved          bool
	projectionVerificationBeforeMaterialize bool
	runtimeResourceClaimedBeforeNetwork     bool
	runtimeResourceClaimedBeforeMaterialize bool
}

type corruptProjectionBeforeMaterializeRuntime struct {
	recordingRuntime
	store        *store.Store
	t            *testing.T
	corrupted    bool
	materialized bool
}

func (r *planOrderRuntime) RenderGenerationArtifacts(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.t.Helper()
	projection, err := r.recordingRuntime.RenderGenerationArtifacts(ctx, req)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, err
	}
	plan, err := r.store.GetGenerationPlan(ctx, req.GenerationID)
	if errors.Is(err, sql.ErrNoRows) {
		return projection, nil
	}
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("get generation plan before materialize render: %w", err)
	}
	planArtifacts, err := generationplan.RuntimeArtifacts(plan.CanonicalPayload)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("read generation plan runtime artifacts before materialize render: %w", err)
	}
	if reflect.DeepEqual(req.PreparedArtifacts, planArtifacts) {
		r.planSeenBeforeMaterializeRender = true
	}
	return projection, nil
}

func (r *corruptProjectionBeforeMaterializeRuntime) RenderGenerationArtifacts(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.t.Helper()
	projection, err := r.recordingRuntime.RenderGenerationArtifacts(ctx, req)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, err
	}
	plan, err := r.store.GetGenerationPlan(ctx, req.GenerationID)
	if errors.Is(err, sql.ErrNoRows) {
		return projection, nil
	}
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("get generation plan before corrupting projection: %w", err)
	}
	planArtifacts, err := generationplan.RuntimeArtifacts(plan.CanonicalPayload)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("read generation plan runtime artifacts before corrupting projection: %w", err)
	}
	if r.corrupted || !reflect.DeepEqual(req.PreparedArtifacts, planArtifacts) {
		return projection, nil
	}
	if _, err := r.store.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET payload_digest = 'sha256:changed-before-materialize'
WHERE generation_id = ?
  AND projection_kind = ?`, req.GenerationID, store.GenerationPlanProjectionBundle); err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("corrupt generation plan projection before materialize: %w", err)
	}
	r.corrupted = true
	return projection, nil
}

func (r *corruptProjectionBeforeMaterializeRuntime) MaterializeGenerationArtifacts(req runtime.StartRequest, projection runtime.GenerationArtifactProjection) error {
	r.materialized = true
	return r.recordingRuntime.MaterializeGenerationArtifacts(req, projection)
}

type blockingPrepareRuntime struct {
	recordingRuntime
	prepareStarted chan struct{}
	releasePrepare chan struct{}
	startedOnce    sync.Once
	releaseOnce    sync.Once
}

func newBlockingPrepareRuntime() *blockingPrepareRuntime {
	return &blockingPrepareRuntime{
		prepareStarted: make(chan struct{}),
		releasePrepare: make(chan struct{}),
	}
}

func (r *blockingPrepareRuntime) RenderGenerationArtifacts(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	r.startedOnce.Do(func() { close(r.prepareStarted) })
	select {
	case <-r.releasePrepare:
		return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
	case <-ctx.Done():
		return runtime.GenerationArtifactProjection{}, ctx.Err()
	}
}

func (r *blockingPrepareRuntime) PrepareGeneration(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	projection, err := r.RenderGenerationArtifacts(ctx, req)
	return projection.Artifacts, err
}

func (r *blockingPrepareRuntime) release() {
	r.releaseOnce.Do(func() { close(r.releasePrepare) })
}

type startHookRuntime struct {
	recordingRuntime
	onStart func(runtime.StartRequest)
}

func (r *startHookRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if r.onStart != nil {
		r.onStart(req)
	}
	return serverRuntimeStartResult(req)
}

type claimAfterProbeRuntime struct {
	recordingRuntime
}

func (r *claimAfterProbeRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	result := serverRuntimeStartResult(req)
	if result.Err != nil {
		return result
	}
	outbox, err := bridge.OpenQueue(req.Generation.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		return runtime.Result{Err: err}
	}
	if _, err := outbox.Write(context.Background(), bridge.Envelope{
		RequestID:    "test_claim_after_probe",
		Type:         bridge.TypeClaimNextTurn,
		SessionID:    req.SessionID,
		GenerationID: req.GenerationID,
	}); err != nil {
		return runtime.Result{Err: err}
	}
	return result
}

func (r *recordingRuntime) PrepareGeneration(_ context.Context, req runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	return testGenerationArtifacts(), nil
}

func (r *recordingRuntime) RenderGenerationArtifacts(_ context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (r *recordingRuntime) MaterializeGenerationArtifacts(req runtime.StartRequest, _ runtime.GenerationArtifactProjection) error {
	r.mu.Lock()
	r.materializeRequests = append(r.materializeRequests, req)
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) PrepareGenerationNetwork(_ context.Context, req runtime.StartRequest) error {
	r.mu.Lock()
	r.networkRequests = append(r.networkRequests, req)
	r.mu.Unlock()
	return nil
}

func (r *planOrderRuntime) MaterializeGenerationArtifacts(req runtime.StartRequest, projection runtime.GenerationArtifactProjection) error {
	r.t.Helper()
	if err := r.recordingRuntime.MaterializeGenerationArtifacts(req, projection); err != nil {
		return err
	}
	if err := r.requireStoredPlanAndMaterializationClaim(context.Background(), req, "materialize"); err != nil {
		return err
	}
	r.planSeenBeforeMaterialize = true
	r.projectionVerificationBeforeMaterialize = true
	r.runtimeResourceClaimedBeforeMaterialize = true
	return nil
}

func (r *planOrderRuntime) PrepareGenerationNetwork(ctx context.Context, req runtime.StartRequest) error {
	r.t.Helper()
	if err := r.recordingRuntime.PrepareGenerationNetwork(ctx, req); err != nil {
		return err
	}
	if err := r.requireStoredPlanAndMaterializationClaim(ctx, req, "network prepare"); err != nil {
		return err
	}
	r.planSeenBeforeNetwork = true
	r.projectionVerificationObserved = true
	r.runtimeResourceClaimedBeforeNetwork = true
	return nil
}

func (r *planOrderRuntime) requireStoredPlanAndMaterializationClaim(ctx context.Context, req runtime.StartRequest, phase string) error {
	plan, err := r.store.GetGenerationPlan(ctx, req.GenerationID)
	if err != nil {
		return fmt.Errorf("get generation plan before %s: %w", phase, err)
	}
	planArtifacts, err := generationplan.RuntimeArtifacts(plan.CanonicalPayload)
	if err != nil {
		return fmt.Errorf("read generation plan runtime artifacts before %s: %w", phase, err)
	}
	if !reflect.DeepEqual(req.PreparedArtifacts, planArtifacts) {
		return fmt.Errorf("prepared artifacts before %s did not come from stored plan: got %+v want %+v", phase, req.PreparedArtifacts, planArtifacts)
	}
	projections, err := r.store.ListGenerationPlanProjections(ctx, req.GenerationID)
	if err != nil {
		return fmt.Errorf("list generation plan projections before %s: %w", phase, err)
	}
	if len(projections) != len(store.GenerationPlanProjectionKinds()) {
		return fmt.Errorf("generation plan projection count before %s = %d want %d", phase, len(projections), len(store.GenerationPlanProjectionKinds()))
	}
	verified, err := r.store.VerifyGenerationPlanProjections(ctx, store.VerifyGenerationPlanProjectionsParams{
		GenerationID: req.GenerationID,
		Expected:     generationPlanProjectionExpectationsForDetails(req.Generation, req.PreparedArtifacts, ""),
	})
	if err != nil {
		return fmt.Errorf("verify generation plan projections before %s: %w", phase, err)
	}
	if !verified {
		return fmt.Errorf("generation plan projections were missing before %s", phase)
	}
	for _, projection := range projections {
		if projection.PlanDigest != plan.PlanDigest {
			return fmt.Errorf("generation plan projection %s digest before %s = %s want %s", projection.ProjectionKind, phase, projection.PlanDigest, plan.PlanDigest)
		}
	}
	instance, err := r.store.GetRuntimeResourceInstance(ctx, req.GenerationID)
	if err != nil {
		return fmt.Errorf("get runtime resource instance before %s: %w", phase, err)
	}
	if instance.State != store.RuntimeResourceMaterializing {
		return fmt.Errorf("runtime resource state before %s = %s want %s", phase, instance.State, store.RuntimeResourceMaterializing)
	}
	if strings.TrimSpace(instance.WorkerID) == "" || strings.TrimSpace(instance.HostID) == "" {
		return fmt.Errorf("runtime resource worker lease was not claimed before %s", phase)
	}
	if instance.LeaseExpiresAt == nil {
		return fmt.Errorf("runtime resource materialization lease is missing before %s", phase)
	}
	if instance.IdempotencyToken != "start:"+req.GenerationID {
		return fmt.Errorf("runtime resource idempotency token before %s = %q want %q", phase, instance.IdempotencyToken, "start:"+req.GenerationID)
	}
	return nil
}

func (r *recordingRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	return serverRuntimeStartResult(req)
}

func (r *recordingRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	r.mu.Lock()
	r.destroyRequests = append(r.destroyRequests, details)
	err := r.destroyErr
	r.mu.Unlock()
	if err != nil {
		return runtime.GenerationResourceCleanup{}, err
	}
	return runtimeCleanupEvidenceForDetails(details), nil
}

func runtimeCleanupEvidenceForDetails(details store.RuntimeGenerationDetails) runtime.GenerationResourceCleanup {
	filesystem := map[string]string{}
	addFilesystem := func(label, path string) {
		if strings.TrimSpace(path) != "" {
			filesystem[label+":"+path] = "lstat:absent"
		}
	}
	addFilesystem("checkpoint", details.CheckpointPath)
	addFilesystem("control", details.ControlDirPath)
	addFilesystem("control_manifest", details.ControlManifestPath)
	addFilesystem("bundle", details.BundleDirPath)
	addFilesystem("spec", details.SpecPath)
	addFilesystem("bridge", details.BridgeDirPath)
	if strings.TrimSpace(details.NetworkHostsPath) != "" {
		addFilesystem("network", filepath.Dir(details.NetworkHostsPath))
	}
	addFilesystem("network_hosts", details.NetworkHostsPath)
	addFilesystem("log", details.LogDirPath)
	if len(filesystem) == 0 {
		filesystem["test:runtime_resource"] = "lstat:absent"
	}
	return runtime.GenerationResourceCleanup{
		RunscDeleted:      true,
		CheckpointDeleted: true,
		ControlDirDeleted: true,
		BundleDirDeleted:  true,
		BridgeDirDeleted:  true,
		NetworkDirDeleted: true,
		LogDirDeleted:     true,
		NetnsDeleted:      true,
		HostVethDeleted:   true,
		NftTableDeleted:   true,
		RunscState:        "runsc_container:absent; check=test",
		IPNetns:           "netns:absent; check=test",
		IPLink:            "host_veth:absent; check=test",
		NFT:               "nft_table:absent; check=test",
		FilesystemLstat:   filesystem,
	}
}

func (r *recordingRuntime) Destroy(_ context.Context, runtimeID string) error {
	r.mu.Lock()
	r.destroyRuntimeIDs = append(r.destroyRuntimeIDs, runtimeID)
	err := r.destroyRuntimeErr
	r.mu.Unlock()
	return err
}

func (r *recordingRuntime) Interrupt(sessionID string) error {
	r.mu.Lock()
	r.interruptSessionIDs = append(r.interruptSessionIDs, sessionID)
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) Checkpoint(_ context.Context, req runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	r.mu.Lock()
	r.checkpointReqs = append(r.checkpointReqs, req)
	err := r.checkpointErr
	r.mu.Unlock()
	if err != nil {
		return runtime.CheckpointResult{}, err
	}
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

func (r *recordingRuntime) requests() ([]runtime.StartRequest, []runtime.StartRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prepares := append([]runtime.StartRequest(nil), r.prepareRequests...)
	starts := append([]runtime.StartRequest(nil), r.startRequests...)
	return prepares, starts
}

func (r *recordingRuntime) networkPrepareRequests() []runtime.StartRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtime.StartRequest(nil), r.networkRequests...)
}

func (r *recordingRuntime) checkpointRequests() []runtime.CheckpointRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtime.CheckpointRequest(nil), r.checkpointReqs...)
}

func (r *recordingRuntime) destroyGenerationRequests() []store.RuntimeGenerationDetails {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]store.RuntimeGenerationDetails(nil), r.destroyRequests...)
}

func (r *recordingRuntime) runtimeDestroyRequests() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.destroyRuntimeIDs...)
}

type restoreFailingRuntime struct {
	recordingRuntime
	err error
}

func (r *restoreFailingRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if req.RestoreFromCheckpoint {
		return runtime.Result{Err: r.err}
	}
	return serverRuntimeStartResult(req)
}

type restoreStartHookRuntime struct {
	recordingRuntime
	onRestoreStart func()
}

func (r *restoreStartHookRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if req.RestoreFromCheckpoint && r.onRestoreStart != nil {
		r.onRestoreStart()
	}
	return serverRuntimeStartResult(req)
}

type restoreValidationRuntime struct {
	restore       *runtime.Runtime
	startRequests []runtime.StartRequest
}

func (r *restoreValidationRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	return testGenerationArtifacts(), nil
}

func (r *restoreValidationRuntime) RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (r *restoreValidationRuntime) MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error {
	return nil
}

func (r *restoreValidationRuntime) PrepareGenerationNetwork(context.Context, runtime.StartRequest) error {
	return nil
}

func (r *restoreValidationRuntime) Start(ctx context.Context, req runtime.StartRequest, output func(runtime.Output)) runtime.Result {
	r.startRequests = append(r.startRequests, req)
	if req.RestoreFromCheckpoint {
		return r.restore.Start(ctx, req, output)
	}
	return serverRuntimeStartResult(req)
}

func (r *restoreValidationRuntime) Destroy(context.Context, string) error {
	return nil
}

func (r *restoreValidationRuntime) DestroyGenerationResources(context.Context, store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtime.GenerationResourceCleanup{}, nil
}

func (r *restoreValidationRuntime) Interrupt(string) error {
	return nil
}

func (r *restoreValidationRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

type serverCommandRunner struct {
	outputs map[string][]byte
	fail    map[string]error
}

func (r serverCommandRunner) CombinedOutput(_ context.Context, name string, args ...string) ([]byte, error) {
	key := strings.Join(append([]string{name}, args...), " ")
	if err := r.fail[key]; err != nil {
		return nil, err
	}
	if out, ok := r.outputs[key]; ok {
		return out, nil
	}
	return nil, nil
}

type failingRuntime struct {
	prepareErr    error
	err           error
	checkpointErr error
}

func (f failingRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	if f.prepareErr != nil {
		return runtime.GenerationArtifacts{}, f.prepareErr
	}
	return testGenerationArtifacts(), nil
}

func (f failingRuntime) RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	if f.prepareErr != nil {
		return runtime.GenerationArtifactProjection{}, f.prepareErr
	}
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (f failingRuntime) MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error {
	return nil
}

func (f failingRuntime) PrepareGenerationNetwork(context.Context, runtime.StartRequest) error {
	return nil
}

func (f failingRuntime) Start(context.Context, runtime.StartRequest, func(runtime.Output)) runtime.Result {
	return runtime.Result{Err: f.err}
}

func (f failingRuntime) Destroy(context.Context, string) error {
	return nil
}

func (f failingRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtimeCleanupEvidenceForDetails(details), nil
}

func (f failingRuntime) Interrupt(string) error {
	return nil
}

func (f failingRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	if f.checkpointErr != nil {
		return runtime.CheckpointResult{}, f.checkpointErr
	}
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

func testGenerationArtifacts() runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		BundleDir:               "/tmp/bundle",
		SpecPath:                "/tmp/bundle/config.json",
		ManifestPath:            "/tmp/control/session.json",
		ManifestDigest:          "manifest_digest",
		ProjectedManifestDigest: "projected_manifest_digest",
		BundleDigest:            "bundle_digest",
		RuntimeConfigDigest:     "runtime_config_digest",
		SpecDigest:              "spec_digest",
		RunscVersion:            "runsc test",
		RunscBinaryPath:         "/usr/local/bin/runsc-test",
		RunscBinaryDigest:       "sha256:runsc-test",
	}
}

func serverRuntimeStartResult(req runtime.StartRequest) runtime.Result {
	if err := writeServerBridgeBootstrapForRequest(req); err != nil {
		return runtime.Result{Err: err}
	}
	return runtime.Result{
		ControlManifestDigest: req.PreparedArtifacts.ManifestDigest,
		RunscVersion:          req.PreparedArtifacts.RunscVersion,
		PostStartProof:        serverPostStartProofForRequest(req),
	}
}

func writeServerBridgeBootstrapForRequest(req runtime.StartRequest) error {
	if strings.TrimSpace(req.Generation.BridgeDirPath) == "" {
		return nil
	}
	if err := bridge.EnsureLayout(req.Generation.BridgeDirPath); err != nil {
		return err
	}
	if err := bridge.TouchHeartbeat(req.Generation.BridgeDirPath, bridge.BridgeHeartbeatFile, time.Now().UTC()); err != nil {
		return err
	}
	outbox, err := bridge.OpenQueue(req.Generation.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		return err
	}
	ctx := context.Background()
	helloPayload, err := json.Marshal(map[string]any{"driver_id": req.DriverID, "protocol_version": 2, "turn_input_schema": "RunTurn"})
	if err != nil {
		return err
	}
	for _, envelope := range []bridge.Envelope{
		{
			RequestID:    "test_heartbeat",
			Type:         bridge.TypeHeartbeat,
			SessionID:    req.SessionID,
			GenerationID: req.GenerationID,
		},
		{
			RequestID:    "test_hello",
			Type:         bridge.TypeHello,
			SessionID:    req.SessionID,
			GenerationID: req.GenerationID,
			Payload:      helloPayload,
		},
		{
			RequestID:    "test_probe",
			Type:         bridge.TypeProbeNetwork,
			SessionID:    req.SessionID,
			GenerationID: req.GenerationID,
		},
	} {
		if _, err := outbox.Write(ctx, envelope); err != nil {
			return err
		}
	}
	return nil
}

func serverPostStartProofForRequest(req runtime.StartRequest) *store.RuntimeResourcePostStartProof {
	containerID := strings.TrimSpace(req.Generation.RunscContainerID)
	if containerID == "" {
		containerID = "harness-gen-" + req.GenerationID
	}
	runscPlatform := strings.TrimSpace(req.Generation.RunscPlatform)
	if runscPlatform == "" {
		runscPlatform = "systrap"
	}
	runscVersion := strings.TrimSpace(req.Generation.RunscVersion)
	if runscVersion == "" {
		runscVersion = req.PreparedArtifacts.RunscVersion
	}
	runscBinaryPath := strings.TrimSpace(req.Generation.RunscBinaryPath)
	if runscBinaryPath == "" {
		runscBinaryPath = req.PreparedArtifacts.RunscBinaryPath
	}
	runscBinaryDigest := strings.TrimSpace(req.Generation.RunscBinaryDigest)
	if runscBinaryDigest == "" {
		runscBinaryDigest = req.PreparedArtifacts.RunscBinaryDigest
	}
	return &store.RuntimeResourcePostStartProof{
		GenerationID:      req.Generation.GenerationID,
		RunscContainerID:  containerID,
		RunscState:        "runsc_container:" + containerID + ":running; check=test",
		RunscPlatform:     runscPlatform,
		RunscVersion:      runscVersion,
		RunscBinaryPath:   runscBinaryPath,
		RunscBinaryDigest: runscBinaryDigest,
		IPNetns:           "netns:present; check=test",
		IPLink:            "host_veth:present; check=test",
		NFT:               "nft_table:present; check=test",
	}
}

func serverBridgeHelloPayload(t *testing.T, driverID string) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"driver_id":         driverID,
		"protocol_version":  2,
		"turn_input_schema": "RunTurn",
	})
	if err != nil {
		t.Fatalf("marshal bridge hello payload: %v", err)
	}
	return payload
}

func recordServerRuntimeArtifacts(t *testing.T, ctx context.Context, st *store.Store, generationID, manifestDigest, runscVersion string) {
	t.Helper()
	artifacts := testGenerationArtifacts()
	recordServerRuntimeArtifactsWithRunsc(t, ctx, st, generationID, manifestDigest, runscVersion, artifacts.RunscBinaryPath, artifacts.RunscBinaryDigest)
}

func recordServerRuntimeArtifactsWithRunsc(t *testing.T, ctx context.Context, st *store.Store, generationID, manifestDigest, runscVersion, runscPath, runscDigest string) {
	t.Helper()
	artifacts := testGenerationArtifacts()
	artifacts.ManifestDigest = manifestDigest
	artifacts.ProjectedManifestDigest = manifestDigest
	artifacts.RunscVersion = runscVersion
	artifacts.RunscBinaryPath = runscPath
	artifacts.RunscBinaryDigest = runscDigest
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, generationID, store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          artifacts.ManifestDigest,
		ProjectedControlManifestDigest: artifacts.ProjectedManifestDigest,
		BundleDigest:                   artifacts.BundleDigest,
		RuntimeConfigDigest:            artifacts.RuntimeConfigDigest,
		SpecDigest:                     artifacts.SpecDigest,
		RunscVersion:                   artifacts.RunscVersion,
		RunscBinaryPath:                artifacts.RunscBinaryPath,
		RunscBinaryDigest:              artifacts.RunscBinaryDigest,
	}); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	var sessionID string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT session_id
FROM runtime_generations
WHERE generation_id = ?`, generationID).Scan(&sessionID); err != nil {
		t.Fatalf("query generation session: %v", err)
	}
	storeServerGenerationPlanForArtifacts(t, ctx, st, sessionID, generationID, artifacts)
}

func mutateServerRuntimeArtifactDigestMirrors(t *testing.T, ctx context.Context, st *store.Store, generationID string) {
	t.Helper()
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET control_manifest_digest = 'mutated_manifest_digest',
    projected_control_manifest_digest = 'mutated_projected_manifest_digest',
    bundle_digest = 'mutated_bundle_digest',
    runtime_config_digest = 'mutated_runtime_config_digest',
    spec_digest = 'mutated_spec_digest'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("mutate runtime artifact digest mirrors: %v", err)
	}
}

func addServerGenerationPlanSkillsSnapshot(t *testing.T, ctx context.Context, st *store.Store, generationID string) store.ContentSnapshotRecord {
	t.Helper()
	snapshotPath := filepath.Join(t.TempDir(), "skills", generationID)
	snapshotDigest := writeServerContentSnapshotFixture(t, snapshotPath)
	snapshot, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               snapshotDigest,
		ImmutableHostPath:    snapshotPath,
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source-" + generationID,
		RetentionClass:       "generation_plan",
	})
	if err != nil {
		t.Fatalf("store generation plan skills snapshot: %v", err)
	}
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if err != nil {
		t.Fatalf("get generation plan for snapshot: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(plan.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan for snapshot: %v", err)
	}
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots[store.ContentSnapshotKindSkills] = map[string]any{
		"kind":                   snapshot.Kind,
		"digest":                 snapshot.Digest,
		"immutable_host_path":    snapshot.ImmutableHostPath,
		"mount_destination":      snapshot.MountDestination,
		"source_evidence_digest": snapshot.SourceEvidenceDigest,
		"retention_class":        snapshot.RetentionClass,
	}
	mounts := payload["mounts"].(map[string]any)
	snapshotMounts, ok := mounts["content_snapshots"].(map[string]any)
	if !ok {
		snapshotMounts = map[string]any{}
		mounts["content_snapshots"] = snapshotMounts
	}
	snapshotMounts[store.ContentSnapshotKindSkills] = map[string]any{
		"mount_name":  "skills_snapshot",
		"type":        "bind",
		"mode":        "ro",
		"exact":       true,
		"source":      snapshot.ImmutableHostPath,
		"destination": snapshot.MountDestination,
		"digest":      snapshot.Digest,
	}
	workspace := payload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	workspace["platform_content_mount_scope"] = "immutable_content_snapshots"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical generation plan with snapshot: %v", err)
	}
	planDigest := store.GenerationPlanDigest(canonical)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), planDigest, generationID); err != nil {
		t.Fatalf("update generation plan snapshot payload: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET plan_digest = ?
WHERE generation_id = ?`, planDigest, generationID); err != nil {
		t.Fatalf("update projection plan digests for snapshot payload: %v", err)
	}
	return snapshot
}

func writeServerContentSnapshotFixture(t *testing.T, root string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "skill"), 0o755); err != nil {
		t.Fatalf("create content snapshot fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("skills fixture\n"), 0o644); err != nil {
		t.Fatalf("write content snapshot readme: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "skill", "SKILL.md"), []byte("# Fixture\n"), 0o644); err != nil {
		t.Fatalf("write content snapshot skill: %v", err)
	}
	digest, err := contentSnapshotPathDigest(root)
	if err != nil {
		t.Fatalf("digest content snapshot fixture: %v", err)
	}
	return digest
}

func currentRunscBinaryMetadataForServerTest(t *testing.T) (string, string) {
	t.Helper()
	path, err := exec.LookPath("runsc")
	if err != nil {
		t.Fatalf("lookup runsc binary: %v", err)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("resolve runsc binary %q: %v", path, err)
	}
	data, err := os.ReadFile(canonical)
	if err != nil {
		t.Fatalf("read runsc binary %q: %v", canonical, err)
	}
	sum := sha256.Sum256(data)
	return canonical, fmt.Sprintf("sha256:%x", sum[:])
}

func writeServerTestAgentImageManifest(t *testing.T, rootfs string, drivers ...agents.ID) string {
	t.Helper()
	manifestPath, err := serverTestAgentImageManifest(rootfs, drivers...)
	if err != nil {
		t.Fatalf("write test agent image manifest: %v", err)
	}
	return manifestPath
}

func mustWriteServerTestAgentImageManifest(rootfs string, drivers ...agents.ID) string {
	manifestPath, err := serverTestAgentImageManifest(rootfs, drivers...)
	if err != nil {
		panic(err)
	}
	return manifestPath
}

func serverTestAgentImageManifest(rootfs string, drivers ...agents.ID) (string, error) {
	entries := make([]imageManifestDriver, 0, len(drivers))
	buildDrivers := make([]string, 0, len(drivers))
	for _, driverID := range drivers {
		spec, ok := agents.DriverSpecFor(string(driverID))
		if !ok {
			return "", fmt.Errorf("missing driver spec for %s", driverID)
		}
		binaryPath, err := expectedDriverBinaryPath(driverID)
		if err != nil {
			return "", fmt.Errorf("expected driver binary path: %w", err)
		}
		hostPath := filepath.Join(rootfs, strings.TrimPrefix(binaryPath, "/"))
		if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
			return "", fmt.Errorf("mkdir driver binary parent: %w", err)
		}
		content := []byte("test binary for " + string(driverID) + "\n")
		if err := os.WriteFile(hostPath, content, 0o755); err != nil {
			return "", fmt.Errorf("write driver binary: %w", err)
		}
		sum := sha256.Sum256(content)
		entry, err := manifestDriverFromSpec(spec)
		if err != nil {
			return "", fmt.Errorf("manifest driver from spec: %w", err)
		}
		entry.InstalledBinaryDigest = fmt.Sprintf("sha256:%x", sum[:])
		entries = append(entries, entry)
		buildDrivers = append(buildDrivers, string(driverID))
	}
	manifest := map[string]any{
		"schema_version": 1,
		"build_input": map[string]any{
			"sandbox_agent_drivers": buildDrivers,
		},
		"drivers": entries,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}
	data = append(data, '\n')
	manifestPath := filepath.Join(rootfs, "etc", "harness-image", "agents.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		return "", fmt.Errorf("mkdir manifest parent: %w", err)
	}
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		return "", fmt.Errorf("write manifest: %w", err)
	}
	return manifestPath, nil
}

func TestDriverManifestHelpersFailClosedForUnknownDriver(t *testing.T) {
	unknown := agents.ID("opencode")
	if _, err := expectedDriverBinaryPath(unknown); err == nil || !strings.Contains(err.Error(), `unsupported driver "opencode"`) {
		t.Fatalf("expected unknown driver binary path error, got %v", err)
	}
}

func validServerGenerationPlanPayload() map[string]any {
	driver, _ := agents.DriverSpecFor("claude_code")
	provider, _ := agents.RuntimeProviderSpecFor("local_runsc")
	featurePolicy, _ := agents.FeaturePolicyPayload(agents.DefaultFeaturePolicyForDriver(driver))
	featurePolicyPayload := map[string]any{}
	for key, value := range featurePolicy {
		featurePolicyPayload[key] = value
	}
	featurePolicyPayload["capability_schema_version"] = agents.DriverCapabilitySchemaVersion
	featurePolicyPayload["capability_vocab_version"] = provider.CapabilityVocabulary
	featurePolicyPayload["driver_capabilities"] = agents.DriverCapabilityPayload(driver)
	featurePolicyPayload["runtime_provider_capabilities"] = agents.RuntimeProviderCapabilityPayload(provider)
	featurePolicyPayload["legacy_supports_interrupt"] = driver.SupportsInterrupt
	featurePolicyPayload["legacy_supports_compaction"] = driver.SupportsCompaction
	featurePolicyPayload["unsupported_features_fail"] = true
	featurePolicyPayload["credential_bearing_mcp_scope"] = "out_of_scope"
	adapterInputDigests := serverAdapterInputDigestPayloadForTest(serverFrozenEvidenceSandboxContractPayloadForTest(
		"sess_frozen_evidence",
		"gen_frozen_evidence",
		"contract_gen_frozen_evidence",
		"claude_code",
		"sha256:driver-state",
	))
	return map[string]any{
		"plan_version": store.GenerationPlanVersion,
		"identity":     map[string]any{"session_id": "sess_frozen_evidence", "generation_id": "gen_frozen_evidence", "product_mode": "agent"},
		"driver": map[string]any{
			"driver_id":               "claude_code",
			"driver_kind":             string(driver.Kind),
			"bridge_protocol":         driver.BridgeProtocol,
			"bridge_protocol_version": driver.BridgeProtocolVersion,
			"turn_input_schema":       driver.TurnInputSchema,
			"output_schema":           driver.OutputSchema,
			"output_format":           driver.OutputFormat,
			"model":                   "claude-test",
			"initial_state_digest":    "sha256:driver-state",
			"initial_state_version":   1,
			"capability_snapshot":     agents.DriverCapabilityPayload(driver),
		},
		"runtime_provider": map[string]any{
			"provider_id":                  provider.ID,
			"provider_config_id":           "local_runsc",
			"provider_profile_id":          provider.ProviderProfileID,
			"isolation_kind":               provider.IsolationKind,
			"template_ref":                 provider.TemplateRef,
			"capability_vocab_version":     provider.CapabilityVocabulary,
			"capability_digest":            agents.CapabilityDigest(provider),
			"capability_snapshot":          agents.RuntimeProviderCapabilityPayload(provider),
			"snapshot_policy":              provider.SnapshotPolicy,
			"agent_runtime_profile_id":     "arp_gen_frozen_evidence",
			"runtime_profile_provider_ref": "systrap",
		},
		"runsc_pin":    map[string]any{"platform": "systrap", "version": "runsc test", "binary_path": "/usr/local/bin/runsc-test", "binary_digest": "sha256:runsc"},
		"image":        map[string]any{"agent_manifest_digest": "sha256:agent-manifest", "rootfs_path": "/var/lib/harness/rootfs", "rootfs_image_digest": nil},
		"bridge_probe": map[string]any{"bridge_mode": "claim-loop"},
		"network": map[string]any{
			"network_profile_id": "net_gen_frozen_evidence", "runsc_network": "sandbox", "runsc_overlay2": "none",
			"sandbox_ip": "10.240.0.2", "sandbox_ip_cidr": "10.240.0.2/30", "host_gateway_ip": "10.240.0.1",
			"sandbox_base_url": "http://10.240.0.1:8080", "host_proxy_bind_url": "http://127.0.0.1:8080",
			"netns_name": "harness-gen-frozen", "netns_path": "/var/run/netns/harness-gen-frozen",
			"host_veth": "vh-frozen", "sandbox_veth": "vs-frozen", "host_side_cidr": "10.240.0.1/30",
			"nft_table_name": "harness-gen-frozen", "egress_policy_id": "egress_frozen",
			"egress_policy_digest": "egress_digest", "dns_policy": "off",
		},
		"data_volumes": map[string]any{
			"workspace":  serverPlanVolumePayload("/var/lib/harness/sessions/sess_frozen_evidence", "/var/lib/harness/evidence/workspaces/sess_frozen_evidence.json", "/workspace"),
			"agent_home": serverPlanVolumePayload("/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code", "/var/lib/harness/evidence/driver-homes/sess_frozen_evidence/claude_code.json", "/agent-home"),
		},
		"mounts": map[string]any{
			"workspace":                      map[string]any{"source": "/var/lib/harness/sessions/sess_frozen_evidence", "destination": "/workspace", "mode": "rw"},
			"agent_home":                     map[string]any{"source": "/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code", "destination": "/agent-home", "mode": "rw"},
			"control":                        map[string]any{"source": "/var/lib/harness/run/control/gen_frozen_evidence", "destination": "/harness-control", "mode": "ro"},
			"bridge":                         map[string]any{"source": "/var/lib/harness/run/bridge/gen_frozen_evidence", "destination": "/harness-control/bridge", "mode": "rw"},
			"network_hosts_path":             nil,
			"driver_config_materializations": nil,
		},
		"runtime_artifacts": map[string]any{
			"control_dir_path": "/var/lib/harness/run/control/gen_frozen_evidence", "control_manifest_path": "/var/lib/harness/run/control/gen_frozen_evidence/session.json",
			"control_manifest_digest": "manifest_digest", "projected_control_manifest_digest": "projected_manifest_digest",
			"bundle_dir_path": "/var/lib/harness/run/runtime/gen_frozen_evidence", "bundle_digest": "bundle_digest",
			"runtime_config_digest": "runtime_config_digest", "spec_path": "/var/lib/harness/run/runtime/gen_frozen_evidence/config.json",
			"spec_digest": "spec_digest", "bridge_dir_path": "/var/lib/harness/run/bridge/gen_frozen_evidence",
			"log_dir_path": "/var/lib/harness/logs/gen_frozen_evidence", "network_hosts_path": nil,
			"materialized_driver_config": []map[string]any{}, "resource_identity_digest": "sha256:resource",
			"sandbox_contract_id": "contract_gen_frozen_evidence", "sandbox_contract_payload_digest": "sha256:sandbox-contract",
			"sandbox_contract_compatibility_shape": store.SandboxContractVersion,
		},
		"feature_policy":    featurePolicyPayload,
		"content_snapshots": map[string]any{"skills": nil, "managed_settings": nil},
		"source_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config-source",
			"agent_manifest_digest": "sha256:agent-manifest",
			"adapter_input_digests": adapterInputDigests,
		},
		"mutable_state_scope": map[string]any{"leases": "runtime_generations", "events": "events", "checkpoint_state": "runtime_generations"},
	}
}

func serverFrozenEvidenceSandboxContractPayloadForTest(sessionID, generationID, contractID, driverID, driverStateDigest string) map[string]any {
	modelAccessAllowed := driverID == "claude_code"
	return map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"contract_schema_version":  store.SandboxContractSchemaVersion,
		"contract_gate_version":    store.SandboxContractGateDriverManifest,
		"contract_id":              contractID,
		"session_id":               sessionID,
		"generation_id":            generationID,
		"runtime_profile_id":       "arp_gen_frozen_evidence",
		"network_profile_id":       "net_gen_frozen_evidence",
		"driver": map[string]any{
			"driver_id":                            driverID,
			"driver_version":                       "test",
			"bridge_protocol":                      "harness_bridge_v2",
			"bridge_protocol_version":              2,
			"turn_input_schema":                    "RunTurn",
			"output_schema":                        "claude_stream_json_v1",
			"command_argv_digest":                  "sha256:command",
			"driver_config_digest":                 "sha256:driver-config",
			"required_runtime_capabilities_digest": "sha256:driver-capabilities",
			"supports_interrupt":                   false,
			"supports_compaction":                  true,
		},
		"runtime_provider": map[string]any{
			"provider_id":              "local_runsc",
			"provider_profile_id":      "local_runsc_default",
			"isolation_kind":           "gvisor",
			"template_ref":             "default",
			"template_digest":          "sha256:template",
			"capability_vocab_version": "1",
			"capability_digest":        "sha256:provider-capabilities",
		},
		"identity": map[string]any{
			"model_access_allowed": modelAccessAllowed,
		},
		"network_identity": map[string]any{
			"runsc_network": "sandbox",
			"sandbox_ip":    "10.240.0.2",
		},
		"credential_policy": serverCredentialPolicyPayloadForTest(driverID),
		"model_access": map[string]any{
			"model_access_allowed":         modelAccessAllowed,
			"sandbox_model_proxy_base_url": "http://harness-model-proxy.internal:8082",
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    map[string]any{},
			"initial_driver_state_digest":   driverStateDigest,
		},
		"runtime_adapter": map[string]any{
			"kind":                "runsc",
			"runsc_platform":      "systrap",
			"runsc_version":       "runsc test",
			"runsc_binary_path":   "/usr/local/bin/runsc-test",
			"runsc_binary_digest": "sha256:runsc",
			"runsc_container_id":  "runsc-gen-frozen",
			"runsc_network":       "sandbox",
			"runsc_overlay2":      "none",
		},
		"input_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config",
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": "sha256:agent-manifest",
		},
	}
}

func serverAdapterInputDigestPayloadForTest(contractPayload map[string]any) map[string]any {
	digests, err := generationplan.AdapterInputDigestsFromSandboxContract(contractPayload)
	if err != nil {
		panic(err)
	}
	return map[string]any{
		"driver_adapter":  digests["driver_adapter"],
		"runtime_adapter": digests["runtime_adapter"],
	}
}

func storeServerFrozenEvidenceCanonicalPayload(t *testing.T) []byte {
	t.Helper()
	canonical, err := serverFrozenEvidenceCanonicalPayload()
	if err != nil {
		t.Fatalf("canonical frozen evidence payload: %v", err)
	}
	return canonical
}

func serverFrozenEvidenceCanonicalPayload() ([]byte, error) {
	return store.CanonicalGenerationPlanPayload(validServerGenerationPlanPayload())
}

func mustServerFrozenEvidenceCanonicalPayload() []byte {
	canonical, err := serverFrozenEvidenceCanonicalPayload()
	if err != nil {
		panic(err)
	}
	return canonical
}

func storeServerFrozenEvidencePlan(t *testing.T, ctx context.Context, st *store.Store, dir string, payload map[string]any) store.GenerationPlanRecord {
	t.Helper()
	session := createServerTestSession(t, ctx, st, dir, "sess_frozen_evidence", string(sessionstate.Created), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'allocating', 'owner', ?)`, "gen_frozen_evidence", session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: "gen_frozen_evidence",
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("store generation plan: %v", err)
	}
	for kind, digest := range map[string]string{
		"sandbox_contract":           "sha256:sandbox-contract",
		"control_manifest":           "sha256:control-manifest",
		"control_manifest_projected": "sha256:control-manifest-projected",
		"oci_spec":                   "sha256:oci-spec",
		"bundle":                     "sha256:bundle",
		"runtime_config":             "sha256:runtime-config",
	} {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      "gen_frozen_evidence",
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    kind,
			ProjectionVersion: 1,
			PayloadDigest:     digest,
		}); err != nil {
			t.Fatalf("store projection %s: %v", kind, err)
		}
	}
	return plan
}

type serverGenerationPlanSourceDigestsForTest struct {
	RuntimeConfigDigest string
	AgentManifestDigest string
}

func storeServerSyntheticSandboxContractParentForPlan(t *testing.T, ctx context.Context, st *store.Store, plan store.GenerationPlanRecord) {
	t.Helper()
	sessionID := serverGenerationPlanSessionID(t, plan.CanonicalPayload)
	contractID := sandboxContractID(plan.GenerationID)
	canonicalPayload, err := store.CanonicalSandboxContractPayload(serverFrozenEvidenceSandboxContractPayloadForTest(
		sessionID,
		plan.GenerationID,
		contractID,
		"claude_code",
		"sha256:driver-state",
	))
	if err != nil {
		t.Fatalf("canonical synthetic sandbox contract parent: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO sandbox_contracts (
  contract_id, generation_id, session_id, sandbox_contract_version,
  contract_schema_version, contract_gate_version, canonical_payload,
  sandbox_contract_digest, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		contractID, plan.GenerationID, sessionID, store.SandboxContractVersion,
		store.SandboxContractSchemaVersion, store.SandboxContractGateDriverManifest,
		string(canonicalPayload), store.SandboxContractDigest(canonicalPayload),
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("store synthetic sandbox contract parent: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET sandbox_contract_id = ?,
    sandbox_contract_version = ?
WHERE generation_id = ?
  AND session_id = ?`, contractID, store.SandboxContractVersion, plan.GenerationID, sessionID); err != nil {
		t.Fatalf("store synthetic sandbox contract generation mirror: %v", err)
	}
}

func storeServerSandboxContractInputEvidenceFromGenerationPlanIfPresent(t *testing.T, ctx context.Context, st *store.Store, generationID string) {
	t.Helper()
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		return
	}
	if err != nil {
		t.Fatalf("get generation plan for input evidence: %v", err)
	}
	storeServerSandboxContractInputEvidenceFromPlan(t, ctx, st, plan)
}

func storeServerSandboxContractInputEvidenceFromPlan(t *testing.T, ctx context.Context, st *store.Store, plan store.GenerationPlanRecord) {
	t.Helper()
	digests := serverGenerationPlanSourceDigests(t, plan.CanonicalPayload)
	contractID := sandboxContractID(plan.GenerationID)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO sandbox_contract_input_evidence (
  contract_id, runtime_config_digest, runtime_config_preimage,
  agent_manifest_digest, agent_manifest_payload, created_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		contractID, digests.RuntimeConfigDigest, "{}",
		digests.AgentManifestDigest, "{}", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("store sandbox contract input evidence: %v", err)
	}
	evidence, err := st.GetSandboxContractInputEvidence(ctx, contractID)
	if err != nil {
		t.Fatalf("get sandbox contract input evidence: %v", err)
	}
	if evidence.RuntimeConfigDigest != digests.RuntimeConfigDigest ||
		evidence.AgentManifestDigest != digests.AgentManifestDigest {
		t.Fatalf("sandbox contract input evidence mismatch: evidence=%+v want=%+v", evidence, digests)
	}
}

func serverGenerationPlanSessionID(t *testing.T, canonicalPayload []byte) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(canonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan payload: %v", err)
	}
	identity, ok := payload["identity"].(map[string]any)
	if !ok {
		t.Fatalf("generation plan missing identity: %s", canonicalPayload)
	}
	sessionID, _ := identity["session_id"].(string)
	if strings.TrimSpace(sessionID) == "" {
		t.Fatalf("generation plan missing identity.session_id: %s", canonicalPayload)
	}
	return sessionID
}

func serverGenerationPlanSourceDigests(t *testing.T, canonicalPayload []byte) serverGenerationPlanSourceDigestsForTest {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(canonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan payload: %v", err)
	}
	sourceDigests, ok := payload["source_digests"].(map[string]any)
	if !ok {
		t.Fatalf("generation plan missing source_digests: %s", canonicalPayload)
	}
	digests := serverGenerationPlanSourceDigestsForTest{}
	digests.RuntimeConfigDigest, _ = sourceDigests["runtime_config_digest"].(string)
	digests.AgentManifestDigest, _ = sourceDigests["agent_manifest_digest"].(string)
	if strings.TrimSpace(digests.RuntimeConfigDigest) == "" ||
		strings.TrimSpace(digests.AgentManifestDigest) == "" {
		t.Fatalf("generation plan missing source digests: %s", canonicalPayload)
	}
	return digests
}

func storeServerGenerationPlanForArtifacts(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, artifacts runtime.GenerationArtifacts) store.GenerationPlanRecord {
	t.Helper()
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		t.Fatalf("get generation details for plan %s: %v", generationID, err)
	}
	mode := "agent"
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COALESCE(mode, '')
FROM sessions
WHERE id = ?`, sessionID).Scan(&mode); err != nil {
		t.Fatalf("get session mode for plan %s: %v", generationID, err)
	}
	if strings.TrimSpace(mode) == "" {
		mode = "agent"
	}
	driverSpec, ok := agents.DriverSpecFor(details.DriverID)
	if !ok {
		t.Fatalf("driver spec missing for %s", details.DriverID)
	}
	providerSpec, ok := agents.RuntimeProviderSpecFor("local_runsc")
	if !ok {
		t.Fatalf("provider spec missing")
	}
	featurePolicy, err := agents.FeaturePolicyPayload(agents.DefaultFeaturePolicyForDriver(driverSpec))
	if err != nil {
		t.Fatalf("feature policy for plan %s: %v", generationID, err)
	}
	featurePolicyPayload := map[string]any{}
	for key, value := range featurePolicy {
		featurePolicyPayload[key] = value
	}
	featurePolicyPayload["capability_schema_version"] = agents.DriverCapabilitySchemaVersion
	featurePolicyPayload["capability_vocab_version"] = providerSpec.CapabilityVocabulary
	featurePolicyPayload["driver_capabilities"] = agents.DriverCapabilityPayload(driverSpec)
	featurePolicyPayload["runtime_provider_capabilities"] = agents.RuntimeProviderCapabilityPayload(providerSpec)
	featurePolicyPayload["legacy_supports_interrupt"] = driverSpec.SupportsInterrupt
	featurePolicyPayload["legacy_supports_compaction"] = driverSpec.SupportsCompaction
	featurePolicyPayload["unsupported_features_fail"] = true
	featurePolicyPayload["credential_bearing_mcp_scope"] = "out_of_scope"
	payload := validServerGenerationPlanPayload()
	payload["identity"] = map[string]any{"session_id": sessionID, "generation_id": generationID, "product_mode": mode}
	workspaceVolume, driverHomeVolume := provisionServerGenerationPlanFixtureVolumes(t, ctx, st, sessionID, details)
	driverPlan := payload["driver"].(map[string]any)
	driverPlan["driver_id"] = string(driverSpec.ID)
	driverPlan["driver_kind"] = string(driverSpec.Kind)
	driverPlan["bridge_protocol"] = driverSpec.BridgeProtocol
	driverPlan["bridge_protocol_version"] = driverSpec.BridgeProtocolVersion
	driverPlan["turn_input_schema"] = driverSpec.TurnInputSchema
	driverPlan["output_schema"] = driverSpec.OutputSchema
	driverPlan["output_format"] = details.OutputFormat
	if strings.TrimSpace(details.Model) == "" {
		driverPlan["model"] = nil
	} else {
		driverPlan["model"] = details.Model
	}
	driverPlan["initial_state_digest"] = details.DriverStateDigest
	driverPlan["initial_state_version"] = details.DriverStateVersion
	driverPlan["capability_snapshot"] = agents.DriverCapabilityPayload(driverSpec)
	runtimeProvider := payload["runtime_provider"].(map[string]any)
	runtimeProvider["agent_runtime_profile_id"] = details.AgentRuntimeProfileID
	runtimeProvider["runtime_profile_provider_ref"] = details.RunscPlatform
	networkPlan := payload["network"].(map[string]any)
	networkPlan["network_profile_id"] = details.NetworkProfileID
	networkPlan["runsc_network"] = details.RunscNetwork
	networkPlan["runsc_overlay2"] = details.RunscOverlay2
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("render sandbox ip for plan %s: %v", generationID, err)
	}
	networkPlan["sandbox_ip"] = sandboxIP
	networkPlan["sandbox_ip_cidr"] = details.SandboxIPCIDR
	networkPlan["host_gateway_ip"] = details.HostGatewayIP
	networkPlan["sandbox_base_url"] = details.SandboxBaseURL
	networkPlan["host_proxy_bind_url"] = details.HostProxyBindURL
	networkPlan["proxy_port"] = details.ProxyPort
	networkPlan["netns_name"] = details.NetnsName
	networkPlan["netns_path"] = details.NetnsPath
	networkPlan["host_veth"] = details.HostVeth
	networkPlan["sandbox_veth"] = details.SandboxVeth
	networkPlan["host_side_cidr"] = details.HostSideCIDR
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		t.Fatalf("render nft table for plan %s: %v", generationID, err)
	}
	networkPlan["nft_table_name"] = nftTableName
	networkPlan["egress_policy_id"] = details.EgressPolicyID
	networkPlan["egress_policy_digest"] = details.EgressPolicyDigest
	networkPlan["dns_policy"] = details.DNSPolicy
	runscPin := payload["runsc_pin"].(map[string]any)
	runscPin["version"] = artifacts.RunscVersion
	runscPin["binary_path"] = artifacts.RunscBinaryPath
	runscPin["binary_digest"] = artifacts.RunscBinaryDigest
	runtimeArtifacts := payload["runtime_artifacts"].(map[string]any)
	runtimeArtifacts["control_manifest_digest"] = artifacts.ManifestDigest
	runtimeArtifacts["projected_control_manifest_digest"] = artifacts.ProjectedManifestDigest
	runtimeArtifacts["control_dir_path"] = details.ControlDirPath
	runtimeArtifacts["control_manifest_path"] = details.ControlManifestPath
	runtimeArtifacts["bundle_dir_path"] = details.BundleDirPath
	runtimeArtifacts["bundle_digest"] = artifacts.BundleDigest
	runtimeArtifacts["runtime_config_digest"] = artifacts.RuntimeConfigDigest
	runtimeArtifacts["spec_path"] = details.SpecPath
	runtimeArtifacts["spec_digest"] = artifacts.SpecDigest
	runtimeArtifacts["bridge_dir_path"] = details.BridgeDirPath
	runtimeArtifacts["log_dir_path"] = details.LogDirPath
	if strings.TrimSpace(details.NetworkHostsPath) == "" {
		runtimeArtifacts["network_hosts_path"] = nil
	} else {
		runtimeArtifacts["network_hosts_path"] = details.NetworkHostsPath
	}
	allocation := serverGenerationAllocationForTest(t, ctx, st, sessionID, generationID)
	sandboxContractPayload := serverRuntimeResourceSandboxContractPayloadForTest(t, details, allocation, sandboxContractID(generationID))
	sandboxContractDigest := serverSandboxContractPayloadDigestForTest(t, sandboxContractPayload)
	runtimeArtifacts["sandbox_contract_id"] = sandboxContractID(generationID)
	runtimeArtifacts["sandbox_contract_payload_digest"] = sandboxContractDigest
	runtimeArtifacts["resource_identity_digest"] = serverRuntimeResourceIdentityDigestForPlanFixture(t, details, artifacts)
	sourceDigests := payload["source_digests"].(map[string]any)
	sourceDigests["adapter_input_digests"] = serverAdapterInputDigestPayloadForTest(sandboxContractPayload)
	dataVolumes := payload["data_volumes"].(map[string]any)
	serverApplyWorkspaceVolumePayload(dataVolumes["workspace"].(map[string]any), workspaceVolume)
	serverApplyDriverHomeVolumePayload(dataVolumes["agent_home"].(map[string]any), driverHomeVolume)
	mounts := payload["mounts"].(map[string]any)
	mounts["workspace"].(map[string]any)["source"] = workspaceVolume.HostPath
	mounts["agent_home"].(map[string]any)["source"] = driverHomeVolume.HostPath
	mounts["control"].(map[string]any)["source"] = details.ControlDirPath
	mounts["bridge"].(map[string]any)["source"] = details.BridgeDirPath
	if strings.TrimSpace(details.NetworkHostsPath) == "" {
		mounts["network_hosts_path"] = nil
	} else {
		mounts["network_hosts_path"] = details.NetworkHostsPath
	}
	payload["feature_policy"] = featurePolicyPayload
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: generationID,
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("store generation plan for %s: %v", generationID, err)
	}
	projections := append([]store.GenerationPlanProjectionExpectation{{
		ProjectionKind:    store.GenerationPlanProjectionSandboxContract,
		ProjectionVersion: store.GenerationPlanProjectionVersion,
		PayloadDigest:     sandboxContractDigest,
	}}, planprojection.ExpectationsForDetails(details, artifacts)...)
	for _, projection := range projections {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    projection.ProjectionKind,
			ProjectionVersion: projection.ProjectionVersion,
			PayloadDigest:     projection.PayloadDigest,
			MaterializedPath:  projection.MaterializedPath,
		}); err != nil {
			t.Fatalf("store generation plan projection %s for %s: %v", projection.ProjectionKind, generationID, err)
		}
	}
	return plan
}

func serverRuntimeResourceIdentityDigestForPlanFixture(t *testing.T, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts) string {
	t.Helper()
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("runtime resource sandbox ip for plan %s: %v", details.GenerationID, err)
	}
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		t.Fatalf("runtime resource nft table for plan %s: %v", details.GenerationID, err)
	}
	params := store.RuntimeResourceInstanceParams{
		GenerationID:           details.GenerationID,
		SessionID:              details.SessionID,
		ContractID:             sandboxContractID(details.GenerationID),
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 mustRuntimeResourceHostID(t),
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          details.RunscPlatform,
		RunscVersion:           artifacts.RunscVersion,
		RunscBinaryPath:        artifacts.RunscBinaryPath,
		RunscBinaryDigest:      artifacts.RunscBinaryDigest,
		NetworkProfileID:       details.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              sandboxIP,
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           nftTableName,
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		NetworkHostsPath:       details.NetworkHostsPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
	}
	_, digest, err := store.RuntimeResourceIdentityForParams(params)
	if err != nil {
		t.Fatalf("runtime resource identity for plan %s: %v", details.GenerationID, err)
	}
	return digest
}

func provisionServerGenerationPlanFixtureVolumes(t *testing.T, ctx context.Context, st *store.Store, sessionID string, details store.RuntimeGenerationDetails) (store.SessionWorkspaceVolume, store.SessionDriverHomeVolume) {
	t.Helper()
	cfg := serverGenerationPlanFixtureVolumeConfig(t, details)
	now := time.Now().UTC()
	workspace, err := st.ProvisionSessionWorkspace(ctx, store.ProvisionSessionWorkspaceParams{
		SessionID: sessionID,
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision workspace volume for plan %s: %v", details.GenerationID, err)
	}
	driverHome, err := st.ProvisionSessionDriverHome(ctx, store.ProvisionSessionDriverHomeParams{
		SessionID: sessionID,
		Driver:    details.DriverID,
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision driver-home volume for plan %s: %v", details.GenerationID, err)
	}
	return workspace, driverHome
}

func serverGenerationPlanFixtureVolumeConfig(t *testing.T, details store.RuntimeGenerationDetails) store.DataVolumeProvisionerConfig {
	t.Helper()
	controlDir := strings.TrimSpace(details.ControlDirPath)
	if controlDir == "" {
		t.Fatalf("generation %s control dir path is required for plan fixture volumes", details.GenerationID)
	}
	runDir := filepath.Dir(filepath.Dir(controlDir))
	fixtureRoot := filepath.Dir(runDir)
	if runDir == "." || fixtureRoot == "." {
		t.Fatalf("generation %s control dir path %q cannot derive plan fixture roots", details.GenerationID, controlDir)
	}
	return store.DataVolumeProvisionerConfig{
		SessionsRoot:   filepath.Join(fixtureRoot, "sessions"),
		AgentHomesRoot: filepath.Join(fixtureRoot, "agent-homes"),
		EvidenceRoot:   filepath.Join(fixtureRoot, "state", "volume-evidence"),
		LayoutVersion:  store.DataVolumeLayoutVersion,
		RuntimeIdentity: store.RuntimeIdentity{
			UID: serverTestSandboxUID(),
			GID: serverTestSandboxGID(),
		},
	}
}

func serverApplyWorkspaceVolumePayload(payload map[string]any, volume store.SessionWorkspaceVolume) {
	payload["session_id"] = volume.SessionID
	payload["host_path"] = volume.HostPath
	payload["layout_version"] = volume.LayoutVersion
	payload["runtime_identity_digest"] = volume.RuntimeIdentityDigest
	payload["provisioning_marker_path"] = volume.ProvisioningMarkerPath
	payload["provisioning_marker_digest"] = volume.ProvisioningMarkerDigest
	payload["sandbox_uid"] = volume.SandboxUID
	payload["sandbox_gid"] = volume.SandboxGID
	payload["sandbox_supplemental_gids"] = append([]int(nil), volume.SandboxSupplementalGIDs...)
}

func serverApplyDriverHomeVolumePayload(payload map[string]any, volume store.SessionDriverHomeVolume) {
	payload["session_id"] = volume.SessionID
	payload["driver"] = volume.Driver
	payload["host_path"] = volume.HostPath
	payload["layout_version"] = volume.LayoutVersion
	payload["runtime_identity_digest"] = volume.RuntimeIdentityDigest
	payload["provisioning_marker_path"] = volume.ProvisioningMarkerPath
	payload["provisioning_marker_digest"] = volume.ProvisioningMarkerDigest
	payload["sandbox_uid"] = volume.SandboxUID
	payload["sandbox_gid"] = volume.SandboxGID
	payload["sandbox_supplemental_gids"] = append([]int(nil), volume.SandboxSupplementalGIDs...)
}

func serverPlanVolumePayload(hostPath, markerPath, destination string) map[string]any {
	return map[string]any{
		"session_id": "sess_frozen_evidence", "host_path": hostPath, "layout_version": 1,
		"runtime_identity_digest": "sha256:identity", "provisioning_marker_path": markerPath,
		"provisioning_marker_digest": "sha256:marker", "sandbox_destination": destination,
		"sandbox_uid": 65534, "sandbox_gid": 65534, "sandbox_supplemental_gids": []int{},
	}
}

func serverGenerationPlanFrozenEvidenceDetails() store.RuntimeGenerationDetails {
	return store.RuntimeGenerationDetails{
		GenerationID:                    "gen_frozen_evidence",
		SessionID:                       "sess_frozen_evidence",
		RunscPlatform:                   "systrap",
		CheckpointBundleDigest:          "sha256:bundle",
		CheckpointRuntimeConfigDigest:   "sha256:runtime-config",
		CheckpointControlManifestDigest: "sha256:control-manifest-projected",
		CheckpointDriverStatesDigest:    "sha256:driver-state-fence",
		CheckpointPlanDigest:            store.GenerationPlanDigest(mustServerFrozenEvidenceCanonicalPayload()),
	}
}

func serverGenerationPlanFrozenEvidenceArtifacts() runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		ManifestDigest:          "sha256:control-manifest",
		ProjectedManifestDigest: "sha256:control-manifest-projected",
		BundleDigest:            "sha256:bundle",
		RuntimeConfigDigest:     "sha256:runtime-config",
		SpecDigest:              "sha256:oci-spec",
		RunscVersion:            "runsc test",
		RunscBinaryPath:         "/usr/local/bin/runsc-test",
		RunscBinaryDigest:       "sha256:runsc",
	}
}

func createServerRuntimeResourceLive(t *testing.T, ctx context.Context, st *store.Store, sessionID string, allocation store.GenerationAllocation, ownerUUID, hostID string, now time.Time) store.RuntimeResourceInstance {
	t.Helper()
	contractID := sandboxContractID(allocation.GenerationID)
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	prefix, err := netip.ParsePrefix(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("parse sandbox cidr: %v", err)
	}
	if _, err := st.StoreSandboxContract(ctx, store.StoreSandboxContractParams{
		ContractID:             contractID,
		SessionID:              sessionID,
		GenerationID:           allocation.GenerationID,
		SandboxContractVersion: store.SandboxContractVersion,
		ContractSchemaVersion:  store.SandboxContractSchemaVersion,
		ContractGateVersion:    store.SandboxContractGateDriverManifest,
		Payload:                serverRuntimeResourceSandboxContractPayloadForTest(t, details, allocation, contractID),
		Now:                    now,
	}); err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	storeServerSandboxContractInputEvidenceFromGenerationPlanIfPresent(t, ctx, st, allocation.GenerationID)
	artifacts := testGenerationArtifacts()
	if strings.TrimSpace(details.RunscVersion) != "" {
		artifacts.RunscVersion = details.RunscVersion
	}
	if strings.TrimSpace(details.RunscBinaryPath) != "" {
		artifacts.RunscBinaryPath = details.RunscBinaryPath
	}
	if strings.TrimSpace(details.RunscBinaryDigest) != "" {
		artifacts.RunscBinaryDigest = details.RunscBinaryDigest
	}
	instance, err := st.CreateRuntimeResourceInstance(ctx, store.RuntimeResourceInstanceParams{
		GenerationID:           allocation.GenerationID,
		SessionID:              sessionID,
		ContractID:             contractID,
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 hostID,
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          "systrap",
		RunscVersion:           artifacts.RunscVersion,
		RunscBinaryPath:        artifacts.RunscBinaryPath,
		RunscBinaryDigest:      artifacts.RunscBinaryDigest,
		NetworkProfileID:       allocation.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              prefix.Addr().String(),
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           mustRuntimeResourceNftTableName(t, allocation.GenerationID),
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		NetworkHostsPath:       details.NetworkHostsPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("create runtime resource instance: %v", err)
	}
	workerID := strings.TrimSpace(ownerUUID)
	if workerID == "" {
		workerID = strings.TrimSuffix(strings.TrimSpace(allocation.Owner), ":"+store.RuntimeManagerRoleTag)
	}
	if err := st.ClaimRuntimeResourceMaterialization(ctx, store.RuntimeResourceMaterializationClaimParams{
		GenerationID:     allocation.GenerationID,
		WorkerID:         workerID,
		HostID:           hostID,
		LeaseExpiresAt:   now.Add(time.Minute),
		IdempotencyToken: "test:" + allocation.GenerationID,
		Now:              now.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("claim runtime resource materialization: %v", err)
	}
	if err := st.MarkRuntimeResourceReady(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		Now:          now.Add(2 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource ready: %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		PostStart:    serverPostStartProofForTest(instance),
		Now:          now.Add(3 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource live: %v", err)
	}
	return instance
}

func serverRuntimeResourceSandboxContractPayloadForTest(t *testing.T, details store.RuntimeGenerationDetails, allocation store.GenerationAllocation, contractID string) map[string]any {
	t.Helper()
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("sandbox contract sandbox ip for %s: %v", allocation.GenerationID, err)
	}
	driverID := allocation.DriverState.DriverID
	credentialPolicy := serverCredentialPolicyForTest(t, driverID)
	modelAccessAllowed := driverID == "claude_code"
	return map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"contract_schema_version":  store.SandboxContractSchemaVersion,
		"contract_gate_version":    store.SandboxContractGateDriverManifest,
		"contract_id":              contractID,
		"session_id":               details.SessionID,
		"generation_id":            allocation.GenerationID,
		"runtime_profile_id":       allocation.AgentRuntimeProfileID,
		"network_profile_id":       allocation.NetworkProfileID,
		"driver": map[string]any{
			"driver_id":                            driverID,
			"driver_version":                       "test",
			"bridge_protocol":                      "harness_bridge_v2",
			"bridge_protocol_version":              2,
			"turn_input_schema":                    "RunTurn",
			"output_schema":                        "claude_stream_json_v1",
			"command_argv_digest":                  "sha256:command",
			"driver_config_digest":                 "sha256:driver-config",
			"required_runtime_capabilities_digest": "sha256:driver-capabilities",
			"supports_interrupt":                   false,
			"supports_compaction":                  true,
		},
		"runtime_provider": map[string]any{
			"provider_id":              "local_runsc",
			"provider_profile_id":      "local_runsc_default",
			"isolation_kind":           "gvisor",
			"template_ref":             "default",
			"template_digest":          "sha256:template",
			"capability_vocab_version": "1",
			"capability_digest":        "sha256:provider-capabilities",
		},
		"identity": map[string]any{
			"model_access_allowed": modelAccessAllowed,
		},
		"network_identity": map[string]any{
			"runsc_network": details.RunscNetwork,
			"sandbox_ip":    sandboxIP,
		},
		"credential_policy": credentialPolicy,
		"model_access": map[string]any{
			"model_access_allowed": modelAccessAllowed,
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    map[string]any{},
			"initial_driver_state_digest":   allocation.DriverState.StateDigest,
		},
		"runtime_adapter": map[string]any{
			"kind":                "runsc",
			"runsc_platform":      details.RunscPlatform,
			"runsc_version":       details.RunscVersion,
			"runsc_binary_path":   details.RunscBinaryPath,
			"runsc_binary_digest": details.RunscBinaryDigest,
			"runsc_container_id":  details.RunscContainerID,
			"runsc_network":       details.RunscNetwork,
			"runsc_overlay2":      details.RunscOverlay2,
		},
		"input_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config",
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": "sha256:agent-manifest",
		},
	}
}

func serverSandboxContractPayloadDigestForTest(t *testing.T, payload map[string]any) string {
	t.Helper()
	canonical, err := store.CanonicalSandboxContractPayload(payload)
	if err != nil {
		t.Fatalf("canonical sandbox contract payload: %v", err)
	}
	return store.SandboxContractDigest(canonical)
}

func serverCredentialPolicyForTest(t *testing.T, driverID string) map[string]any {
	t.Helper()
	return serverCredentialPolicyPayloadForTest(driverID)
}

func serverCredentialPolicyPayloadForTest(driverID string) map[string]any {
	secretGrants := []map[string]any{}
	if driverID == "claude_code" {
		secretGrants = append(secretGrants, map[string]any{
			"grant_id":                  "model_provider:anthropic_proxy",
			"domain":                    "model_provider",
			"scope":                     "anthropic_messages",
			"exposure_mode":             "proxy_only",
			"ttl_seconds":               nil,
			"allowed_drivers":           []string{driverID},
			"allowed_runtime_providers": []string{"local_runsc"},
		})
	}
	policy := map[string]any{
		"provider_credentials": "host-only",
		"sandbox_secret_mount": "absent",
		"proxy_token":          "absent",
		"secret_grants":        secretGrants,
	}
	digest, err := store.CredentialPolicyDigest(policy)
	if err != nil {
		panic(err)
	}
	policy["digest"] = digest
	return policy
}

func serverPostStartProofForTest(instance store.RuntimeResourceInstance) *store.RuntimeResourcePostStartProof {
	return &store.RuntimeResourcePostStartProof{
		HostID:                 instance.HostID,
		GenerationID:           instance.GenerationID,
		ContractID:             instance.ContractID,
		SandboxContractVersion: instance.SandboxContractVersion,
		RunscContainerID:       instance.RunscContainerID,
		RunscState:             "runsc_container:" + instance.RunscContainerID + ":running; check=test",
		RunscPlatform:          instance.RunscPlatform,
		RunscVersion:           instance.RunscVersion,
		RunscBinaryPath:        instance.RunscBinaryPath,
		RunscBinaryDigest:      instance.RunscBinaryDigest,
		IPNetns:                "netns:present; check=test",
		IPLink:                 "host_veth:present; check=test",
		NFT:                    "nft_table:present; check=test",
		BridgeStartup:          "bridge_startup_probe:passed; check=test",
	}
}

type serverCheckpointImageManifest struct {
	Version int                                 `json:"version"`
	Files   []serverCheckpointImageManifestFile `json:"files"`
}

type serverCheckpointImageManifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func writeServerCheckpointFilesWithoutManifest(t *testing.T, checkpointPath string) {
	t.Helper()
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		t.Fatalf("create checkpoint path: %v", err)
	}
	for _, name := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		if err := os.WriteFile(filepath.Join(checkpointPath, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write checkpoint file %s: %v", name, err)
		}
	}
}

func buildServerCheckpointImageManifest(checkpointPath string) (serverCheckpointImageManifest, error) {
	manifest := serverCheckpointImageManifest{Version: 1}
	for _, name := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		path := filepath.Join(checkpointPath, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return serverCheckpointImageManifest{}, err
		}
		sum := sha256.Sum256(data)
		manifest.Files = append(manifest.Files, serverCheckpointImageManifestFile{
			Path:   name,
			Size:   int64(len(data)),
			SHA256: fmt.Sprintf("%x", sum),
		})
	}
	return manifest, nil
}

func writeServerJSONFile(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func openServerOwnedStore(t *testing.T, ctx context.Context, dir string) (*store.Store, *store.OwnerLock) {
	t.Helper()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	owner, err := store.AcquireOwnerLock(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := st.WriteOwner(ctx, owner); err != nil {
		t.Fatalf("write owner: %v", err)
	}
	return st, owner
}

func createServerTestSession(t *testing.T, ctx context.Context, st *store.Store, dir, id, status string, now time.Time, expiresAt *time.Time) store.Session {
	t.Helper()
	session := store.Session{
		ID:        id,
		UserID:    labUserID,
		Status:    status,
		DriverID:  "claude_code",
		Mode:      store.ModeForDriver("claude_code"),
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: expiresAt,
	}
	if err := os.MkdirAll(filepath.Join(dir, "sessions", id), 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	return session
}

func createServerPlannedActiveGeneration(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, dir, sessionID string, driver agents.ID) (store.Session, store.GenerationAllocation) {
	t.Helper()
	now := time.Now().UTC()
	driverID := string(driver)
	session := store.Session{
		ID:        sessionID,
		UserID:    labUserID,
		Status:    string(sessionstate.Created),
		DriverID:  driverID,
		Mode:      store.ModeForDriver(driverID),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := os.MkdirAll(filepath.Join(dir, "sessions", sessionID), 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create planned active session: %v", err)
	}
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     store.GenerationLeaseOwner(ownerUUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, driverID),
	})
	if err != nil {
		t.Fatalf("allocate planned active generation: %v", err)
	}
	artifacts := testGenerationArtifacts()
	recordServerRuntimeArtifacts(t, ctx, st, allocation.GenerationID, artifacts.ManifestDigest, artifacts.RunscVersion)
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark planned active generation live: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET status = ?,
    updated_at = ?
WHERE id = ?`, string(sessionstate.RunningActive), now.Add(2*time.Second).Format(time.RFC3339Nano), sessionID); err != nil {
		t.Fatalf("mark planned active session running: %v", err)
	}
	session.Status = string(sessionstate.RunningActive)
	session.ActiveGenerationID = allocation.GenerationID
	session.UpdatedAt = now.Add(2 * time.Second)
	return session, allocation
}

func serverRunscContainerID(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) string {
	t.Helper()
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if strings.TrimSpace(details.RunscContainerID) == "" {
		t.Fatalf("generation %s has no runsc container id", generationID)
	}
	return details.RunscContainerID
}

func enableSessionAutoCheckpoint(t *testing.T, ctx context.Context, st *store.Store, sessionID string) {
	t.Helper()
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE sessions SET auto_checkpoint_enabled = 1 WHERE id = ?`, sessionID); err != nil {
		t.Fatalf("enable auto checkpoint: %v", err)
	}
}

func mustRuntimeResourceHostID(t *testing.T) string {
	t.Helper()
	hostID, err := runtimeResourceHostID()
	if err != nil {
		t.Fatalf("runtime resource host id: %v", err)
	}
	return hostID
}

func mustRuntimeResourceNftTableName(t *testing.T, generationID string) string {
	t.Helper()
	tableName, err := runtimeResourceNftTableName(generationID)
	if err != nil {
		t.Fatalf("runtime resource nft table name: %v", err)
	}
	return tableName
}

func prepareServerIdleGeneration(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, sessionID string) store.GenerationAllocation {
	t.Helper()
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     store.GenerationLeaseOwner(ownerUUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	artifacts := testGenerationArtifacts()
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          artifacts.ManifestDigest,
		ProjectedControlManifestDigest: artifacts.ProjectedManifestDigest,
		BundleDigest:                   artifacts.BundleDigest,
		RuntimeConfigDigest:            artifacts.RuntimeConfigDigest,
		SpecDigest:                     artifacts.SpecDigest,
		RunscVersion:                   artifacts.RunscVersion,
		RunscBinaryPath:                artifacts.RunscBinaryPath,
		RunscBinaryDigest:              artifacts.RunscBinaryDigest,
	}); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	storeServerGenerationPlanForArtifacts(t, ctx, st, sessionID, allocation.GenerationID, artifacts)
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, sessionID, allocation, ownerUUID, mustRuntimeResourceHostID(t), now.Add(2*time.Second))
	if err := st.UpdateSessionStatusAndActivity(ctx, sessionID, string(sessionstate.RunningIdle), nil, now.Add(-time.Minute)); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}
	return allocation
}

func markServerGenerationCheckpointed(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	ensureServerRuntimeResourceLiveForCheckpoint(t, ctx, st, sessionID, generationID, now.Add(-time.Millisecond))
	formattedNow := now.UTC().Format(time.RFC3339Nano)
	fence := serverCheckpointDriverStateFenceForTest(t, ctx, st, sessionID, generationID)
	checkpointPlanDigest := "sha256:plan"
	if plan, err := st.GetGenerationPlan(ctx, generationID); err == nil {
		checkpointPlanDigest = plan.PlanDigest
	} else if err != sql.ErrNoRows {
		t.Fatalf("get checkpoint generation plan: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = runsc_version,
    checkpoint_runsc_platform = runsc_platform,
    checkpoint_runsc_binary_path = (
      SELECT runsc_binary_path
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_runsc_binary_digest = (
      SELECT runsc_binary_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_bundle_digest = 'bundle_digest',
    checkpoint_runtime_config_digest = 'runtime_config_digest',
    checkpoint_control_manifest_digest = (
      SELECT control_manifest_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_driver_states_digest = ?,
    checkpoint_plan_digest = ?,
    checkpoint_image_manifest_digest = ?,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?`, formattedNow, fence, checkpointPlanDigest, checkpointImageManifestDigestForTest, formattedNow, generationID, sessionID); err != nil {
		t.Fatalf("set checkpointed generation: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?
  AND session_id = ?`, generationID, sessionID); err != nil {
		t.Fatalf("reserve checkpointed network: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reserved_checkpointed'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("reserve checkpointed resources: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'checkpoint_reserved',
    lease_expires_at = NULL,
    idempotency_token = NULL,
    updated_at = ?
WHERE generation_id = ?
  AND state IN ('live', 'checkpoint_reserved')`, formattedNow, generationID); err != nil {
		t.Fatalf("reserve checkpointed runtime resource: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, sessionID, string(sessionstate.Checkpointed), nil); err != nil {
		t.Fatalf("set checkpointed session: %v", err)
	}
}

func ensureServerRuntimeResourceLiveForCheckpoint(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	if _, err := st.GetRuntimeResourceInstance(ctx, generationID); err == nil {
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("get checkpoint runtime resource instance: %v", err)
	}
	allocation := serverGenerationAllocationForTest(t, ctx, st, sessionID, generationID)
	createServerRuntimeResourceLive(t, ctx, st, sessionID, allocation, "checkpoint-test-owner", mustRuntimeResourceHostID(t), now)
}

func serverGenerationAllocationForTest(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) store.GenerationAllocation {
	t.Helper()
	allocation := store.GenerationAllocation{GenerationID: generationID}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT network_profile_id, agent_runtime_profile_id, COALESCE(lease_owner, '')
FROM runtime_generations
WHERE session_id = ?
  AND generation_id = ?`, sessionID, generationID).Scan(
		&allocation.NetworkProfileID,
		&allocation.AgentRuntimeProfileID,
		&allocation.Owner,
	); err != nil {
		t.Fatalf("query generation allocation for checkpoint: %v", err)
	}
	if strings.TrimSpace(allocation.Owner) == "" {
		allocation.Owner = store.GenerationLeaseOwner("checkpoint-test-owner")
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT driver_id, state_digest, state_version
FROM session_driver_states
WHERE session_id = ?`, sessionID).Scan(
		&allocation.DriverState.DriverID,
		&allocation.DriverState.StateDigest,
		&allocation.DriverState.StateVersion,
	); err != nil {
		t.Fatalf("query driver state for checkpoint: %v", err)
	}
	return allocation
}

func serverCheckpointDriverStateFenceForTest(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) string {
	t.Helper()
	var driverID, stateDigest string
	var stateVersion int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT ds.driver_id, ds.state_digest, ds.state_version
FROM session_driver_states ds
JOIN runtime_generations g ON g.session_id = ds.session_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND ds.driver_id = a.driver_id`, sessionID, generationID).Scan(&driverID, &stateDigest, &stateVersion); err != nil {
		t.Fatalf("query driver state fence input: %v", err)
	}
	fence, err := store.CheckpointDriverStatesDigest(generationID, []store.DriverStateToken{{
		DriverID:     driverID,
		StateDigest:  stateDigest,
		StateVersion: stateVersion,
	}})
	if err != nil {
		t.Fatalf("compute driver state fence: %v", err)
	}
	return fence
}

func newServerTestWatcher(t *testing.T, sessionsRoot string, st *store.Store, hub *events.Hub) *artifacts.Watcher {
	t.Helper()
	return artifacts.New(store.DataVolumeProvisionerConfig{
		SessionsRoot:   sessionsRoot,
		AgentHomesRoot: filepath.Join(t.TempDir(), "agent-homes"),
		EvidenceRoot:   filepath.Join(t.TempDir(), "volume-evidence"),
		RuntimeIdentity: store.RuntimeIdentity{
			UID: serverTestSandboxUID(),
			GID: serverTestSandboxGID(),
		},
	}, st, hub, slog.Default())
}

func serverDataVolumeConfigForTest(cfg config.Config) (store.DataVolumeProvisionerConfig, error) {
	roots, err := config.ValidateIsolationRoots(cfg.IsolationRoots())
	if err != nil {
		return store.DataVolumeProvisionerConfig{}, err
	}
	identity := cfg.Harness.SandboxIdentity
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

func applyServerTestDeploymentConfig(cfg *config.Config) {
	enabled := true
	disableNonessentialTraffic := true
	cfg.Harness.DefaultAgent = cfg.DefaultAgent
	cfg.Harness.Agents = map[string]config.AgentConfig{
		"claude_code": {
			Enabled:                    &enabled,
			DriverID:                   "claude_code",
			ModelProfile:               "anthropic_default",
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: &disableNonessentialTraffic,
		},
		"pi": {
			Enabled:                    &enabled,
			DriverID:                   "pi",
			ModelProfile:               "anthropic_default",
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: &disableNonessentialTraffic,
		},
		"sh": {
			Enabled:         &enabled,
			DriverID:        "sh",
			RuntimeProvider: "local_runsc",
		},
	}
	cfg.Harness.ModelProfiles = map[string]config.ModelProfileConfig{
		"anthropic_default": {
			Enabled:  &enabled,
			Provider: "anthropic_messages",
			Model:    "sonnet",
			ProxyRef: config.DefaultModelProxyRef,
		},
	}
	cfg.Harness.RuntimeProviders = map[string]config.RuntimeProviderConfig{
		"local_runsc": {
			Enabled:    &enabled,
			ProviderID: "local_runsc",
			ProfileID:  "local_runsc_default",
		},
	}
}

func testServerConfig(dir string) config.Config {
	rootfs := filepath.Join(dir, "rootfs")
	mustWriteServerTestAgentImageManifest(rootfs, agents.ClaudeCode, agents.Pi, agents.Shell)
	cfg := config.Config{
		SessionsRoot:     filepath.Join(dir, "sessions"),
		AgentHomesRoot:   filepath.Join(dir, "agent-homes"),
		BundleRoot:       filepath.Join(dir, "bundle"),
		RootFSPath:       rootfs,
		DBPath:           filepath.Join(dir, "state", "orchestrator.db"),
		RepoRoot:         dir,
		SessionRetention: time.Hour,
		MaxSessions:      10,
		DefaultAgent:     "claude_code",
		ModelProxy: config.ModelProxyConfig{
			BindURL:        "http://0.0.0.0:8082",
			SandboxBaseURL: "http://harness-model-proxy.internal:8082",
			BindPort:       8082,
		},
		Harness: config.HarnessConfig{
			RunDir: filepath.Join(dir, "run"),
			ModelProxy: config.ModelProxyConfig{
				BindURL:        "http://0.0.0.0:8082",
				SandboxBaseURL: "http://harness-model-proxy.internal:8082",
				BindPort:       8082,
			},
			Network: config.NetworkConfig{
				CIDRPool: config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/29")},
				Egress: config.EgressConfig{
					DorisFEHosts: []string{"172.16.0.138"},
					DorisBEHosts: []string{"172.16.0.139"},
					DorisPorts:   []int{9030},
					DNSPolicy:    config.DNSPolicyHostnamesOnly,
				},
			},
			Bridge: config.BridgeConfig{
				LeaseTTL:          config.Duration{Duration: time.Minute},
				HeartbeatInterval: config.Duration{Duration: 10 * time.Millisecond},
				PollInterval:      config.Duration{Duration: 10 * time.Millisecond},
				AckStartedGrace:   config.Duration{Duration: 90 * time.Second},
				ReconnectGrace:    config.Duration{Duration: 30 * time.Second},
			},
			Events: config.EventsConfig{
				RetentionWindow:        config.Duration{Duration: time.Hour},
				RetentionRows:          1_000,
				EmitOutputBatchMaxRows: 64,
				EmitOutputBatchMaxAge:  config.Duration{Duration: 100 * time.Millisecond},
			},
			Reaper: config.ReaperConfig{
				FailedRetention: config.Duration{Duration: 0},
			},
			SandboxIdentity: config.SandboxIdentity{
				UID: serverTestSandboxUID(),
				GID: serverTestSandboxGID(),
			},
			ProxyServiceIdentity: config.ProxyServiceIdentity{
				UID: os.Geteuid(),
				GID: os.Getegid(),
			},
		},
	}
	applyServerTestDeploymentConfig(&cfg)
	return cfg
}

func serverTestSandboxUID() int {
	uid := os.Getuid()
	if uid > 0 {
		return uid
	}
	return 65534
}

func serverTestSandboxGID() int {
	gid := os.Getgid()
	if gid > 0 {
		return gid
	}
	return 65534
}

func TestResourceAllocatorConfigUsesHostOnlyClaudeCredentials(t *testing.T) {
	cfg := testServerConfig(t.TempDir())
	cfg.ModelProxy.SandboxBaseURL = "http://harness-model-proxy.internal:8082"
	srv := &Server{cfg: cfg}

	claudeConfig := srv.resourceAllocatorConfig("claude_code")
	if !claudeConfig.ProviderCredentialsHostOnly {
		t.Fatalf("claude allocator should keep provider credentials host-only: %+v", claudeConfig)
	}
	if claudeConfig.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("claude sandbox model proxy base url = %q", claudeConfig.SandboxModelProxyBaseURL)
	}
	if claudeConfig.SandboxUID != cfg.Harness.SandboxIdentity.UID ||
		claudeConfig.SandboxGID != cfg.Harness.SandboxIdentity.GID {
		t.Fatalf("claude allocator sandbox identity = %+v", claudeConfig)
	}

	shellConfig := srv.resourceAllocatorConfig("sh")
	if shellConfig.ProviderCredentialsHostOnly || shellConfig.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("shell allocator should not request host-only provider credentials: %+v", shellConfig)
	}
}

func TestResourceAllocatorConfigUsesModelProxyPort(t *testing.T) {
	cfg := testServerConfig(t.TempDir())
	cfg.ModelProxy = config.ModelProxyConfig{
		BindURL:        "http://0.0.0.0:8083",
		SandboxBaseURL: "http://harness-model-proxy.internal:8083",
		BindPort:       8083,
	}
	srv := &Server{cfg: cfg}

	allocatorConfig := srv.resourceAllocatorConfig("claude_code")
	if allocatorConfig.HostProxyBindURL != "http://0.0.0.0:8083" ||
		allocatorConfig.ProxyPort != 8083 ||
		allocatorConfig.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8083" {
		t.Fatalf("allocator model proxy config = %+v", allocatorConfig)
	}
}

func TestResourceAllocatorConfigUsesHarnessDeploymentConfig(t *testing.T) {
	cfg := testServerConfig(t.TempDir())
	enabled := true
	disableNonessentialTraffic := false
	cfg.Harness.Agents = map[string]config.AgentConfig{
		"claude_code": {
			Enabled:                    &enabled,
			DriverID:                   "claude_code",
			ModelProfile:               "custom_anthropic",
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: &disableNonessentialTraffic,
		},
	}
	cfg.Harness.ModelProfiles = map[string]config.ModelProfileConfig{
		"custom_anthropic": {
			Enabled:  &enabled,
			Provider: "anthropic_messages",
			Model:    "opus",
			ProxyRef: config.DefaultModelProxyRef,
		},
	}
	srv := &Server{cfg: cfg}

	allocatorConfig := srv.resourceAllocatorConfig("claude_code")
	if allocatorConfig.Model != "opus" ||
		allocatorConfig.OutputFormat != "stream-json" ||
		allocatorConfig.DisableNonessentialTraffic ||
		!allocatorConfig.ProviderCredentialsHostOnly {
		t.Fatalf("allocator deployment config = %+v", allocatorConfig)
	}
}

func serverTestAllocatorConfig(cfg config.Config, driverID string) store.ResourceAllocatorConfig {
	if canonical, err := agents.CanonicalDriverID(driverID); err == nil {
		driverID = string(canonical)
	}
	outputFormat := ""
	modelAccess := false
	if spec, ok := agents.DriverSpecFor(driverID); ok {
		outputFormat = spec.OutputFormat
		modelAccess = spec.ModelAccess
	}
	model := ""
	disableNonessentialTraffic := false
	if _, agentCfg, ok := config.EnabledAgentConfigForDriver(cfg.DeploymentAgents(), driverID); ok {
		if agentCfg.DisableNonessentialTraffic != nil {
			disableNonessentialTraffic = *agentCfg.DisableNonessentialTraffic
		}
		if strings.TrimSpace(agentCfg.ModelProfile) != "" {
			if profile, ok := cfg.DeploymentModelProfiles()[agentCfg.ModelProfile]; ok && strings.TrimSpace(profile.Model) != "" {
				model = strings.TrimSpace(profile.Model)
			}
		}
	}
	return store.ResourceAllocatorConfig{
		RunDir:                      cfg.Harness.RunDir,
		CIDRPool:                    cfg.Harness.Network.CIDRPool.Prefix,
		EgressDorisFEHosts:          cfg.Harness.Network.Egress.DorisFEHosts,
		EgressDorisBEHosts:          cfg.Harness.Network.Egress.DorisBEHosts,
		EgressDorisPorts:            cfg.Harness.Network.Egress.DorisPorts,
		EgressDNSPolicy:             string(cfg.Harness.Network.Egress.DNSPolicy),
		HostProxyBindURL:            cfg.ModelProxy.BindURL,
		ProxyPort:                   cfg.ModelProxy.BindPort,
		DriverID:                    driverID,
		Model:                       model,
		OutputFormat:                outputFormat,
		DisableNonessentialTraffic:  disableNonessentialTraffic,
		SandboxUID:                  cfg.Harness.SandboxIdentity.UID,
		SandboxGID:                  cfg.Harness.SandboxIdentity.GID,
		SandboxSupplementalGIDs:     cfg.Harness.SandboxIdentity.SupplementalGIDs,
		ModelAccessAllowed:          &modelAccess,
		ProviderCredentialsHostOnly: modelAccess,
		SandboxModelProxyBaseURL:    cfg.ModelProxy.SandboxBaseURL,
	}
}

func createServerGenerationFilesystem(t *testing.T, details store.RuntimeGenerationDetails) {
	t.Helper()
	for _, path := range []string{details.CheckpointPath, details.ControlDirPath, details.BundleDirPath, details.BridgeDirPath, details.LogDirPath} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create generation filesystem path %s: %v", path, err)
		}
		if err := os.WriteFile(filepath.Join(path, ".keep"), []byte("keep"), 0o644); err != nil {
			t.Fatalf("write generation filesystem marker %s: %v", path, err)
		}
	}
}

func waitForSessionStatus(t *testing.T, ctx context.Context, st *store.Store, sessionID, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, err := st.GetSession(ctx, sessionID)
		if err != nil {
			t.Fatalf("get session: %v", err)
		}
		if got.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := st.GetSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("get final session: %v", err)
	}
	data, _ := json.Marshal(got)
	t.Fatalf("session did not reach %s: %s", want, data)
}

func waitForGenerationResourceStates(t *testing.T, ctx context.Context, st *store.Store, generationID, wantNetwork, wantResource string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var networkState, resourceState string
		if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, generationID).Scan(&networkState, &resourceState); err != nil {
			t.Fatalf("query generation resource states: %v", err)
		}
		if networkState == wantNetwork && resourceState == wantResource {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	var networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, generationID).Scan(&networkState, &resourceState); err != nil {
		t.Fatalf("query final generation resource states: %v", err)
	}
	t.Fatalf("generation %s resource states did not reach %s/%s: network=%s resource=%s", generationID, wantNetwork, wantResource, networkState, resourceState)
}

func waitForCheckpointRequests(t *testing.T, ctx context.Context, rt *recordingRuntime, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := len(rt.checkpointRequests()); got >= want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before checkpoint requests reached %d", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("checkpoint requests=%d want at least %d", len(rt.checkpointRequests()), want)
}

func waitForGenerationStatus(t *testing.T, ctx context.Context, st *store.Store, generationID, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var got string
		if err := st.DBForTest().QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&got); err != nil {
			t.Fatalf("query generation status: %v", err)
		}
		if got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before generation reached %s", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	var got string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&got); err != nil {
		t.Fatalf("query final generation status: %v", err)
	}
	t.Fatalf("generation did not reach %s: got %s", want, got)
}

func waitForGenerationLeaseAfter(t *testing.T, ctx context.Context, st *store.Store, generationID string, after time.Time) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var raw string
		if err := st.DBForTest().QueryRowContext(ctx, `SELECT lease_expires_at FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&raw); err != nil {
			t.Fatalf("query generation lease: %v", err)
		}
		if got, err := time.Parse(time.RFC3339Nano, raw); err == nil && got.After(after) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before generation lease renewed")
		case <-time.After(5 * time.Millisecond):
		}
	}
	var raw string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT lease_expires_at FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&raw); err != nil {
		t.Fatalf("query final generation lease: %v", err)
	}
	t.Fatalf("generation %s lease was not renewed after %s: got %s", generationID, after, raw)
}

func waitForEventIDs(t *testing.T, ctx context.Context, st *store.Store, want []int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		records, err := st.ListEvents(ctx, store.ListEventsParams{})
		if err != nil {
			t.Fatalf("list events: %v", err)
		}
		got := make([]int64, 0, len(records))
		for _, record := range records {
			got = append(got, record.EventID)
		}
		if int64sEqual(got, want) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before retained events reached %v", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	records, err := st.ListEvents(context.Background(), store.ListEventsParams{})
	if err != nil {
		t.Fatalf("list final events: %v", err)
	}
	got := make([]int64, 0, len(records))
	for _, record := range records {
		got = append(got, record.EventID)
	}
	t.Fatalf("event ids=%v want %v", got, want)
}

func int64sEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func createServerRunningProxyTurn(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, dir, sessionID string, now time.Time) (store.GenerationAllocation, int64, string) {
	t.Helper()
	createServerTestSession(t, ctx, st, dir, sessionID, string(sessionstate.RunningActive), now, nil)
	owner := store.GenerationLeaseOwner(ownerUUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     owner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, sessionID, allocation, ownerUUID, "host-proxy", now.Add(2*time.Second))
	turnID, err := st.EnqueueTurn(ctx, sessionID, "proxy observed turn", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "claim_" + sessionID,
		LeaseTTL:     time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	sandboxSourceIP := serverSandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID)
	if _, err := st.AckTurnStarted(ctx, store.AckStartedParams{
		SessionID:       sessionID,
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: sandboxSourceIP,
		LeaseTTL:        time.Minute,
		Now:             now.Add(4 * time.Second),
	}); err != nil {
		t.Fatalf("ack turn started: %v", err)
	}
	return allocation, turnID, sandboxSourceIP
}

func serverSandboxSourceIPForGeneration(t *testing.T, ctx context.Context, st *store.Store, generationID string) string {
	t.Helper()
	var sandboxCIDR string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT sandbox_ip_cidr
FROM network_profiles
WHERE generation_id = ?`, generationID).Scan(&sandboxCIDR); err != nil {
		t.Fatalf("query sandbox ip cidr: %v", err)
	}
	parts := strings.SplitN(sandboxCIDR, "/", 2)
	if len(parts) != 2 || parts[0] == "" {
		t.Fatalf("unexpected sandbox ip cidr: %q", sandboxCIDR)
	}
	return parts[0]
}

func waitForHubEvent(t *testing.T, ch <-chan events.Event, eventType string) events.Event {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-ch:
			if event.Type == eventType {
				return event
			}
		case <-deadline:
			t.Fatalf("timeout waiting for hub event %s", eventType)
		}
	}
}

func assertProxyCorrelationSocketPermissions(t *testing.T, socketPath string, proxyServiceGID int) {
	t.Helper()
	for _, check := range []struct {
		name string
		path string
		mode os.FileMode
	}{
		{name: "socket root", path: filepath.Dir(socketPath), mode: 0o750},
		{name: "socket", path: socketPath, mode: 0o660},
	} {
		info, err := os.Stat(check.path)
		if err != nil {
			t.Fatalf("stat proxy correlation %s: %v", check.name, err)
		}
		if info.Mode().Perm() != check.mode {
			t.Fatalf("proxy correlation %s mode=%#o want %#o", check.name, info.Mode().Perm(), check.mode)
		}
		if os.Geteuid() != 0 {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatalf("proxy correlation %s stat type = %T", check.name, info.Sys())
		}
		if stat.Uid != 0 || stat.Gid != uint32(proxyServiceGID) {
			t.Fatalf("proxy correlation %s ownership=%d:%d want 0:%d", check.name, stat.Uid, stat.Gid, proxyServiceGID)
		}
	}
}

func assertPublicSessionJSONOmitsHostFields(t *testing.T, payload []byte) {
	t.Helper()
	body := string(payload)
	for _, field := range []string{
		`"workspace"`,
		`"agent_home_path"`,
		`"agent":`,
		`"active_generation_id":`,
		`"restore_id"`,
		`"checkpoint_path"`,
		`"claude_session_uuid"`,
	} {
		if strings.Contains(body, field) {
			t.Fatalf("public session payload exposed host-only field %s: %s", field, body)
		}
	}
}

func assertContains(t *testing.T, value, want string) {
	t.Helper()
	if !strings.Contains(value, want) {
		t.Fatalf("expected %q to contain %q", value, want)
	}
}

func jsonArrayContainsAll(values []any, want ...string) bool {
	seen := map[string]struct{}{}
	for _, value := range values {
		if text, ok := value.(string); ok {
			seen[text] = struct{}{}
		}
	}
	for _, value := range want {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}

func drainHasEvent(ch <-chan events.Event, eventType string) bool {
	for {
		select {
		case event := <-ch:
			if event.Type == eventType {
				return true
			}
		default:
			return false
		}
	}
}
