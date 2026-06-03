package runtime

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/store"
)

func shortID(id string) (string, error) {
	token := strings.NewReplacer("gen_", "", "-", "").Replace(strings.TrimSpace(id))
	if len(token) > 12 {
		token = token[:12]
	}
	if token == "" || !hasASCIIAlnum(token) {
		return "", fmt.Errorf("short generation id is required")
	}
	return token, nil
}

func hasASCIIAlnum(value string) bool {
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}

type bridgeProbeRuntimeConfig struct {
	bridgeMode      string
	heartbeat       time.Duration
	pollInterval    time.Duration
	healthzStatuses []int
}

func (r *Runtime) requiredBridgeProbeConfig() (bridgeProbeRuntimeConfig, error) {
	cfg := bridgeProbeRuntimeConfig{
		bridgeMode:      strings.TrimSpace(r.cfg.BridgeMode),
		heartbeat:       r.cfg.BridgeHeartbeat,
		pollInterval:    r.cfg.BridgePollInterval,
		healthzStatuses: append([]int(nil), r.cfg.ProbeHealthzStatuses...),
	}
	if cfg.bridgeMode == "" {
		return bridgeProbeRuntimeConfig{}, fmt.Errorf("bridge mode is required")
	}
	if cfg.heartbeat <= 0 {
		return bridgeProbeRuntimeConfig{}, fmt.Errorf("bridge heartbeat interval is required")
	}
	if cfg.pollInterval <= 0 {
		return bridgeProbeRuntimeConfig{}, fmt.Errorf("bridge poll interval is required")
	}
	if len(cfg.healthzStatuses) == 0 {
		return bridgeProbeRuntimeConfig{}, fmt.Errorf("probe healthz statuses are required")
	}
	for _, status := range cfg.healthzStatuses {
		if status < 100 || status > 599 {
			return bridgeProbeRuntimeConfig{}, fmt.Errorf("invalid probe healthz status %d", status)
		}
	}
	return cfg, nil
}

func formatSeconds(value time.Duration) string {
	if value%time.Second == 0 {
		return strconv.FormatInt(int64(value/time.Second), 10)
	}
	return strconv.FormatFloat(float64(value)/float64(time.Second), 'f', -1, 64)
}

func (r *Runtime) runscNetwork(details store.RuntimeGenerationDetails) (string, error) {
	network := strings.TrimSpace(details.RunscNetwork)
	if network == "" {
		return "", fmt.Errorf("runsc network is required")
	}
	return network, nil
}

func (r *Runtime) runscOverlay2(details store.RuntimeGenerationDetails) (string, error) {
	overlay2 := strings.TrimSpace(details.RunscOverlay2)
	if overlay2 == "" {
		return "", fmt.Errorf("runsc overlay2 is required")
	}
	return overlay2, nil
}

type runtimeSandboxIdentity struct {
	UID              int
	GID              int
	SupplementalGIDs []int
}

func (r *Runtime) requiredSandboxIdentity(details store.RuntimeGenerationDetails) (runtimeSandboxIdentity, error) {
	supplementalGIDs := append([]int(nil), details.SandboxSupplementalGIDs...)
	if len(supplementalGIDs) == 0 {
		supplementalGIDs = append([]int(nil), r.cfg.SandboxSupplementalGIDs...)
	}
	identity := runtimeSandboxIdentity{
		UID:              details.SandboxUID,
		GID:              details.SandboxGID,
		SupplementalGIDs: supplementalGIDs,
	}
	if identity.UID <= 0 {
		identity.UID = r.cfg.SandboxUID
	}
	if identity.GID <= 0 {
		identity.GID = r.cfg.SandboxGID
	}
	sort.Ints(identity.SupplementalGIDs)
	if identity.UID <= 0 {
		return runtimeSandboxIdentity{}, fmt.Errorf("sandbox identity uid must be > 0")
	}
	if identity.GID <= 0 {
		return runtimeSandboxIdentity{}, fmt.Errorf("sandbox identity gid must be > 0")
	}
	seen := map[int]struct{}{}
	for _, gid := range identity.SupplementalGIDs {
		if gid <= 0 {
			return runtimeSandboxIdentity{}, fmt.Errorf("sandbox identity supplemental gids must be positive")
		}
		if _, ok := seen[gid]; ok {
			return runtimeSandboxIdentity{}, fmt.Errorf("sandbox identity supplemental gids contain duplicate gid %d", gid)
		}
		seen[gid] = struct{}{}
	}
	return identity, nil
}
