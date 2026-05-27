package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const DataVolumeLayoutVersion = 1

type RuntimeIdentity struct {
	UID              int
	GID              int
	SupplementalGIDs []int
}

type DataVolumeProvisionerConfig struct {
	SessionsRoot    string
	AgentHomesRoot  string
	EvidenceRoot    string
	LayoutVersion   int
	RuntimeIdentity RuntimeIdentity
}

type ProvisionSessionWorkspaceParams struct {
	SessionID string
	Config    DataVolumeProvisionerConfig
	Now       time.Time
}

type ProvisionSessionDriverHomeParams struct {
	SessionID string
	Driver    string
	Config    DataVolumeProvisionerConfig
	Now       time.Time
}

type VerifySessionWorkspaceVolumeParams struct {
	SessionID string
	Config    DataVolumeProvisionerConfig
}

type VerifySessionDriverHomeVolumeParams struct {
	SessionID string
	Driver    string
	Config    DataVolumeProvisionerConfig
}

type SessionWorkspaceVolume struct {
	SessionID                string
	HostPath                 string
	LayoutVersion            int
	SandboxUID               int
	SandboxGID               int
	SandboxSupplementalGIDs  []int
	RuntimeIdentityDigest    string
	ProvisionedAt            time.Time
	ProvisioningMarkerPath   string
	ProvisioningMarkerDigest string
}

type SessionDriverHomeVolume struct {
	SessionID                string
	Driver                   string
	HostPath                 string
	LayoutVersion            int
	SandboxUID               int
	SandboxGID               int
	SandboxSupplementalGIDs  []int
	RuntimeIdentityDigest    string
	ProvisionedAt            time.Time
	ProvisioningMarkerPath   string
	ProvisioningMarkerDigest string
}

type dataVolumeKind string

const (
	dataVolumeWorkspace  dataVolumeKind = "workspace"
	dataVolumeDriverHome dataVolumeKind = "driver_home"
)

type dataVolumeMarker struct {
	MarkerVersion         int                    `json:"marker_version"`
	VolumeType            string                 `json:"volume_type"`
	SessionID             string                 `json:"session_id"`
	Driver                string                 `json:"driver,omitempty"`
	HostPath              string                 `json:"host_path"`
	LayoutVersion         int                    `json:"layout_version"`
	RuntimeIdentity       dataVolumeIdentityJSON `json:"runtime_identity"`
	RuntimeIdentityDigest string                 `json:"runtime_identity_digest"`
	ProvisionedAt         string                 `json:"provisioned_at"`
}

type dataVolumeIdentityJSON struct {
	SandboxUID              int   `json:"sandbox_uid"`
	SandboxGID              int   `json:"sandbox_gid"`
	SandboxSupplementalGIDs []int `json:"sandbox_supplemental_gids"`
}

func (s *Store) ProvisionSessionWorkspace(ctx context.Context, p ProvisionSessionWorkspaceParams) (SessionWorkspaceVolume, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	cfg, err := normalizeDataVolumeConfig(p.Config)
	if err != nil {
		return SessionWorkspaceVolume{}, err
	}
	sessionID, err := dataVolumeSafePathComponent("session id", p.SessionID)
	if err != nil {
		return SessionWorkspaceVolume{}, err
	}
	hostPath := filepath.Join(cfg.SessionsRoot, sessionID)
	markerPath := filepath.Join(cfg.EvidenceRoot, "workspaces", sessionID+".json")
	static := dataVolumeStatic{
		kind:       dataVolumeWorkspace,
		sessionID:  sessionID,
		hostPath:   hostPath,
		markerPath: markerPath,
		cfg:        cfg,
	}
	existing, err := s.getSessionWorkspace(ctx, sessionID)
	if err == nil {
		if err := validateExistingWorkspaceVolume(existing, static); err != nil {
			return SessionWorkspaceVolume{}, err
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SessionWorkspaceVolume{}, err
	}
	if err := prepareFreshDataVolumeHostDir(hostPath, cfg.SessionsRoot); err != nil {
		return SessionWorkspaceVolume{}, err
	}
	marker, markerBytes, markerDigest, err := buildDataVolumeMarker(static, p.Now)
	if err != nil {
		return SessionWorkspaceVolume{}, err
	}
	if err := writeDataVolumeMarker(markerPath, cfg.EvidenceRoot, markerBytes, markerDigest); err != nil {
		return SessionWorkspaceVolume{}, err
	}
	row := SessionWorkspaceVolume{
		SessionID:                sessionID,
		HostPath:                 hostPath,
		LayoutVersion:            cfg.LayoutVersion,
		SandboxUID:               cfg.RuntimeIdentity.UID,
		SandboxGID:               cfg.RuntimeIdentity.GID,
		SandboxSupplementalGIDs:  append([]int(nil), cfg.RuntimeIdentity.SupplementalGIDs...),
		RuntimeIdentityDigest:    marker.RuntimeIdentityDigest,
		ProvisionedAt:            p.Now.UTC(),
		ProvisioningMarkerPath:   markerPath,
		ProvisioningMarkerDigest: markerDigest,
	}
	if err := s.insertSessionWorkspace(ctx, row); err != nil {
		return SessionWorkspaceVolume{}, err
	}
	return s.getSessionWorkspace(ctx, sessionID)
}

func (s *Store) ProvisionSessionDriverHome(ctx context.Context, p ProvisionSessionDriverHomeParams) (SessionDriverHomeVolume, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	cfg, err := normalizeDataVolumeConfig(p.Config)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	sessionID, err := dataVolumeSafePathComponent("session id", p.SessionID)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	driver, err := dataVolumeSafePathComponent("driver", p.Driver)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	hostPath := filepath.Join(cfg.AgentHomesRoot, sessionID, driver)
	markerPath := filepath.Join(cfg.EvidenceRoot, "driver-homes", sessionID, driver+".json")
	static := dataVolumeStatic{
		kind:       dataVolumeDriverHome,
		sessionID:  sessionID,
		driver:     driver,
		hostPath:   hostPath,
		markerPath: markerPath,
		cfg:        cfg,
	}
	existing, err := s.getSessionDriverHome(ctx, sessionID, driver)
	if err == nil {
		if err := validateExistingDriverHomeVolume(existing, static); err != nil {
			return SessionDriverHomeVolume{}, err
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SessionDriverHomeVolume{}, err
	}
	if err := prepareFreshDataVolumeHostDir(hostPath, cfg.AgentHomesRoot); err != nil {
		return SessionDriverHomeVolume{}, err
	}
	marker, markerBytes, markerDigest, err := buildDataVolumeMarker(static, p.Now)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	if err := writeDataVolumeMarker(markerPath, cfg.EvidenceRoot, markerBytes, markerDigest); err != nil {
		return SessionDriverHomeVolume{}, err
	}
	row := SessionDriverHomeVolume{
		SessionID:                sessionID,
		Driver:                   driver,
		HostPath:                 hostPath,
		LayoutVersion:            cfg.LayoutVersion,
		SandboxUID:               cfg.RuntimeIdentity.UID,
		SandboxGID:               cfg.RuntimeIdentity.GID,
		SandboxSupplementalGIDs:  append([]int(nil), cfg.RuntimeIdentity.SupplementalGIDs...),
		RuntimeIdentityDigest:    marker.RuntimeIdentityDigest,
		ProvisionedAt:            p.Now.UTC(),
		ProvisioningMarkerPath:   markerPath,
		ProvisioningMarkerDigest: markerDigest,
	}
	if err := s.insertSessionDriverHome(ctx, row); err != nil {
		return SessionDriverHomeVolume{}, err
	}
	return s.getSessionDriverHome(ctx, sessionID, driver)
}

func RuntimeIdentityDigest(identity RuntimeIdentity) (string, error) {
	normalized := normalizeRuntimeIdentity(identity)
	if err := validateRuntimeIdentity(normalized); err != nil {
		return "", err
	}
	payload := dataVolumeIdentityJSON{
		SandboxUID:              normalized.UID,
		SandboxGID:              normalized.GID,
		SandboxSupplementalGIDs: normalized.SupplementalGIDs,
	}
	data, err := canonicalDataVolumeJSON(payload)
	if err != nil {
		return "", err
	}
	return SandboxContractDigest(data), nil
}

func (s *Store) GetSessionWorkspaceVolume(ctx context.Context, sessionID string) (SessionWorkspaceVolume, error) {
	sessionID, err := dataVolumeSafePathComponent("session id", sessionID)
	if err != nil {
		return SessionWorkspaceVolume{}, err
	}
	volume, err := s.getSessionWorkspace(ctx, sessionID)
	if err != nil {
		return SessionWorkspaceVolume{}, err
	}
	if err := verifyDataVolumeMarker(volume.ProvisioningMarkerPath, volume.ProvisioningMarkerDigest); err != nil {
		return SessionWorkspaceVolume{}, err
	}
	return volume, nil
}

func (s *Store) VerifySessionWorkspaceVolume(ctx context.Context, p VerifySessionWorkspaceVolumeParams) (SessionWorkspaceVolume, error) {
	cfg, err := normalizeDataVolumeConfig(p.Config)
	if err != nil {
		return SessionWorkspaceVolume{}, err
	}
	sessionID, err := dataVolumeSafePathComponent("session id", p.SessionID)
	if err != nil {
		return SessionWorkspaceVolume{}, err
	}
	static := dataVolumeStatic{
		kind:       dataVolumeWorkspace,
		sessionID:  sessionID,
		hostPath:   filepath.Join(cfg.SessionsRoot, sessionID),
		markerPath: filepath.Join(cfg.EvidenceRoot, "workspaces", sessionID+".json"),
		cfg:        cfg,
	}
	volume, err := s.getSessionWorkspace(ctx, sessionID)
	if err != nil {
		return SessionWorkspaceVolume{}, err
	}
	if err := validateExistingWorkspaceVolume(volume, static); err != nil {
		return SessionWorkspaceVolume{}, err
	}
	return volume, nil
}

func (s *Store) ListVerifiedSessionWorkspaceVolumes(ctx context.Context, cfg DataVolumeProvisionerConfig) ([]SessionWorkspaceVolume, error) {
	normalized, err := normalizeDataVolumeConfig(cfg)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT session_id, host_path, layout_version, sandbox_uid, sandbox_gid,
       sandbox_supplemental_gids, runtime_identity_digest, provisioned_at,
       provisioning_marker_path, provisioning_marker_digest
FROM session_workspaces
ORDER BY session_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	volumes := []SessionWorkspaceVolume{}
	for rows.Next() {
		volume, err := scanSessionWorkspaceVolume(rows)
		if err != nil {
			return nil, err
		}
		sessionID, err := dataVolumeSafePathComponent("session id", volume.SessionID)
		if err != nil {
			return nil, err
		}
		static := dataVolumeStatic{
			kind:       dataVolumeWorkspace,
			sessionID:  sessionID,
			hostPath:   filepath.Join(normalized.SessionsRoot, sessionID),
			markerPath: filepath.Join(normalized.EvidenceRoot, "workspaces", sessionID+".json"),
			cfg:        normalized,
		}
		if err := validateExistingWorkspaceVolume(volume, static); err != nil {
			return nil, err
		}
		volumes = append(volumes, volume)
	}
	return volumes, rows.Err()
}

func (s *Store) GetSessionDriverHomeVolume(ctx context.Context, sessionID, driver string) (SessionDriverHomeVolume, error) {
	sessionID, err := dataVolumeSafePathComponent("session id", sessionID)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	driver, err = dataVolumeSafePathComponent("driver", driver)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	volume, err := s.getSessionDriverHome(ctx, sessionID, driver)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	if err := verifyDataVolumeMarker(volume.ProvisioningMarkerPath, volume.ProvisioningMarkerDigest); err != nil {
		return SessionDriverHomeVolume{}, err
	}
	return volume, nil
}

func (s *Store) VerifySessionDriverHomeVolume(ctx context.Context, p VerifySessionDriverHomeVolumeParams) (SessionDriverHomeVolume, error) {
	cfg, err := normalizeDataVolumeConfig(p.Config)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	sessionID, err := dataVolumeSafePathComponent("session id", p.SessionID)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	driver, err := dataVolumeSafePathComponent("driver", p.Driver)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	static := dataVolumeStatic{
		kind:       dataVolumeDriverHome,
		sessionID:  sessionID,
		driver:     driver,
		hostPath:   filepath.Join(cfg.AgentHomesRoot, sessionID, driver),
		markerPath: filepath.Join(cfg.EvidenceRoot, "driver-homes", sessionID, driver+".json"),
		cfg:        cfg,
	}
	volume, err := s.getSessionDriverHome(ctx, sessionID, driver)
	if err != nil {
		return SessionDriverHomeVolume{}, err
	}
	if err := validateExistingDriverHomeVolume(volume, static); err != nil {
		return SessionDriverHomeVolume{}, err
	}
	return volume, nil
}

type normalizedDataVolumeConfig struct {
	SessionsRoot    string
	AgentHomesRoot  string
	EvidenceRoot    string
	LayoutVersion   int
	RuntimeIdentity RuntimeIdentity
}

type dataVolumeStatic struct {
	kind       dataVolumeKind
	sessionID  string
	driver     string
	hostPath   string
	markerPath string
	cfg        normalizedDataVolumeConfig
}

func normalizeDataVolumeConfig(cfg DataVolumeProvisionerConfig) (normalizedDataVolumeConfig, error) {
	layoutVersion := cfg.LayoutVersion
	if layoutVersion == 0 {
		layoutVersion = DataVolumeLayoutVersion
	}
	if layoutVersion <= 0 {
		return normalizedDataVolumeConfig{}, fmt.Errorf("data volume layout version must be > 0")
	}
	identity := normalizeRuntimeIdentity(cfg.RuntimeIdentity)
	if err := validateRuntimeIdentity(identity); err != nil {
		return normalizedDataVolumeConfig{}, err
	}
	sessionsRoot, err := dataVolumeCanonicalRoot(cfg.SessionsRoot, "sessions root")
	if err != nil {
		return normalizedDataVolumeConfig{}, err
	}
	agentHomesRoot, err := dataVolumeCanonicalRoot(cfg.AgentHomesRoot, "agent homes root")
	if err != nil {
		return normalizedDataVolumeConfig{}, err
	}
	evidenceRoot, err := dataVolumeCanonicalRoot(cfg.EvidenceRoot, "data volume evidence root")
	if err != nil {
		return normalizedDataVolumeConfig{}, err
	}
	if dataVolumeRootsOverlap(sessionsRoot, agentHomesRoot) {
		return normalizedDataVolumeConfig{}, fmt.Errorf("data volume sessions root %q overlaps agent homes root %q", sessionsRoot, agentHomesRoot)
	}
	for _, root := range []struct {
		label string
		path  string
	}{
		{label: "sessions root", path: sessionsRoot},
		{label: "agent homes root", path: agentHomesRoot},
	} {
		if dataVolumeRootsOverlap(evidenceRoot, root.path) {
			return normalizedDataVolumeConfig{}, fmt.Errorf("data volume evidence root %q overlaps %s %q", evidenceRoot, root.label, root.path)
		}
	}
	return normalizedDataVolumeConfig{
		SessionsRoot:    sessionsRoot,
		AgentHomesRoot:  agentHomesRoot,
		EvidenceRoot:    evidenceRoot,
		LayoutVersion:   layoutVersion,
		RuntimeIdentity: identity,
	}, nil
}

func normalizeRuntimeIdentity(identity RuntimeIdentity) RuntimeIdentity {
	normalized := identity
	normalized.SupplementalGIDs = append([]int(nil), identity.SupplementalGIDs...)
	sort.Ints(normalized.SupplementalGIDs)
	return normalized
}

func validateRuntimeIdentity(identity RuntimeIdentity) error {
	if identity.UID <= 0 {
		return fmt.Errorf("runtime identity uid must be > 0")
	}
	if identity.GID <= 0 {
		return fmt.Errorf("runtime identity gid must be > 0")
	}
	seen := map[int]struct{}{}
	for _, gid := range identity.SupplementalGIDs {
		if gid <= 0 {
			return fmt.Errorf("runtime identity supplemental gids must be positive")
		}
		if _, ok := seen[gid]; ok {
			return fmt.Errorf("runtime identity supplemental gids contain duplicate gid %d", gid)
		}
		seen[gid] = struct{}{}
	}
	return nil
}

func dataVolumeCanonicalRoot(path, label string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) {
		return "", fmt.Errorf("%s is required and must be absolute", label)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("%s %q must be absolute: %w", label, path, err)
	}
	cleaned := filepath.Clean(absolute)
	if cleaned == string(filepath.Separator) {
		return "", fmt.Errorf("%s must not be filesystem root", label)
	}
	if resolved, err := filepath.EvalSymlinks(cleaned); err == nil {
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("resolve %s %q: %w", label, cleaned, err)
	}
	existing, missing, err := dataVolumeDeepestExistingRoot(cleaned)
	if err != nil {
		return "", fmt.Errorf("resolve %s %q: %w", label, cleaned, err)
	}
	if existing == "" {
		return cleaned, nil
	}
	resolved, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", fmt.Errorf("resolve %s existing prefix %q: %w", label, existing, err)
	}
	return filepath.Clean(filepath.Join(append([]string{resolved}, missing...)...)), nil
}

func dataVolumeDeepestExistingRoot(path string) (string, []string, error) {
	var missing []string
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		if _, err := os.Lstat(current); err == nil {
			for i, j := 0, len(missing)-1; i < j; i, j = i+1, j-1 {
				missing[i], missing[j] = missing[j], missing[i]
			}
			return current, missing, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", nil, err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", nil, nil
		}
		missing = append(missing, filepath.Base(current))
	}
}

func dataVolumeRootsOverlap(a, b string) bool {
	return dataVolumePathWithinRoot(a, b) || dataVolumePathWithinRoot(b, a)
}

func dataVolumeSafePathComponent(label, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || filepath.Base(value) != value || value == "." || value == ".." {
		return "", fmt.Errorf("%s %q is not a safe path component", label, value)
	}
	return value, nil
}

func dataVolumeValidateHostPath(path, root string) error {
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == ".." {
			return fmt.Errorf("data volume host path %q must not contain '..'", path)
		}
	}
	if !filepath.IsAbs(path) {
		return fmt.Errorf("data volume host path %q must be absolute", path)
	}
	cleaned := filepath.Clean(path)
	if cleaned != path {
		return fmt.Errorf("data volume host path %q must be canonical", path)
	}
	if !dataVolumePathWithinRoot(cleaned, root) {
		return fmt.Errorf("data volume host path %q is outside root %q", cleaned, root)
	}
	return nil
}

func dataVolumePathWithinRoot(path, root string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func prepareFreshDataVolumeHostDir(path, root string) error {
	if err := dataVolumeValidateHostPath(path, root); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(path, 0o755)
	}
	if err != nil {
		return fmt.Errorf("stat data volume host path %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("data volume host path %q must not be a symlink", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("data volume host path %q must be a directory", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read data volume host path %q: %w", path, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("fresh data volume host path %q must be empty", path)
	}
	return nil
}

func buildDataVolumeMarker(static dataVolumeStatic, provisionedAt time.Time) (dataVolumeMarker, []byte, string, error) {
	identityDigest, err := RuntimeIdentityDigest(static.cfg.RuntimeIdentity)
	if err != nil {
		return dataVolumeMarker{}, nil, "", err
	}
	marker := dataVolumeMarker{
		MarkerVersion: 1,
		VolumeType:    string(static.kind),
		SessionID:     static.sessionID,
		Driver:        static.driver,
		HostPath:      static.hostPath,
		LayoutVersion: static.cfg.LayoutVersion,
		RuntimeIdentity: dataVolumeIdentityJSON{
			SandboxUID:              static.cfg.RuntimeIdentity.UID,
			SandboxGID:              static.cfg.RuntimeIdentity.GID,
			SandboxSupplementalGIDs: static.cfg.RuntimeIdentity.SupplementalGIDs,
		},
		RuntimeIdentityDigest: identityDigest,
		ProvisionedAt:         formatTime(provisionedAt),
	}
	data, err := canonicalDataVolumeJSON(marker)
	if err != nil {
		return dataVolumeMarker{}, nil, "", err
	}
	return marker, data, SandboxContractDigest(data), nil
}

func writeDataVolumeMarker(path, evidenceRoot string, payload []byte, digest string) error {
	if err := ensureDataVolumeMarkerDir(evidenceRoot, filepath.Dir(path)); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err == nil {
		if err := verifyRootOwnedRegularDataVolumeMarkerInfo(path, info); err != nil {
			return err
		}
		existing, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read data volume marker %q: %w", path, err)
		}
		if bytes.Equal(existing, payload) && SandboxContractDigest(existing) == digest {
			return nil
		}
		return fmt.Errorf("data volume marker %q already exists with different payload", path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read data volume marker %q: %w", path, err)
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("write data volume marker: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write data volume marker: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write data volume marker: %w", err)
	}
	if err := verifyRootOwnedRegularDataVolumeMarker(tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename data volume marker: %w", err)
	}
	return verifyRootOwnedRegularDataVolumeMarker(path)
}

func readVerifiedDataVolumeMarker(path, digest string) ([]byte, error) {
	if err := verifyRootOwnedRegularDataVolumeMarker(path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read data volume marker %q: %w", path, err)
	}
	if got := SandboxContractDigest(data); got != digest {
		return nil, fmt.Errorf("data volume marker digest mismatch for %q: got %s want %s", path, got, digest)
	}
	canonical, err := canonicalDataVolumeJSONBytes(data)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(data, canonical) {
		return nil, fmt.Errorf("data volume marker %q is not canonical", path)
	}
	return data, nil
}

func ensureDataVolumeMarkerDir(evidenceRoot, markerDir string) error {
	evidenceRoot = filepath.Clean(evidenceRoot)
	markerDir = filepath.Clean(markerDir)
	if evidenceRoot == string(filepath.Separator) {
		return fmt.Errorf("data volume evidence root must not be filesystem root")
	}
	if !dataVolumePathWithinRoot(markerDir, evidenceRoot) {
		return fmt.Errorf("data volume marker dir %q is outside evidence root %q", markerDir, evidenceRoot)
	}
	if err := os.MkdirAll(evidenceRoot, 0o755); err != nil {
		return fmt.Errorf("create data volume evidence root: %w", err)
	}
	if err := verifyRootOwnedDataVolumeDir(evidenceRoot, "data volume evidence root"); err != nil {
		return err
	}
	rel, err := filepath.Rel(evidenceRoot, markerDir)
	if err != nil {
		return fmt.Errorf("resolve data volume marker dir %q: %w", markerDir, err)
	}
	if rel == "." {
		return nil
	}
	current := evidenceRoot
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("data volume marker dir %q is not canonical under evidence root %q", markerDir, evidenceRoot)
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0o755); err != nil {
				return fmt.Errorf("create data volume marker dir %q: %w", current, err)
			}
			info, err = os.Lstat(current)
		}
		if err != nil {
			return fmt.Errorf("stat data volume marker dir %q: %w", current, err)
		}
		if err := verifyRootOwnedDataVolumeDirInfo(current, info, "data volume marker dir"); err != nil {
			return err
		}
	}
	return nil
}

func verifyRootOwnedDataVolumeDir(path, label string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat %s %q: %w", label, path, err)
	}
	return verifyRootOwnedDataVolumeDirInfo(path, info, label)
}

func verifyRootOwnedDataVolumeDirInfo(path string, info os.FileInfo, label string) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s %q must not be a symlink", label, path)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s %q must be a directory", label, path)
	}
	return verifyRootOwnedProtectedDataVolumePath(path, info, label)
}

func verifyRootOwnedRegularDataVolumeMarker(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat data volume marker %q: %w", path, err)
	}
	return verifyRootOwnedRegularDataVolumeMarkerInfo(path, info)
}

func verifyRootOwnedRegularDataVolumeMarkerInfo(path string, info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("data volume marker %q must not be a symlink", path)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("data volume marker %q must be a regular file", path)
	}
	return verifyRootOwnedProtectedDataVolumePath(path, info, "data volume marker")
}

func verifyRootOwnedProtectedDataVolumePath(path string, info os.FileInfo, label string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("stat ownership unavailable for %s %q", label, path)
	}
	if stat.Uid != 0 {
		return fmt.Errorf("%s %q must be root-owned, got uid %d", label, path, stat.Uid)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%s %q must not be group/world writable", label, path)
	}
	return nil
}

func verifyDataVolumeMarker(path, digest string) error {
	_, err := readVerifiedDataVolumeMarker(path, digest)
	return err
}

func verifyDataVolumeMarkerMatches(path, digest string, expected dataVolumeMarker) error {
	data, err := readVerifiedDataVolumeMarker(path, digest)
	if err != nil {
		return err
	}
	expectedData, err := canonicalDataVolumeJSON(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, expectedData) {
		return fmt.Errorf("data volume marker %q does not match expected provisioning evidence", path)
	}
	return nil
}

func validateExistingWorkspaceVolume(row SessionWorkspaceVolume, static dataVolumeStatic) error {
	if row.SessionID != static.sessionID ||
		row.HostPath != static.hostPath ||
		row.LayoutVersion != static.cfg.LayoutVersion ||
		row.SandboxUID != static.cfg.RuntimeIdentity.UID ||
		row.SandboxGID != static.cfg.RuntimeIdentity.GID ||
		!equalIntSlices(row.SandboxSupplementalGIDs, static.cfg.RuntimeIdentity.SupplementalGIDs) ||
		row.ProvisioningMarkerPath != static.markerPath {
		return fmt.Errorf("session workspace volume row does not match expected provisioning config")
	}
	identityDigest, err := RuntimeIdentityDigest(static.cfg.RuntimeIdentity)
	if err != nil {
		return err
	}
	if row.RuntimeIdentityDigest != identityDigest {
		return fmt.Errorf("session workspace volume row does not match expected runtime identity digest")
	}
	return verifyDataVolumeMarkerMatches(row.ProvisioningMarkerPath, row.ProvisioningMarkerDigest, dataVolumeMarker{
		MarkerVersion: 1,
		VolumeType:    string(dataVolumeWorkspace),
		SessionID:     static.sessionID,
		HostPath:      static.hostPath,
		LayoutVersion: static.cfg.LayoutVersion,
		RuntimeIdentity: dataVolumeIdentityJSON{
			SandboxUID:              static.cfg.RuntimeIdentity.UID,
			SandboxGID:              static.cfg.RuntimeIdentity.GID,
			SandboxSupplementalGIDs: static.cfg.RuntimeIdentity.SupplementalGIDs,
		},
		RuntimeIdentityDigest: identityDigest,
		ProvisionedAt:         formatTime(row.ProvisionedAt),
	})
}

func validateExistingDriverHomeVolume(row SessionDriverHomeVolume, static dataVolumeStatic) error {
	if row.SessionID != static.sessionID ||
		row.Driver != static.driver ||
		row.HostPath != static.hostPath ||
		row.LayoutVersion != static.cfg.LayoutVersion ||
		row.SandboxUID != static.cfg.RuntimeIdentity.UID ||
		row.SandboxGID != static.cfg.RuntimeIdentity.GID ||
		!equalIntSlices(row.SandboxSupplementalGIDs, static.cfg.RuntimeIdentity.SupplementalGIDs) ||
		row.ProvisioningMarkerPath != static.markerPath {
		return fmt.Errorf("session driver home volume row does not match expected provisioning config")
	}
	identityDigest, err := RuntimeIdentityDigest(static.cfg.RuntimeIdentity)
	if err != nil {
		return err
	}
	if row.RuntimeIdentityDigest != identityDigest {
		return fmt.Errorf("session driver home volume row does not match expected runtime identity digest")
	}
	return verifyDataVolumeMarkerMatches(row.ProvisioningMarkerPath, row.ProvisioningMarkerDigest, dataVolumeMarker{
		MarkerVersion: 1,
		VolumeType:    string(dataVolumeDriverHome),
		SessionID:     static.sessionID,
		Driver:        static.driver,
		HostPath:      static.hostPath,
		LayoutVersion: static.cfg.LayoutVersion,
		RuntimeIdentity: dataVolumeIdentityJSON{
			SandboxUID:              static.cfg.RuntimeIdentity.UID,
			SandboxGID:              static.cfg.RuntimeIdentity.GID,
			SandboxSupplementalGIDs: static.cfg.RuntimeIdentity.SupplementalGIDs,
		},
		RuntimeIdentityDigest: identityDigest,
		ProvisionedAt:         formatTime(row.ProvisionedAt),
	})
}

func (s *Store) insertSessionWorkspace(ctx context.Context, row SessionWorkspaceVolume) error {
	gids, err := json.Marshal(row.SandboxSupplementalGIDs)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO session_workspaces (
  session_id, host_path, layout_version, sandbox_uid, sandbox_gid,
  sandbox_supplemental_gids, runtime_identity_digest, provisioned_at,
  provisioning_marker_path, provisioning_marker_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id) DO NOTHING`,
		row.SessionID, row.HostPath, row.LayoutVersion, row.SandboxUID, row.SandboxGID,
		string(gids), row.RuntimeIdentityDigest, formatTime(row.ProvisionedAt),
		row.ProvisioningMarkerPath, row.ProvisioningMarkerDigest)
	return err
}

func (s *Store) insertSessionDriverHome(ctx context.Context, row SessionDriverHomeVolume) error {
	gids, err := json.Marshal(row.SandboxSupplementalGIDs)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO session_driver_homes (
  session_id, driver, host_path, layout_version, sandbox_uid, sandbox_gid,
  sandbox_supplemental_gids, runtime_identity_digest, provisioned_at,
  provisioning_marker_path, provisioning_marker_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id, driver) DO NOTHING`,
		row.SessionID, row.Driver, row.HostPath, row.LayoutVersion, row.SandboxUID, row.SandboxGID,
		string(gids), row.RuntimeIdentityDigest, formatTime(row.ProvisionedAt),
		row.ProvisioningMarkerPath, row.ProvisioningMarkerDigest)
	return err
}

func (s *Store) getSessionWorkspace(ctx context.Context, sessionID string) (SessionWorkspaceVolume, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT session_id, host_path, layout_version, sandbox_uid, sandbox_gid,
       sandbox_supplemental_gids, runtime_identity_digest, provisioned_at,
       provisioning_marker_path, provisioning_marker_digest
FROM session_workspaces
WHERE session_id = ?`, sessionID)
	return scanSessionWorkspaceVolume(row)
}

func scanSessionWorkspaceVolume(row interface {
	Scan(dest ...any) error
}) (SessionWorkspaceVolume, error) {
	var volume SessionWorkspaceVolume
	var gids, provisionedAt string
	if err := row.Scan(
		&volume.SessionID,
		&volume.HostPath,
		&volume.LayoutVersion,
		&volume.SandboxUID,
		&volume.SandboxGID,
		&gids,
		&volume.RuntimeIdentityDigest,
		&provisionedAt,
		&volume.ProvisioningMarkerPath,
		&volume.ProvisioningMarkerDigest,
	); err != nil {
		return SessionWorkspaceVolume{}, err
	}
	if err := json.Unmarshal([]byte(gids), &volume.SandboxSupplementalGIDs); err != nil {
		return SessionWorkspaceVolume{}, err
	}
	volume.ProvisionedAt = parseTime(provisionedAt)
	return volume, nil
}

func (s *Store) getSessionDriverHome(ctx context.Context, sessionID, driver string) (SessionDriverHomeVolume, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT session_id, driver, host_path, layout_version, sandbox_uid, sandbox_gid,
       sandbox_supplemental_gids, runtime_identity_digest, provisioned_at,
       provisioning_marker_path, provisioning_marker_digest
FROM session_driver_homes
WHERE session_id = ?
  AND driver = ?`, sessionID, driver)
	var volume SessionDriverHomeVolume
	var gids, provisionedAt string
	if err := row.Scan(
		&volume.SessionID,
		&volume.Driver,
		&volume.HostPath,
		&volume.LayoutVersion,
		&volume.SandboxUID,
		&volume.SandboxGID,
		&gids,
		&volume.RuntimeIdentityDigest,
		&provisionedAt,
		&volume.ProvisioningMarkerPath,
		&volume.ProvisioningMarkerDigest,
	); err != nil {
		return SessionDriverHomeVolume{}, err
	}
	if err := json.Unmarshal([]byte(gids), &volume.SandboxSupplementalGIDs); err != nil {
		return SessionDriverHomeVolume{}, err
	}
	volume.ProvisionedAt = parseTime(provisionedAt)
	return volume, nil
}

func canonicalDataVolumeJSON(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return canonicalDataVolumeJSONBytes(data)
}

func canonicalDataVolumeJSONBytes(data []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("data volume marker contains trailing JSON")
	}
	if err := rejectFloatingPointJSONNumbers(normalized); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(normalized); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func equalIntSlices(a, b []int) bool {
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
