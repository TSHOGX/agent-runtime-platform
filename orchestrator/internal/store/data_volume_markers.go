package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
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
