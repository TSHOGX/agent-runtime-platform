package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateIsolationRootsAllowsReservedSubroots(t *testing.T) {
	roots := isolationRootsForTest(t)

	canonical, err := ValidateIsolationRoots(roots)
	if err != nil {
		t.Fatalf("validate isolation roots: %v", err)
	}
	if canonical.DataVolumeEvidenceRoot != filepath.Clean(roots.DataVolumeEvidenceRoot) ||
		canonical.ProxyInternalRoot != filepath.Clean(roots.ProxyInternalRoot) ||
		canonical.DBStateRoot != filepath.Dir(filepath.Clean(roots.DBPath)) {
		t.Fatalf("unexpected canonical roots: %+v", canonical)
	}
}

func TestValidateIsolationRootsRejectsDBUnderSandboxRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.DBPath = filepath.Join(roots.SessionsRoot, "orchestrator.db")

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("expected overlapping db root rejection, got %v", err)
	}
}

func TestValidateIsolationRootsRejectsProxyInternalUnderControlRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.ProxyInternalRoot = filepath.Join(roots.RunDir, "control", "proxy-internal")

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "overlaps sandbox-bindable run control root") {
		t.Fatalf("expected proxy internal overlap rejection, got %v", err)
	}
}

func TestValidateIsolationRootsRejectsProxyInternalUnderLogsRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.ProxyInternalRoot = filepath.Join(roots.RunDir, "logs", "proxy-internal")

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "overlaps sandbox-bindable run logs root") {
		t.Fatalf("expected proxy internal overlap rejection, got %v", err)
	}
}

func TestValidateIsolationRootsRejectsEvidenceUnderSandboxRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.DataVolumeEvidenceRoot = filepath.Join(roots.AgentHomesRoot, "evidence")

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("expected evidence overlap rejection, got %v", err)
	}
}

func TestValidateIsolationRootsRejectsRelativeRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.RootFSPath = "relative/rootfs"

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("expected absolute path rejection, got %v", err)
	}
}
