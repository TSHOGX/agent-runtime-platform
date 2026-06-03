package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type IsolationRoots struct {
	SessionsRoot           string
	AgentHomesRoot         string
	RunDir                 string
	PreparedBundleRoot     string
	RootFSPath             string
	DBPath                 string
	SchemaPackRoot         string
	DataVolumeEvidenceRoot string
	ProxyInternalRoot      string
	ProviderCredentialRoot string
}

type CanonicalIsolationRoots struct {
	SessionsRoot           string
	AgentHomesRoot         string
	RunDir                 string
	PreparedBundleRoot     string
	RootFSPath             string
	DBStateRoot            string
	SchemaPackRoot         string
	DataVolumeEvidenceRoot string
	ProxyInternalRoot      string
	ProviderCredentialRoot string
}

func (c Config) IsolationRoots() IsolationRoots {
	schemaPackRoot := filepath.Join(c.RepoRoot, "schema-pack")
	if _, err := os.Stat(schemaPackRoot); err != nil {
		schemaPackRoot = ""
	}
	return IsolationRoots{
		SessionsRoot:           c.SessionsRoot,
		AgentHomesRoot:         c.AgentHomesRoot,
		RunDir:                 c.Harness.RunDir,
		PreparedBundleRoot:     c.BundleRoot,
		RootFSPath:             c.RootFSPath,
		DBPath:                 c.DBPath,
		SchemaPackRoot:         schemaPackRoot,
		DataVolumeEvidenceRoot: filepath.Join(filepath.Dir(c.DBPath), "volume-evidence"),
		ProxyInternalRoot:      filepath.Join(c.Harness.RunDir, "proxy-internal"),
	}
}

func ValidateIsolationRoots(roots IsolationRoots) (CanonicalIsolationRoots, error) {
	canonical := CanonicalIsolationRoots{}
	required := []struct {
		label string
		value string
		set   func(string)
	}{
		{label: "sessions root", value: roots.SessionsRoot, set: func(path string) { canonical.SessionsRoot = path }},
		{label: "agent homes root", value: roots.AgentHomesRoot, set: func(path string) { canonical.AgentHomesRoot = path }},
		{label: "run dir", value: roots.RunDir, set: func(path string) { canonical.RunDir = path }},
		{label: "prepared bundle root", value: roots.PreparedBundleRoot, set: func(path string) { canonical.PreparedBundleRoot = path }},
		{label: "rootfs path", value: roots.RootFSPath, set: func(path string) { canonical.RootFSPath = path }},
	}
	for _, root := range required {
		path, err := canonicalIsolationRoot(root.label, root.value)
		if err != nil {
			return CanonicalIsolationRoots{}, err
		}
		root.set(path)
	}
	dbPath, err := canonicalIsolationRoot("db path", roots.DBPath)
	if err != nil {
		return CanonicalIsolationRoots{}, err
	}
	canonical.DBStateRoot = filepath.Dir(dbPath)
	if strings.TrimSpace(roots.SchemaPackRoot) != "" {
		canonical.SchemaPackRoot, err = canonicalIsolationRoot("schema pack root", roots.SchemaPackRoot)
		if err != nil {
			return CanonicalIsolationRoots{}, err
		}
	}
	if strings.TrimSpace(roots.DataVolumeEvidenceRoot) != "" {
		canonical.DataVolumeEvidenceRoot, err = canonicalIsolationRoot("data volume evidence root", roots.DataVolumeEvidenceRoot)
		if err != nil {
			return CanonicalIsolationRoots{}, err
		}
	} else {
		canonical.DataVolumeEvidenceRoot = filepath.Join(canonical.DBStateRoot, "volume-evidence")
	}
	if strings.TrimSpace(roots.ProxyInternalRoot) != "" {
		canonical.ProxyInternalRoot, err = canonicalIsolationRoot("proxy internal root", roots.ProxyInternalRoot)
		if err != nil {
			return CanonicalIsolationRoots{}, err
		}
	} else {
		canonical.ProxyInternalRoot = filepath.Join(canonical.RunDir, "proxy-internal")
	}
	if strings.TrimSpace(roots.ProviderCredentialRoot) != "" {
		canonical.ProviderCredentialRoot, err = canonicalIsolationRoot("provider credential root", roots.ProviderCredentialRoot)
		if err != nil {
			return CanonicalIsolationRoots{}, err
		}
	}

	topLevel := []isolationRoot{
		{label: "sessions root", path: canonical.SessionsRoot},
		{label: "agent homes root", path: canonical.AgentHomesRoot},
		{label: "run dir", path: canonical.RunDir},
		{label: "prepared bundle root", path: canonical.PreparedBundleRoot},
		{label: "rootfs path", path: canonical.RootFSPath},
		{label: "db state root", path: canonical.DBStateRoot},
	}
	if canonical.SchemaPackRoot != "" {
		topLevel = append(topLevel, isolationRoot{label: "schema pack root", path: canonical.SchemaPackRoot})
	}
	if canonical.ProviderCredentialRoot != "" {
		topLevel = append(topLevel, isolationRoot{label: "provider credential root", path: canonical.ProviderCredentialRoot})
	}
	if !isolationPathWithin(canonical.DataVolumeEvidenceRoot, canonical.DBStateRoot) {
		topLevel = append(topLevel, isolationRoot{label: "data volume evidence root", path: canonical.DataVolumeEvidenceRoot})
	}
	if !isolationPathWithin(canonical.ProxyInternalRoot, canonical.RunDir) {
		topLevel = append(topLevel, isolationRoot{label: "proxy internal root", path: canonical.ProxyInternalRoot})
	}
	if err := validateIsolationTopLevelDisjoint(topLevel); err != nil {
		return CanonicalIsolationRoots{}, err
	}
	if err := validateReservedHostRoot("data volume evidence root", canonical.DataVolumeEvidenceRoot, canonical, canonical.DBStateRoot); err != nil {
		return CanonicalIsolationRoots{}, err
	}
	if err := validateReservedHostRoot("proxy internal root", canonical.ProxyInternalRoot, canonical, canonical.RunDir); err != nil {
		return CanonicalIsolationRoots{}, err
	}
	return canonical, nil
}

type isolationRoot struct {
	label string
	path  string
}

func canonicalIsolationRoot(label, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("isolation %s is required", label)
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("isolation %s %q must be absolute", label, path)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("isolation %s %q must be absolute: %w", label, path, err)
	}
	cleaned := filepath.Clean(absolute)
	if cleaned == string(filepath.Separator) {
		return "", fmt.Errorf("isolation %s must not be filesystem root", label)
	}
	if resolved, err := filepath.EvalSymlinks(cleaned); err == nil {
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("resolve isolation %s %q: %w", label, cleaned, err)
	}
	existing, missing, err := deepestExistingRoot(cleaned)
	if err != nil {
		return "", fmt.Errorf("resolve isolation %s %q: %w", label, cleaned, err)
	}
	if existing == "" {
		return cleaned, nil
	}
	resolved, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", fmt.Errorf("resolve isolation %s existing prefix %q: %w", label, existing, err)
	}
	return filepath.Clean(filepath.Join(append([]string{resolved}, missing...)...)), nil
}

func deepestExistingRoot(path string) (string, []string, error) {
	var missing []string
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		if _, err := os.Lstat(current); err == nil {
			for i, j := 0, len(missing)-1; i < j; i, j = i+1, j-1 {
				missing[i], missing[j] = missing[j], missing[i]
			}
			return current, missing, nil
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", nil, err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", nil, nil
		}
		missing = append(missing, filepath.Base(current))
	}
}

func validateIsolationTopLevelDisjoint(roots []isolationRoot) error {
	for i := 0; i < len(roots); i++ {
		for j := i + 1; j < len(roots); j++ {
			if isolationRootsOverlap(roots[i].path, roots[j].path) {
				return fmt.Errorf("isolation %s %q overlaps %s %q", roots[i].label, roots[i].path, roots[j].label, roots[j].path)
			}
		}
	}
	return nil
}

func validateReservedHostRoot(label, path string, roots CanonicalIsolationRoots, allowedParent string) error {
	if !isolationPathWithin(path, allowedParent) && isolationRootsOverlap(path, allowedParent) {
		return fmt.Errorf("isolation %s %q must not contain reserved parent %q", label, path, allowedParent)
	}
	sandboxBindable := []isolationRoot{
		{label: "sessions root", path: roots.SessionsRoot},
		{label: "agent homes root", path: roots.AgentHomesRoot},
		{label: "run control root", path: filepath.Join(roots.RunDir, "control")},
		{label: "run runtime root", path: filepath.Join(roots.RunDir, "runtime")},
		{label: "run bridge root", path: filepath.Join(roots.RunDir, "bridge")},
		{label: "run network root", path: filepath.Join(roots.RunDir, "network")},
		{label: "run logs root", path: filepath.Join(roots.RunDir, "logs")},
	}
	if roots.SchemaPackRoot != "" {
		sandboxBindable = append(sandboxBindable, isolationRoot{label: "schema pack root", path: roots.SchemaPackRoot})
	}
	for _, root := range sandboxBindable {
		if isolationRootsOverlap(path, root.path) {
			return fmt.Errorf("isolation %s %q overlaps sandbox-bindable %s %q", label, path, root.label, root.path)
		}
	}
	if path == roots.RunDir || path == roots.DBStateRoot {
		return fmt.Errorf("isolation %s must be a reserved subroot, got top-level root %q", label, path)
	}
	return nil
}

func isolationRootsOverlap(a, b string) bool {
	return isolationPathWithin(a, b) || isolationPathWithin(b, a)
}

func isolationPathWithin(path, root string) bool {
	path = filepath.Clean(path)
	root = filepath.Clean(root)
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}
