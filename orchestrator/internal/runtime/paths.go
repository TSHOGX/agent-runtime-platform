package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func cleanAbsoluteRoot(path, label string) (string, error) {
	cleaned := cleanAbsolutePath(path)
	if cleaned == "" {
		return "", fmt.Errorf("%s is required and must be absolute", label)
	}
	return cleaned, nil
}

func cleanAbsolutePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) {
		return ""
	}
	return filepath.Clean(path)
}

func safePathComponent(label, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || filepath.Base(value) != value || value == "." || value == ".." {
		return "", fmt.Errorf("%s %q is not a safe path component", label, value)
	}
	return value, nil
}

func pathContainsDotDot(path string) bool {
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == ".." {
			return true
		}
	}
	return false
}

func ensurePathStaysWithinRoot(path, root string) error {
	if !pathWithinRoot(path, root) {
		return fmt.Errorf("path is outside root %q", root)
	}
	resolvedRoot := root
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		resolvedRoot = filepath.Clean(resolved)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("resolve root: %w", err)
	}

	resolvedPath, err := filepath.EvalSymlinks(path)
	if err == nil {
		if !pathWithinRoot(filepath.Clean(resolvedPath), resolvedRoot) {
			return fmt.Errorf("resolved path escapes root %q", resolvedRoot)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("resolve path: %w", err)
	}
	prefix, err := deepestExistingPath(path)
	if err != nil {
		return err
	}
	if prefix == "" {
		return nil
	}
	if !pathWithinRoot(prefix, root) {
		return nil
	}
	resolvedPrefix, err := filepath.EvalSymlinks(prefix)
	if err != nil {
		return fmt.Errorf("resolve existing prefix: %w", err)
	}
	if !pathWithinRoot(filepath.Clean(resolvedPrefix), resolvedRoot) {
		return fmt.Errorf("resolved existing prefix escapes root %q", resolvedRoot)
	}
	return nil
}

func deepestExistingPath(path string) (string, error) {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		if _, err := os.Lstat(current); err == nil {
			return current, nil
		} else if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat existing prefix %q: %w", current, err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", nil
		}
	}
}

func pathWithinRoot(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}
