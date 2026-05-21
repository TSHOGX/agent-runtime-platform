package artifacts

import "testing"

func TestIsInternalArtifactPath(t *testing.T) {
	cases := map[string]bool{
		".home":           true,
		".home/config":    true,
		".home/cache/a":   true,
		"workspace.txt":   false,
		"foo/.home":       false,
		"foo/.home/bar":   false,
		"nested/file.md":  false,
		".homey/file.txt": false,
	}

	for path, want := range cases {
		if got := IsInternalArtifactPath(path); got != want {
			t.Fatalf("path %q: want %v, got %v", path, want, got)
		}
	}
}
