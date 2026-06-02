package server

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

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
