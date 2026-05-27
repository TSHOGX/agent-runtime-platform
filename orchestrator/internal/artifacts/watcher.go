package artifacts

import (
	"context"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/store"
)

type Watcher struct {
	volumeConfig store.DataVolumeProvisionerConfig
	store        *store.Store
	hub          *events.Hub
	log          *slog.Logger
}

func New(volumeConfig store.DataVolumeProvisionerConfig, store *store.Store, hub *events.Hub, log *slog.Logger) *Watcher {
	return &Watcher{volumeConfig: volumeConfig, store: store, hub: hub, log: log}
}

func (w *Watcher) Run(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	watchedRoots := map[string]struct{}{}
	if err := w.refreshWorkspaceWatches(ctx, watcher, watchedRoots); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.refreshWorkspaceWatches(ctx, watcher, watchedRoots); err != nil {
				w.log.Warn("failed to refresh artifact workspace watches", "error", err)
			}
		case err := <-watcher.Errors:
			if err != nil {
				w.log.Warn("artifact watcher error", "error", err)
			}
		case event := <-watcher.Events:
			if event.Has(fsnotify.Create) {
				if info, err := os.Lstat(event.Name); err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
					if _, _, _, ok := w.resolveArtifactPath(ctx, event.Name); ok {
						_ = w.addRecursive(watcher, event.Name)
					}
					continue
				}
			}
			if event.Has(fsnotify.Create) || event.Has(fsnotify.Write) {
				w.recordPath(ctx, event.Name)
			}
			if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				w.deletePath(ctx, event.Name)
			}
		}
	}
}

func (w *Watcher) ScanSession(ctx context.Context, sessionID string) error {
	workspace, err := w.store.VerifySessionWorkspaceVolume(ctx, store.VerifySessionWorkspaceVolumeParams{
		SessionID: sessionID,
		Config:    w.volumeConfig,
	})
	if err != nil {
		return err
	}
	return filepath.WalkDir(workspace.HostPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		artifactPath, ok := workspaceRelativePath(workspace.HostPath, path)
		if !ok {
			return nil
		}
		w.recordWorkspacePath(ctx, workspace.SessionID, workspace.HostPath, artifactPath, path)
		return nil
	})
}

func (w *Watcher) refreshWorkspaceWatches(ctx context.Context, watcher *fsnotify.Watcher, watchedRoots map[string]struct{}) error {
	workspaces, err := w.store.ListVerifiedSessionWorkspaceVolumes(ctx, w.volumeConfig)
	if err != nil {
		return err
	}
	for _, workspace := range workspaces {
		if _, ok := watchedRoots[workspace.HostPath]; ok {
			continue
		}
		info, err := os.Lstat(workspace.HostPath)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return os.ErrInvalid
		}
		if err := w.addRecursive(watcher, workspace.HostPath); err != nil {
			return err
		}
		watchedRoots[workspace.HostPath] = struct{}{}
	}
	return nil
}

func (w *Watcher) addRecursive(watcher *fsnotify.Watcher, root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() || entry.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		if err := watcher.Add(path); err != nil {
			w.log.Warn("failed to watch artifact directory", "path", path, "error", err)
		}
		return nil
	})
}

func (w *Watcher) recordPath(ctx context.Context, path string) {
	sessionID, artifactPath, workspaceRoot, ok := w.resolveArtifactPath(ctx, path)
	if !ok {
		return
	}
	w.recordWorkspacePath(ctx, sessionID, workspaceRoot, artifactPath, path)
}

func (w *Watcher) recordWorkspacePath(ctx context.Context, sessionID, workspaceRoot, artifactPath, path string) {
	info, err := os.Lstat(path)
	if err != nil || info.IsDir() || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return
	}
	if hasSymlinkComponent(workspaceRoot, artifactPath) {
		return
	}
	realWorkspaceRoot, err := filepath.EvalSymlinks(workspaceRoot)
	if err != nil {
		return
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil || !pathInside(realWorkspaceRoot, realPath) {
		return
	}
	artifact := store.Artifact{
		SessionID: sessionID,
		Path:      artifactPath,
		Size:      info.Size(),
		ModTime:   info.ModTime().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := w.store.UpsertArtifact(ctx, artifact); err != nil {
		w.log.Warn("failed to record artifact", "path", path, "error", err)
		return
	}
	w.hub.Publish(events.Event{Type: "artifact.updated", SessionID: artifact.SessionID, Payload: artifact})
}

func (w *Watcher) deletePath(ctx context.Context, path string) {
	sessionID, artifactPath, _, ok := w.resolveArtifactPath(ctx, path)
	if !ok {
		return
	}
	if err := w.store.DeleteArtifactPath(ctx, sessionID, artifactPath); err != nil {
		w.log.Warn("failed to delete artifact metadata", "path", path, "error", err)
		return
	}
	w.hub.Publish(events.Event{
		Type:      "artifact.deleted",
		SessionID: sessionID,
		Payload: map[string]string{
			"session_id": sessionID,
			"path":       artifactPath,
		},
	})
}

func (w *Watcher) resolveArtifactPath(ctx context.Context, path string) (string, string, string, bool) {
	workspaces, err := w.store.ListVerifiedSessionWorkspaceVolumes(ctx, w.volumeConfig)
	if err != nil {
		w.log.Warn("failed to resolve artifact workspace", "path", path, "error", err)
		return "", "", "", false
	}
	for _, workspace := range workspaces {
		artifactPath, ok := workspaceRelativePath(workspace.HostPath, path)
		if ok {
			return workspace.SessionID, artifactPath, workspace.HostPath, true
		}
	}
	return "", "", "", false
}

func workspaceRelativePath(root, path string) (string, bool) {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

func hasSymlinkComponent(root, slashPath string) bool {
	current := root
	for _, segment := range strings.Split(slashPath, "/") {
		current = filepath.Join(current, segment)
		info, err := os.Lstat(current)
		if err != nil {
			return true
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return true
		}
	}
	return false
}

func pathInside(root, candidate string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(candidate))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
