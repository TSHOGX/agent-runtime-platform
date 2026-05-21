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
	root  string
	store *store.Store
	hub   *events.Hub
	log   *slog.Logger
}

func New(root string, store *store.Store, hub *events.Hub, log *slog.Logger) *Watcher {
	return &Watcher{root: root, store: store, hub: hub, log: log}
}

func (w *Watcher) Run(ctx context.Context) error {
	if err := os.MkdirAll(w.root, 0o755); err != nil {
		return err
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	if err := w.addRecursive(watcher, w.root); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-watcher.Errors:
			if err != nil {
				w.log.Warn("artifact watcher error", "error", err)
			}
		case event := <-watcher.Events:
			if event.Has(fsnotify.Create) {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					_ = w.addRecursive(watcher, event.Name)
					continue
				}
			}
			if event.Has(fsnotify.Create) || event.Has(fsnotify.Write) || event.Has(fsnotify.Rename) {
				w.recordPath(ctx, event.Name)
			}
		}
	}
}

func (w *Watcher) ScanSession(ctx context.Context, sessionID string) error {
	return filepath.WalkDir(filepath.Join(w.root, sessionID), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		w.recordPath(ctx, path)
		return nil
	})
}

func (w *Watcher) addRecursive(watcher *fsnotify.Watcher, root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || !entry.IsDir() {
			return nil
		}
		if err := watcher.Add(path); err != nil {
			w.log.Warn("failed to watch artifact directory", "path", path, "error", err)
		}
		return nil
	})
}

func (w *Watcher) recordPath(ctx context.Context, path string) {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return
	}
	rel, err := filepath.Rel(w.root, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return
	}
	parts := strings.SplitN(rel, string(filepath.Separator), 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return
	}
	artifact := store.Artifact{
		SessionID: parts[0],
		Path:      filepath.ToSlash(parts[1]),
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
