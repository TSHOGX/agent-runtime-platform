package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const ownerHeartbeatInterval = 5 * time.Second

type OwnerLock struct {
	UUID   string
	BootID string
	RunDir string

	file *os.File
	once sync.Once
}

func AcquireOwnerLock(runDir string) (*OwnerLock, error) {
	if strings.TrimSpace(runDir) == "" {
		return nil, fmt.Errorf("run dir is required")
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return nil, fmt.Errorf("create run dir: %w", err)
	}
	path := filepath.Join(runDir, "orchestrator.pid")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open orchestrator pid file: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("orchestrator pid file %s is already locked", path)
		}
		return nil, fmt.Errorf("lock orchestrator pid file %s: %w", path, err)
	}
	if err := file.Truncate(0); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("truncate orchestrator pid file: %w", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("seek orchestrator pid file: %w", err)
	}
	if _, err := fmt.Fprintf(file, "%d\n", os.Getpid()); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("write orchestrator pid file: %w", err)
	}
	bootID, err := readBootID()
	if err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, err
	}
	uuid, err := randomHex(16)
	if err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, err
	}
	return &OwnerLock{UUID: uuid, BootID: bootID, RunDir: runDir, file: file}, nil
}

func (l *OwnerLock) Close() error {
	var err error
	l.once.Do(func() {
		if l.file == nil {
			return
		}
		err = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
		if closeErr := l.file.Close(); err == nil {
			err = closeErr
		}
	})
	return err
}

func (s *Store) WriteOwner(ctx context.Context, owner *OwnerLock) error {
	now := formatTime(time.Now().UTC())
	_, err := s.db.ExecContext(ctx, `
INSERT INTO orchestrator_owner (singleton, uuid, boot_id, host_run_dir, acquired_at, heartbeat_at)
VALUES (1, ?, ?, ?, ?, ?)
ON CONFLICT(singleton) DO UPDATE SET
  uuid = excluded.uuid,
  boot_id = excluded.boot_id,
  host_run_dir = excluded.host_run_dir,
  acquired_at = excluded.acquired_at,
  heartbeat_at = excluded.heartbeat_at`,
		owner.UUID, owner.BootID, owner.RunDir, now, now)
	return err
}

func (s *Store) HeartbeatOwner(ctx context.Context, ownerUUID string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `
UPDATE orchestrator_owner
SET heartbeat_at = ?
WHERE singleton = 1
  AND uuid = ?`, formatTime(now), ownerUUID)
	if err != nil {
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("orchestrator owner uuid mismatch")
	}
	return nil
}

func (s *Store) AssertOwner(ctx context.Context, ownerUUID string) error {
	var got string
	err := s.db.QueryRowContext(ctx, `SELECT uuid FROM orchestrator_owner WHERE singleton = 1`).Scan(&got)
	if err != nil {
		return err
	}
	if got != ownerUUID {
		return fmt.Errorf("orchestrator owner uuid mismatch: db=%s process=%s", got, ownerUUID)
	}
	return nil
}

func (s *Store) StartOwnerHeartbeat(ctx context.Context, owner *OwnerLock) <-chan error {
	errc := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(ownerHeartbeatInterval)
		defer ticker.Stop()
		defer close(errc)
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if err := s.HeartbeatOwner(ctx, owner.UUID, now.UTC()); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, sql.ErrConnDone) {
					errc <- err
					return
				}
			}
		}
	}()
	return errc
}

func readBootID() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read boot id: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

func randomHex(bytesLen int) (string, error) {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate owner uuid: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
