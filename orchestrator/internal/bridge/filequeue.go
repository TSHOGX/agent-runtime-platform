package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	BridgeMountDestination = "/harness-control/bridge"

	InboxDir         = "inbox"
	OutboxDir        = "outbox"
	HeartbeatDir     = "heartbeat"
	HostHeartbeatDir = "host-heartbeat"
	SandboxTmpDir    = "tmp"
	HostTmpDir       = "host-tmp"
)

const (
	BridgeHeartbeatFile = "bridge"
	HostHeartbeatFile   = "host"
	CheckpointReadyFile = "checkpoint-ready"
)

const (
	TypeHello            = "hello"
	TypeHelloAck         = "hello_ack"
	TypeHeartbeat        = "heartbeat"
	TypeProbeNetwork     = "probe_network"
	TypeClaimNextTurn    = "claim_next_turn"
	TypeResumeTurn       = "resume_turn"
	TypeGrant            = "grant"
	TypeNoWork           = "no_work"
	TypeAckTurnStarted   = "ack_turn_started"
	TypeEmitOutput       = "emit_output"
	TypeAckTurnCompleted = "ack_turn_completed"
	TypeError            = "error"
)

type Envelope struct {
	MessageID    string          `json:"message_id"`
	RequestID    string          `json:"request_id,omitempty"`
	Type         string          `json:"type"`
	SessionID    string          `json:"session_id"`
	GenerationID string          `json:"generation_id"`
	TurnID       *int64          `json:"turn_id,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}

type Queue struct {
	name   string
	dir    string
	tmpDir string
}

type MessageFile struct {
	Seq      uint64
	Path     string
	Envelope Envelope
}

func EnsureLayout(root string) error {
	if strings.TrimSpace(root) == "" {
		return fmt.Errorf("bridge root is required")
	}
	for _, dir := range []string{
		root,
		filepath.Join(root, SandboxTmpDir),
		filepath.Join(root, InboxDir),
		filepath.Join(root, OutboxDir),
		filepath.Join(root, HeartbeatDir),
		filepath.Join(root, HostHeartbeatDir),
		filepath.Join(root, HostTmpDir),
		HostControlRoot(root),
		HostOwnedPath(root, InboxDir),
		HostOwnedPath(root, HostHeartbeatDir),
		HostOwnedPath(root, HostTmpDir),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if err := syncDir(HostControlRoot(root)); err != nil {
		return err
	}
	return syncDir(root)
}

func HostControlRoot(root string) string {
	return filepath.Clean(root) + ".host"
}

func HostOwnedPath(root, name string) string {
	return filepath.Join(HostControlRoot(root), name)
}

func OpenQueue(root, name string) (Queue, error) {
	if name != InboxDir && name != OutboxDir {
		return Queue{}, fmt.Errorf("unsupported bridge queue %q", name)
	}
	if err := EnsureLayout(root); err != nil {
		return Queue{}, err
	}
	queueTmpDir := filepath.Join(root, SandboxTmpDir)
	queueDir := filepath.Join(root, name)
	if name == InboxDir {
		queueDir = HostOwnedPath(root, InboxDir)
		queueTmpDir = HostOwnedPath(root, HostTmpDir)
	}
	return Queue{name: name, dir: queueDir, tmpDir: queueTmpDir}, nil
}

func (q Queue) Write(ctx context.Context, envelope Envelope) (uint64, error) {
	if strings.TrimSpace(envelope.MessageID) == "" {
		envelope.MessageID = uuid.NewString()
	}
	payload, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		return 0, err
	}
	payload = append(payload, '\n')
	if q.name == InboxDir {
		if err := q.pruneSequenceFiles(); err != nil {
			return 0, err
		}
	}
	tmpPath := filepath.Join(q.tmpDir, uuid.NewString()+".json")
	if err := writeFileDurable(tmpPath, payload, 0o644); err != nil {
		return 0, err
	}
	defer func() { _ = os.Remove(tmpPath) }()

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		next, err := q.nextSeq()
		if err != nil {
			return 0, err
		}
		target := filepath.Join(q.dir, seqFileName(next))
		if err := os.Link(tmpPath, target); err == nil {
			if err := syncDir(q.dir); err != nil {
				return 0, err
			}
			return next, nil
		} else if os.IsExist(err) {
			continue
		} else {
			return 0, err
		}
	}
}

func (q Queue) pruneSequenceFiles() error {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return err
	}
	pruned := false
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, ok := parseSeqFileName(entry.Name()); !ok {
			continue
		}
		if err := os.Remove(filepath.Join(q.dir, entry.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
		pruned = true
	}
	if pruned {
		return syncDir(q.dir)
	}
	return nil
}

func (q Queue) ReadAll() ([]MessageFile, error) {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, err
	}
	files := make([]MessageFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		seq, ok := parseSeqFileName(entry.Name())
		if !ok {
			continue
		}
		path := filepath.Join(q.dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var envelope Envelope
		if err := json.Unmarshal(data, &envelope); err != nil {
			return nil, fmt.Errorf("read bridge message %s: %w", path, err)
		}
		files = append(files, MessageFile{Seq: seq, Path: path, Envelope: envelope})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Seq < files[j].Seq
	})
	return files, nil
}

func (m MessageFile) Unlink() error {
	if strings.TrimSpace(m.Path) == "" {
		return nil
	}
	if err := os.Remove(m.Path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(filepath.Dir(m.Path))
}

func TouchHeartbeat(root, name string, now time.Time) error {
	if name != BridgeHeartbeatFile && name != HostHeartbeatFile {
		return fmt.Errorf("unsupported bridge heartbeat file %q", name)
	}
	return touchControlFile(root, heartbeatDir(name), name, now, ".heartbeat")
}

func HeartbeatPath(root, name string) string {
	if name == HostHeartbeatFile {
		return filepath.Join(HostOwnedPath(root, HostHeartbeatDir), name)
	}
	return filepath.Join(root, heartbeatDir(name), name)
}

func heartbeatDir(name string) string {
	if name == HostHeartbeatFile {
		return HostHeartbeatDir
	}
	return HeartbeatDir
}

func TouchCheckpointReady(root string, now time.Time) error {
	return touchControlFile(root, HeartbeatDir, CheckpointReadyFile, now, ".ready")
}

func ClearCheckpointReady(root string) error {
	if strings.TrimSpace(root) == "" {
		return fmt.Errorf("bridge root is required")
	}
	path := filepath.Join(root, HeartbeatDir, CheckpointReadyFile)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func touchControlFile(root, dir, name string, now time.Time, suffix string) error {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if err := EnsureLayout(root); err != nil {
		return err
	}
	controlTmpDir := SandboxTmpDir
	targetDir := filepath.Join(root, dir)
	if dir == HostHeartbeatDir {
		controlTmpDir = filepath.Join(HostControlRoot(root), HostTmpDir)
		targetDir = filepath.Join(HostControlRoot(root), HostHeartbeatDir)
	} else {
		controlTmpDir = filepath.Join(root, controlTmpDir)
	}
	tmpPath := filepath.Join(controlTmpDir, uuid.NewString()+suffix)
	payload := []byte(now.Format(time.RFC3339Nano) + "\n")
	if err := writeFileDurable(tmpPath, payload, 0o644); err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmpPath) }()
	target := filepath.Join(targetDir, name)
	if err := os.Rename(tmpPath, target); err != nil {
		return err
	}
	return syncDir(targetDir)
}

func (q Queue) nextSeq() (uint64, error) {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return 0, err
	}
	var maxSeq uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if seq, ok := parseSeqFileName(entry.Name()); ok && seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq + 1, nil
}

func seqFileName(seq uint64) string {
	return fmt.Sprintf("%020d.json", seq)
}

func parseSeqFileName(name string) (uint64, bool) {
	if len(name) != len("00000000000000000001.json") || !strings.HasSuffix(name, ".json") {
		return 0, false
	}
	raw := strings.TrimSuffix(name, ".json")
	seq, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || seq == 0 {
		return 0, false
	}
	if seqFileName(seq) != name {
		return 0, false
	}
	return seq, true
}

func writeFileDurable(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
