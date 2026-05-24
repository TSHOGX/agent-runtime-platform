package bridge

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureLayoutCreatesBridgeTransportDirs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "bridge", "gen_a")
	if err := EnsureLayout(root); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}
	for _, name := range []string{tmpDir, InboxDir, OutboxDir, HeartbeatDir} {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if !info.IsDir() {
			t.Fatalf("%s is not a directory", name)
		}
	}
}

func TestQueueWriteReadAndUnlinkUsesOrderedSequenceFiles(t *testing.T) {
	root := t.TempDir()
	queue, err := OpenQueue(root, OutboxDir)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	payload := json.RawMessage(`{"status":"ok"}`)
	first, err := queue.Write(context.Background(), Envelope{
		RequestID:    "req_a",
		Type:         "hello",
		SessionID:    "sess_a",
		GenerationID: "gen_a",
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("write first: %v", err)
	}
	second, err := queue.Write(context.Background(), Envelope{
		RequestID:    "req_b",
		Type:         "heartbeat",
		SessionID:    "sess_a",
		GenerationID: "gen_a",
	})
	if err != nil {
		t.Fatalf("write second: %v", err)
	}
	if first != 1 || second != 2 {
		t.Fatalf("unexpected seqs: first=%d second=%d", first, second)
	}
	if _, err := os.Stat(filepath.Join(root, OutboxDir, "00000000000000000001.json")); err != nil {
		t.Fatalf("first seq file missing: %v", err)
	}

	files, err := queue.ReadAll()
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(files) != 2 || files[0].Seq != 1 || files[1].Seq != 2 {
		t.Fatalf("unexpected files: %+v", files)
	}
	if files[0].Envelope.MessageID == "" || files[0].Envelope.RequestID != "req_a" || !sameJSON(files[0].Envelope.Payload, payload) {
		t.Fatalf("unexpected first envelope: %+v", files[0].Envelope)
	}
	if err := files[0].Unlink(); err != nil {
		t.Fatalf("unlink first: %v", err)
	}
	files, err = queue.ReadAll()
	if err != nil {
		t.Fatalf("read after unlink: %v", err)
	}
	if len(files) != 1 || files[0].Seq != 2 {
		t.Fatalf("unexpected files after unlink: %+v", files)
	}
}

func sameJSON(a, b json.RawMessage) bool {
	var av any
	var bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	encodedA, err := json.Marshal(av)
	if err != nil {
		return false
	}
	encodedB, err := json.Marshal(bv)
	if err != nil {
		return false
	}
	return string(encodedA) == string(encodedB)
}

func TestQueueIgnoresInvalidNamesAndContinuesFromMaxSequence(t *testing.T) {
	root := t.TempDir()
	queue, err := OpenQueue(root, InboxDir)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, InboxDir, "not-a-message.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write invalid name: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, InboxDir, "00000000000000000003.json"), []byte(`{"message_id":"m","type":"hello","session_id":"s","generation_id":"g"}`), 0o644); err != nil {
		t.Fatalf("write seq 3: %v", err)
	}
	seq, err := queue.Write(context.Background(), Envelope{
		Type:         "claim_next_turn",
		SessionID:    "s",
		GenerationID: "g",
	})
	if err != nil {
		t.Fatalf("write queue: %v", err)
	}
	if seq != 4 {
		t.Fatalf("seq=%d want 4", seq)
	}
}
