//go:build phase7lab

package bridge

import (
	"os"
	"strings"
	"testing"
)

func TestPhase7BridgeDurabilityLabReadsSandboxFsyncedMessage(t *testing.T) {
	root := strings.TrimSpace(os.Getenv("PHASE7_BRIDGE_LAB_DIR"))
	if root == "" {
		t.Skip("PHASE7_BRIDGE_LAB_DIR is required for the Phase 7 gVisor durability lab")
	}
	outbox, err := OpenQueue(root, OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	files, err := outbox.ReadAll()
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("outbox files=%d want 1", len(files))
	}
	file := files[0]
	if file.Seq != 1 {
		t.Fatalf("outbox sequence=%d want 1", file.Seq)
	}
	if file.Envelope.Type != TypeHeartbeat ||
		file.Envelope.SessionID != "sess_phase7_lab" ||
		file.Envelope.GenerationID != "gen_phase7_lab" ||
		!strings.HasPrefix(file.Envelope.MessageID, "phase7-durability-") {
		t.Fatalf("unexpected durability envelope: %+v", file.Envelope)
	}
	if err := file.Unlink(); err != nil {
		t.Fatalf("unlink processed outbox file: %v", err)
	}
	remaining, err := outbox.ReadAll()
	if err != nil {
		t.Fatalf("read outbox after unlink: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("outbox replayed after unlink: %+v", remaining)
	}
}
