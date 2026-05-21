package sessionstate

import "testing"

func TestSessionStatusSemantics(t *testing.T) {
	tests := []struct {
		status      Status
		acceptInput bool
		busy        bool
		active      bool
		terminal    bool
	}{
		{Created, true, false, true, false},
		{RunningActive, false, true, true, false},
		{RunningIdle, true, false, true, false},
		{Checkpointing, false, true, true, false},
		{Checkpointed, true, false, true, false},
		{Failed, false, false, false, true},
		{Destroyed, false, false, false, true},
	}

	for _, tt := range tests {
		status := string(tt.status)
		if err := Validate(status); err != nil {
			t.Fatalf("validate %s: %v", status, err)
		}
		if got := CanAcceptInput(status); got != tt.acceptInput {
			t.Fatalf("CanAcceptInput(%s): want %v, got %v", status, tt.acceptInput, got)
		}
		if got := IsBusy(status); got != tt.busy {
			t.Fatalf("IsBusy(%s): want %v, got %v", status, tt.busy, got)
		}
		if got := IsActive(status); got != tt.active {
			t.Fatalf("IsActive(%s): want %v, got %v", status, tt.active, got)
		}
		if got := IsTerminal(status); got != tt.terminal {
			t.Fatalf("IsTerminal(%s): want %v, got %v", status, tt.terminal, got)
		}
	}
}

func TestRejectsLegacyStatuses(t *testing.T) {
	for _, status := range []string{"running", "idle", "completed"} {
		if err := Validate(status); err == nil {
			t.Fatalf("Validate(%s) should fail", status)
		}
	}
}
