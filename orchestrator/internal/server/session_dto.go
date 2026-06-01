package server

import (
	"time"

	"harness-platform/orchestrator/internal/store"
)

type apiSession struct {
	ID                    string     `json:"id"`
	UserID                string     `json:"user_id"`
	Status                string     `json:"status"`
	Mode                  string     `json:"mode"`
	ModeLabel             string     `json:"mode_label"`
	RestoreMS             *int64     `json:"restore_ms,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
	ExpiresAt             *time.Time `json:"expires_at,omitempty"`
	EndedAt               *time.Time `json:"ended_at,omitempty"`
	LastActivityAt        *time.Time `json:"last_activity_at,omitempty"`
	AutoCheckpointEnabled bool       `json:"auto_checkpoint_enabled"`
	FailureReason         string     `json:"failure_reason,omitempty"`
	ErrorClass            string     `json:"error_class,omitempty"`
}

func publicSession(session store.Session) apiSession {
	mode := session.Mode
	if mode == "" {
		mode = store.ModeForDriver(session.DriverID)
	}
	return apiSession{
		ID:                    session.ID,
		UserID:                session.UserID,
		Status:                session.Status,
		Mode:                  mode,
		ModeLabel:             modeLabel(mode),
		RestoreMS:             session.RestoreMS,
		CreatedAt:             session.CreatedAt,
		UpdatedAt:             session.UpdatedAt,
		ExpiresAt:             session.ExpiresAt,
		EndedAt:               session.EndedAt,
		LastActivityAt:        session.LastActivityAt,
		AutoCheckpointEnabled: session.AutoCheckpointEnabled,
		FailureReason:         session.FailureReason,
		ErrorClass:            session.ErrorClass,
	}
}

func modeLabel(mode string) string {
	switch mode {
	case "shell":
		return "Shell"
	default:
		return "Agent"
	}
}

func publicSessions(sessions []store.Session) []apiSession {
	items := make([]apiSession, 0, len(sessions))
	for _, session := range sessions {
		items = append(items, publicSession(session))
	}
	return items
}
