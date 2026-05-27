package server

import (
	"time"

	"harness-platform/orchestrator/internal/store"
)

type apiSession struct {
	ID                    string     `json:"id"`
	UserID                string     `json:"user_id"`
	Status                string     `json:"status"`
	Agent                 string     `json:"agent"`
	ActiveGenerationID    string     `json:"active_generation_id,omitempty"`
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
	return apiSession{
		ID:                    session.ID,
		UserID:                session.UserID,
		Status:                session.Status,
		Agent:                 session.Agent,
		ActiveGenerationID:    session.ActiveGenerationID,
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

func publicSessions(sessions []store.Session) []apiSession {
	items := make([]apiSession, 0, len(sessions))
	for _, session := range sessions {
		items = append(items, publicSession(session))
	}
	return items
}
