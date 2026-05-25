package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrProxyContextUnavailable = errors.New("proxy active context unavailable")
	ErrProxyRequestUnknown     = errors.New("proxy request unknown")
)

type StartProxyRequestParams struct {
	SandboxSourceIP string
	ProxyRequestID  string
	UpstreamModel   string
	UpstreamBaseURL string
	Now             time.Time
}

type StartProxyRequestResult struct {
	EventID         int64
	SessionID       string
	GenerationID    string
	TurnID          int64
	RequestSequence int64
	Replayed        bool
}

type FinishProxyRequestParams struct {
	ProxyRequestID             string
	ProxyConnectLatencyMS      *int64
	UpstreamFirstByteLatencyMS *int64
	UpstreamTotalLatencyMS     *int64
	RetryCount                 *int64
	TimeoutKind                string
	HTTPStatus                 *int64
	ErrorClass                 string
	Error                      string
	Now                        time.Time
}

type FinishProxyRequestResult struct {
	EventID      int64
	SessionID    string
	GenerationID string
	TurnID       int64
	EventType    string
	Replayed     bool
}

func (s *Store) StartProxyRequest(ctx context.Context, p StartProxyRequestParams) (StartProxyRequestResult, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	p.ProxyRequestID = strings.TrimSpace(p.ProxyRequestID)
	p.SandboxSourceIP = strings.TrimSpace(p.SandboxSourceIP)
	p.UpstreamModel = strings.TrimSpace(p.UpstreamModel)
	p.UpstreamBaseURL = strings.TrimSpace(p.UpstreamBaseURL)
	if p.SandboxSourceIP == "" {
		return StartProxyRequestResult{}, fmt.Errorf("sandbox source ip is required")
	}
	if p.ProxyRequestID == "" {
		return StartProxyRequestResult{}, fmt.Errorf("proxy request id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return StartProxyRequestResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if existing, ok, err := proxyStartedByRequestIDTx(ctx, tx, p.ProxyRequestID); err != nil {
		return StartProxyRequestResult{}, err
	} else if ok {
		existing.Replayed = true
		return existing, tx.Commit()
	}

	var result StartProxyRequestResult
	var nextRequestSequence int64
	if err := tx.QueryRowContext(ctx, `
SELECT c.session_id, c.generation_id, c.turn_id, c.next_request_sequence
FROM active_model_request_contexts c
JOIN turns t ON t.id = c.turn_id
  AND t.session_id = c.session_id
  AND t.generation_id = c.generation_id
JOIN runtime_generations g ON g.generation_id = c.generation_id
  AND g.session_id = c.session_id
JOIN sessions s ON s.id = c.session_id
WHERE c.sandbox_source_ip = ?
  AND c.expires_at > ?
  AND t.status = 'running'
  AND t.lease_owner = c.lease_owner
  AND t.lease_expires_at > ?
  AND g.status IN ('active','idle')
  AND g.lease_owner = c.lease_owner
  AND g.lease_expires_at > ?
  AND s.active_generation_id = c.generation_id
  AND s.status NOT IN ('failed', 'destroyed')`,
		p.SandboxSourceIP, formatTime(p.Now), formatTime(p.Now), formatTime(p.Now),
	).Scan(&result.SessionID, &result.GenerationID, &result.TurnID, &nextRequestSequence); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return StartProxyRequestResult{}, ErrProxyContextUnavailable
		}
		return StartProxyRequestResult{}, err
	}
	result.RequestSequence = nextRequestSequence

	res, err := tx.ExecContext(ctx, `
UPDATE active_model_request_contexts
SET next_request_sequence = ?,
    updated_at = ?
WHERE sandbox_source_ip = ?
  AND session_id = ?
  AND generation_id = ?
  AND turn_id = ?
  AND next_request_sequence = ?
  AND expires_at > ?`,
		nextRequestSequence+1, formatTime(p.Now), p.SandboxSourceIP, result.SessionID,
		result.GenerationID, result.TurnID, nextRequestSequence, formatTime(p.Now))
	if err != nil {
		return StartProxyRequestResult{}, err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return StartProxyRequestResult{}, err
	} else if affected != 1 {
		return StartProxyRequestResult{}, ErrProxyContextUnavailable
	}

	payload := map[string]any{
		"proxy_request_id":  p.ProxyRequestID,
		"sandbox_source_ip": p.SandboxSourceIP,
		"request_sequence":  result.RequestSequence,
		"correlation_mode":  "source_ip",
	}
	if p.UpstreamModel != "" {
		payload["upstream_model"] = p.UpstreamModel
	}
	if p.UpstreamBaseURL != "" {
		payload["upstream_base_url"] = p.UpstreamBaseURL
	}
	result.EventID, err = appendEventTx(ctx, tx, AppendEventParams{
		SessionID:      result.SessionID,
		TurnID:         &result.TurnID,
		GenerationID:   result.GenerationID,
		ProxyRequestID: p.ProxyRequestID,
		Type:           "proxy.request.started",
		Payload:        payload,
		Now:            p.Now,
	})
	if err != nil {
		return StartProxyRequestResult{}, err
	}
	if result.EventID == 0 {
		existing, ok, err := proxyStartedByRequestIDTx(ctx, tx, p.ProxyRequestID)
		if err != nil {
			return StartProxyRequestResult{}, err
		}
		if !ok {
			return StartProxyRequestResult{}, fmt.Errorf("proxy request start dedupe without existing event")
		}
		existing.Replayed = true
		return existing, tx.Commit()
	}
	return result, tx.Commit()
}

func (s *Store) FinishProxyRequest(ctx context.Context, p FinishProxyRequestParams) (FinishProxyRequestResult, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.ProxyRequestID == "" {
		return FinishProxyRequestResult{}, fmt.Errorf("proxy request id is required")
	}
	p.ProxyRequestID = strings.TrimSpace(p.ProxyRequestID)
	p.TimeoutKind = strings.TrimSpace(p.TimeoutKind)
	p.ErrorClass = strings.TrimSpace(p.ErrorClass)
	p.Error = strings.TrimSpace(p.Error)
	if p.ProxyRequestID == "" {
		return FinishProxyRequestResult{}, fmt.Errorf("proxy request id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FinishProxyRequestResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if existing, ok, err := proxyFinishedByRequestIDTx(ctx, tx, p.ProxyRequestID); err != nil {
		return FinishProxyRequestResult{}, err
	} else if ok {
		existing.Replayed = true
		return existing, tx.Commit()
	}

	started, ok, err := proxyStartedByRequestIDTx(ctx, tx, p.ProxyRequestID)
	if err != nil {
		return FinishProxyRequestResult{}, err
	}
	if !ok {
		return FinishProxyRequestResult{}, ErrProxyRequestUnknown
	}
	if err := validateProxyFinishError(p.ErrorClass, p.TimeoutKind); err != nil {
		return FinishProxyRequestResult{}, err
	}

	eventType := "proxy.request.completed"
	if p.ErrorClass != "" {
		eventType = "proxy.request.failed"
	}
	payload := map[string]any{
		"proxy_request_id": p.ProxyRequestID,
		"request_sequence": started.RequestSequence,
	}
	addIntPayload(payload, "proxy_connect_latency_ms", p.ProxyConnectLatencyMS)
	addIntPayload(payload, "upstream_first_byte_latency_ms", p.UpstreamFirstByteLatencyMS)
	addIntPayload(payload, "upstream_total_latency_ms", p.UpstreamTotalLatencyMS)
	addIntPayload(payload, "retry_count", p.RetryCount)
	addIntPayload(payload, "http_status", p.HTTPStatus)
	if p.TimeoutKind != "" {
		payload["timeout_kind"] = p.TimeoutKind
	}
	if p.ErrorClass != "" {
		payload["error_class"] = p.ErrorClass
	}
	if p.Error != "" {
		payload["error"] = p.Error
	}

	eventID, err := appendEventTx(ctx, tx, AppendEventParams{
		SessionID:      started.SessionID,
		TurnID:         &started.TurnID,
		GenerationID:   started.GenerationID,
		ProxyRequestID: p.ProxyRequestID,
		Type:           eventType,
		Payload:        payload,
		Now:            p.Now,
	})
	if err != nil {
		return FinishProxyRequestResult{}, err
	}
	if eventID == 0 {
		existing, ok, err := proxyFinishedByRequestIDTx(ctx, tx, p.ProxyRequestID)
		if err != nil {
			return FinishProxyRequestResult{}, err
		}
		if !ok {
			return FinishProxyRequestResult{}, fmt.Errorf("proxy request finish dedupe without existing event")
		}
		existing.Replayed = true
		return existing, tx.Commit()
	}
	result := FinishProxyRequestResult{
		EventID:      eventID,
		SessionID:    started.SessionID,
		GenerationID: started.GenerationID,
		TurnID:       started.TurnID,
		EventType:    eventType,
	}
	return result, tx.Commit()
}

func proxyStartedByRequestIDTx(ctx context.Context, tx *sql.Tx, proxyRequestID string) (StartProxyRequestResult, bool, error) {
	record, ok, err := proxyEventByRequestIDTx(ctx, tx, proxyRequestID, `type = 'proxy.request.started'`)
	if err != nil || !ok {
		return StartProxyRequestResult{}, ok, err
	}
	sequence, err := requestSequenceFromPayload(record.Payload)
	if err != nil {
		return StartProxyRequestResult{}, false, err
	}
	return StartProxyRequestResult{
		EventID:         record.EventID,
		SessionID:       record.SessionID,
		GenerationID:    record.GenerationID,
		TurnID:          int64Value(record.TurnID),
		RequestSequence: sequence,
	}, true, nil
}

func proxyFinishedByRequestIDTx(ctx context.Context, tx *sql.Tx, proxyRequestID string) (FinishProxyRequestResult, bool, error) {
	record, ok, err := proxyEventByRequestIDTx(ctx, tx, proxyRequestID, `type IN ('proxy.request.completed', 'proxy.request.failed')`)
	if err != nil || !ok {
		return FinishProxyRequestResult{}, ok, err
	}
	return FinishProxyRequestResult{
		EventID:      record.EventID,
		SessionID:    record.SessionID,
		GenerationID: record.GenerationID,
		TurnID:       int64Value(record.TurnID),
		EventType:    record.Type,
	}, true, nil
}

func proxyEventByRequestIDTx(ctx context.Context, tx *sql.Tx, proxyRequestID, typePredicate string) (EventRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT event_id, session_id, turn_id, generation_id, output_sequence,
  proxy_request_id, stream, severity, type, payload, created_at
FROM events
WHERE proxy_request_id = ?
  AND `+typePredicate+`
ORDER BY event_id
LIMIT 1`, proxyRequestID)
	record, err := scanEventRecord(row)
	if err == nil {
		return record, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return EventRecord{}, false, nil
	}
	return EventRecord{}, false, err
}

func requestSequenceFromPayload(payload json.RawMessage) (int64, error) {
	var value struct {
		RequestSequence int64 `json:"request_sequence"`
	}
	if err := json.Unmarshal(payload, &value); err != nil {
		return 0, err
	}
	return value.RequestSequence, nil
}

func addIntPayload(payload map[string]any, key string, value *int64) {
	if value != nil {
		payload[key] = *value
	}
}

func int64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func validateProxyFinishError(errorClass, timeoutKind string) error {
	if errorClass == "" {
		if timeoutKind != "" {
			return fmt.Errorf("timeout_kind requires error_class timeout")
		}
		return nil
	}
	switch errorClass {
	case "auth", "network", "upstream_5xx", "rate_limit", "timeout", "malformed_stream", "canceled":
	default:
		return fmt.Errorf("invalid proxy error_class %q", errorClass)
	}
	if errorClass == "timeout" {
		switch timeoutKind {
		case "connect", "first_byte", "total", "idle_stream":
			return nil
		default:
			return fmt.Errorf("timeout proxy errors require a valid timeout_kind")
		}
	}
	if timeoutKind != "" {
		return fmt.Errorf("timeout_kind requires error_class timeout")
	}
	return nil
}
