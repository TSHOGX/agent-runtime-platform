package server

import (
	"bytes"
	"encoding/json"
	"strings"

	"harness-platform/orchestrator/internal/events"
)

func publicEvent(event events.Event) events.Event {
	event.GenerationID = ""
	event.Payload = publicEventPayload(event.Payload)
	return event
}

func publicEventPayload(payload any) any {
	if payload == nil {
		return nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil
	}
	return sanitizePublicEventValue(value)
}

func sanitizePublicEventValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		next := make(map[string]any, len(typed))
		for key, item := range typed {
			if publicEventForbiddenKey(key) {
				continue
			}
			next[key] = sanitizePublicEventValue(item)
		}
		return next
	case []any:
		next := make([]any, 0, len(typed))
		for _, item := range typed {
			next = append(next, sanitizePublicEventValue(item))
		}
		return next
	default:
		return value
	}
}

func publicEventForbiddenKey(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	switch normalized {
	case
		"generation_id",
		"active_generation_id",
		"restore_id",
		"driver_id",
		"agent",
		"driver",
		"driver_state",
		"driver_private_state",
		"data_volumes",
		"resource_identity",
		"resource_identity_payload",
		"runtime_resource_identity",
		"host_path",
		"host_paths",
		"workspace",
		"workspace_path",
		"agent_home_path",
		"checkpoint_path",
		"bundle_dir_path",
		"control_dir_path",
		"control_manifest_path",
		"bridge_dir_path",
		"log_dir_path",
		"rootfs_path",
		"network_hosts_path",
		"netns_path":
		return true
	default:
		return false
	}
}
