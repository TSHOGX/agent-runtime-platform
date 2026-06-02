package generationplan

import (
	"encoding/json"
	"strings"

	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/store"
)

func ContentSnapshotRefs(payload []byte) map[string]string {
	object, err := PayloadObject(payload)
	if err != nil {
		return nil
	}
	snapshots, ok := object["content_snapshots"].(map[string]any)
	if !ok {
		return nil
	}
	out := map[string]string{}
	for kind, value := range snapshots {
		snapshot, ok := value.(map[string]any)
		if !ok {
			continue
		}
		digest := strings.TrimSpace(stringValue(snapshot["digest"]))
		if digest != "" {
			out[kind] = digest
		}
	}
	return out
}

func PayloadObject(payload []byte) (map[string]any, error) {
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		return nil, err
	}
	var object map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(canonical)))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	return object, nil
}

func OptionalProjectionPayloadDigest(kind, value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return planprojection.PayloadDigest(kind, value)
}
