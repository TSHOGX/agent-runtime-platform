package generationplan

import (
	"encoding/json"
	"strings"

	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/store"
)

type ContentSnapshotRef struct {
	Kind                 string
	Digest               string
	ImmutableHostPath    string
	MountDestination     string
	SourceEvidenceDigest string
	RetentionClass       string
}

func ContentSnapshotRefs(payload []byte) map[string]string {
	refs := ContentSnapshotReferences(payload)
	out := map[string]string{}
	for kind, ref := range refs {
		out[kind] = ref.Digest
	}
	return out
}

func ContentSnapshotReferences(payload []byte) map[string]ContentSnapshotRef {
	object, err := PayloadObject(payload)
	if err != nil {
		return nil
	}
	snapshots, ok := object["content_snapshots"].(map[string]any)
	if !ok {
		return nil
	}
	out := map[string]ContentSnapshotRef{}
	for kind, value := range snapshots {
		snapshot, ok := value.(map[string]any)
		if !ok {
			continue
		}
		ref := ContentSnapshotRef{
			Kind:                 strings.TrimSpace(stringValue(snapshot["kind"])),
			Digest:               strings.TrimSpace(stringValue(snapshot["digest"])),
			ImmutableHostPath:    strings.TrimSpace(stringValue(snapshot["immutable_host_path"])),
			MountDestination:     strings.TrimSpace(stringValue(snapshot["mount_destination"])),
			SourceEvidenceDigest: strings.TrimSpace(stringValue(snapshot["source_evidence_digest"])),
			RetentionClass:       strings.TrimSpace(stringValue(snapshot["retention_class"])),
		}
		if ref.Digest != "" {
			out[kind] = ref
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
