package generationplan

import (
	"fmt"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/store"
)

func validateContentSnapshots(object map[string]any, featurePolicy map[string]any) error {
	snapshots, err := requireObject(object, "content_snapshots")
	if err != nil {
		return err
	}
	allowed := map[string]bool{"skills": true, "managed_settings": true}
	for key := range snapshots {
		if !allowed[key] {
			return fmt.Errorf("generation plan content_snapshots.%s is unsupported", key)
		}
	}
	for _, key := range []string{"skills", "managed_settings"} {
		value, ok := snapshots[key]
		if !ok {
			return fmt.Errorf("generation plan content_snapshots.%s is required", key)
		}
		if value == nil {
			continue
		}
		snapshot, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("generation plan content_snapshots.%s must be an object or null", key)
		}
		if err := validateContentSnapshot(key, snapshot); err != nil {
			return err
		}
		switch key {
		case store.ContentSnapshotKindSkills, store.ContentSnapshotKindManagedSettings:
			if err := validateContentSnapshotMountEvidence(object, key, snapshot); err != nil {
				return err
			}
		}
	}
	if err := validateRequiredContentSnapshotSelections(featurePolicy, snapshots); err != nil {
		return err
	}
	return validateContentSnapshotMountScope(object, snapshots)
}

func validateRequiredContentSnapshotSelections(featurePolicy map[string]any, snapshots map[string]any) error {
	requirements := []struct {
		feature      agents.FeatureID
		snapshotKind string
	}{
		{feature: agents.FeatureSkillsSnapshot, snapshotKind: store.ContentSnapshotKindSkills},
		{feature: agents.FeatureManagedSettings, snapshotKind: store.ContentSnapshotKindManagedSettings},
	}
	for _, requirement := range requirements {
		if agents.FeaturePolicyState(stringField(featurePolicy, string(requirement.feature))) != agents.FeaturePolicyRequired {
			continue
		}
		value, ok := snapshots[requirement.snapshotKind]
		if !ok || value == nil {
			return fmt.Errorf("generation plan content_snapshots.%s is required by feature_policy.%s", requirement.snapshotKind, requirement.feature)
		}
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("generation plan content_snapshots.%s must be an object when feature_policy.%s is required", requirement.snapshotKind, requirement.feature)
		}
	}
	return nil
}

func validateContentSnapshot(name string, snapshot map[string]any) error {
	for _, key := range []string{"kind", "digest", "immutable_host_path", "mount_destination", "source_evidence_digest", "retention_class"} {
		if strings.TrimSpace(stringField(snapshot, key)) == "" {
			return fmt.Errorf("generation plan content_snapshots.%s.%s is required", name, key)
		}
	}
	if strings.TrimSpace(stringField(snapshot, "kind")) != name {
		return fmt.Errorf("generation plan content_snapshots.%s.kind must be %s", name, name)
	}
	for _, key := range []string{"digest", "source_evidence_digest"} {
		if !isSha256(stringField(snapshot, key)) {
			return fmt.Errorf("generation plan content_snapshots.%s.%s must be sha256", name, key)
		}
	}
	if !isCanonicalAbsolutePath(stringField(snapshot, "immutable_host_path")) {
		return fmt.Errorf("generation plan content_snapshots.%s.immutable_host_path must be canonical absolute", name)
	}
	if !isCanonicalAbsolutePath(stringField(snapshot, "mount_destination")) {
		return fmt.Errorf("generation plan content_snapshots.%s.mount_destination must be canonical absolute", name)
	}
	if destination, ok := contentSnapshotMountDestination(name); ok && stringField(snapshot, "mount_destination") != destination {
		return fmt.Errorf("generation plan content_snapshots.%s.mount_destination must be %s", name, destination)
	}
	return nil
}

func isCanonicalAbsolutePath(value string) bool {
	value = strings.TrimSpace(value)
	return filepath.IsAbs(value) && filepath.Clean(value) == value
}

func validateContentSnapshotMountEvidence(object map[string]any, kind string, snapshot map[string]any) error {
	mounts, ok := object["mounts"].(map[string]any)
	if !ok {
		return fmt.Errorf("generation plan mounts object is required for %s content snapshot", kind)
	}
	contentSnapshots, ok := mounts["content_snapshots"].(map[string]any)
	if !ok {
		return fmt.Errorf("generation plan mounts.content_snapshots is required for %s content snapshot", kind)
	}
	mount, ok := contentSnapshots[kind].(map[string]any)
	if !ok {
		return fmt.Errorf("generation plan mounts.content_snapshots.%s is required", kind)
	}
	mountName, ok := contentSnapshotMountName(kind)
	if !ok {
		return fmt.Errorf("generation plan content snapshot %s mount surface is unsupported", kind)
	}
	checks := []struct {
		field string
		got   string
		want  string
	}{
		{"mount_name", stringField(mount, "mount_name"), mountName},
		{"type", stringField(mount, "type"), "bind"},
		{"mode", stringField(mount, "mode"), "ro"},
		{"source", stringField(mount, "source"), stringField(snapshot, "immutable_host_path")},
		{"destination", stringField(mount, "destination"), stringField(snapshot, "mount_destination")},
		{"digest", stringField(mount, "digest"), stringField(snapshot, "digest")},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.got) != strings.TrimSpace(check.want) {
			return fmt.Errorf("generation plan mounts.content_snapshots.%s.%s mismatch", kind, check.field)
		}
	}
	if boolField(mount, "exact") != true {
		return fmt.Errorf("generation plan mounts.content_snapshots.%s.exact must be true", kind)
	}
	return nil
}

func contentSnapshotMountName(kind string) (string, bool) {
	switch strings.TrimSpace(kind) {
	case store.ContentSnapshotKindSkills:
		return "skills_snapshot", true
	case store.ContentSnapshotKindManagedSettings:
		return "managed_settings_snapshot", true
	default:
		return "", false
	}
}

func contentSnapshotMountDestination(kind string) (string, bool) {
	switch strings.TrimSpace(kind) {
	case store.ContentSnapshotKindSkills:
		return store.ContentSnapshotSkillsMount, true
	case store.ContentSnapshotKindManagedSettings:
		return store.ContentSnapshotManagedSettingsMount, true
	default:
		return "", false
	}
}

func validateContentSnapshotMountScope(object map[string]any, snapshots map[string]any) error {
	hasSnapshot := false
	for _, value := range snapshots {
		if _, ok := value.(map[string]any); ok {
			hasSnapshot = true
			break
		}
	}
	if !hasSnapshot {
		return nil
	}
	dataVolumes, err := requireObject(object, "data_volumes")
	if err != nil {
		return err
	}
	workspace, err := requireObject(dataVolumes, "workspace")
	if err != nil {
		return err
	}
	if stringField(workspace, "platform_content_mount_scope") != "immutable_content_snapshots" {
		return fmt.Errorf("generation plan data_volumes.workspace.platform_content_mount_scope must be immutable_content_snapshots when content snapshots are mounted")
	}
	return nil
}
