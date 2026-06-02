package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/store"
)

func (s *Server) generationContentSnapshots(ctx context.Context, session store.Session, details store.RuntimeGenerationDetails) ([]store.ContentSnapshotRecord, error) {
	driverID, err := agents.CanonicalDriverID(details.DriverID)
	if err != nil {
		return nil, err
	}
	mode := strings.TrimSpace(session.Mode)
	if mode == "" {
		return nil, fmt.Errorf("session mode is required")
	}
	deployment, capabilityErr := s.resolveDriverDeployment(mode, driverID)
	if capabilityErr != nil {
		return nil, capabilityErr
	}
	policy := agents.DefaultFeaturePolicyForDriver(deployment.DriverSpec)
	return s.selectGenerationContentSnapshots(ctx, policy)
}

type contentSnapshotFeatureRequirement struct {
	feature agents.FeatureID
	kind    string
}

var contentSnapshotFeatureRequirements = []contentSnapshotFeatureRequirement{
	{feature: agents.FeatureSkillsSnapshot, kind: store.ContentSnapshotKindSkills},
	{feature: agents.FeatureManagedSettings, kind: store.ContentSnapshotKindManagedSettings},
}

func (s *Server) selectGenerationContentSnapshots(ctx context.Context, policy agents.FeaturePolicy) ([]store.ContentSnapshotRecord, error) {
	selected := []store.ContentSnapshotRecord{}
	for _, requirement := range contentSnapshotFeatureRequirements {
		if policy[requirement.feature] != agents.FeaturePolicyRequired {
			continue
		}
		records, err := s.store.ListContentSnapshots(ctx, requirement.kind)
		if err != nil {
			return nil, err
		}
		switch len(records) {
		case 0:
			return nil, fmt.Errorf("content snapshot selection for required feature %s has no %s snapshot", requirement.feature, requirement.kind)
		case 1:
			selected = append(selected, records[0])
		default:
			return nil, fmt.Errorf("content snapshot selection for required feature %s is ambiguous: %d %s snapshots", requirement.feature, len(records), requirement.kind)
		}
	}
	return selected, nil
}

func (s *Server) generationContentSnapshotsForStart(ctx context.Context, session store.Session, details store.RuntimeGenerationDetails, isNew bool) ([]store.ContentSnapshotRecord, error) {
	if isNew {
		return s.generationContentSnapshots(ctx, session, details)
	}
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, strings.TrimSpace(details.GenerationID))
	if err != nil {
		return nil, err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return nil, err
	}
	return s.generationPlanContentSnapshotRecords(ctx, plan.CanonicalPayload)
}

func (s *Server) generationPlanContentSnapshotDigests(ctx context.Context, payload []byte) (map[string]string, error) {
	records, err := s.generationPlanContentSnapshotRecords(ctx, payload)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, record := range records {
		if err := verifyContentSnapshotDigest(record); err != nil {
			return nil, err
		}
		out[record.Kind] = record.Digest
	}
	return out, nil
}

func (s *Server) generationPlanContentSnapshotRecords(ctx context.Context, payload []byte) ([]store.ContentSnapshotRecord, error) {
	refs := generationplan.ContentSnapshotReferences(payload)
	kinds := make([]string, 0, len(refs))
	for kind := range refs {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	records := make([]store.ContentSnapshotRecord, 0, len(kinds))
	for _, kind := range kinds {
		ref := refs[kind]
		record, err := s.store.GetContentSnapshot(ctx, kind, ref.Digest)
		if err != nil {
			return nil, fmt.Errorf("generation plan content snapshot %s: %w", kind, err)
		}
		if err := verifyGenerationPlanContentSnapshotRef(kind, ref, record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func verifyGenerationPlanContentSnapshotRef(kind string, ref generationplan.ContentSnapshotRef, record store.ContentSnapshotRecord) error {
	checks := []struct {
		field string
		got   string
		want  string
	}{
		{"kind", ref.Kind, record.Kind},
		{"digest", ref.Digest, record.Digest},
		{"immutable_host_path", ref.ImmutableHostPath, record.ImmutableHostPath},
		{"mount_destination", ref.MountDestination, record.MountDestination},
		{"source_evidence_digest", ref.SourceEvidenceDigest, record.SourceEvidenceDigest},
		{"retention_class", ref.RetentionClass, record.RetentionClass},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.got) != strings.TrimSpace(check.want) {
			return fmt.Errorf("generation plan content snapshot %s %s mismatch: got %s want %s", kind, check.field, check.got, check.want)
		}
	}
	return nil
}

type contentSnapshotDigestEntry struct {
	Path   string `json:"path"`
	Type   string `json:"type"`
	Mode   string `json:"mode,omitempty"`
	Size   int64  `json:"size,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	Target string `json:"target,omitempty"`
}

func verifyContentSnapshotDigest(record store.ContentSnapshotRecord) error {
	digest, err := contentSnapshotPathDigest(record.ImmutableHostPath)
	if err != nil {
		return fmt.Errorf("content snapshot %s digest: %w", record.Kind, err)
	}
	if digest != record.Digest {
		return fmt.Errorf("content snapshot %s digest mismatch: got %s want %s", record.Kind, digest, record.Digest)
	}
	return nil
}

func contentSnapshotPathDigest(root string) (string, error) {
	if strings.TrimSpace(root) != root || root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", fmt.Errorf("immutable host path %q must be canonical absolute", root)
	}
	entries := []contentSnapshotDigestEntry{}
	if err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			rel = ""
		} else {
			rel = filepath.ToSlash(rel)
		}
		entry := contentSnapshotDigestEntry{
			Path: rel,
			Mode: fmt.Sprintf("%#o", info.Mode().Perm()),
		}
		mode := info.Mode()
		switch {
		case mode.IsDir():
			entry.Type = "dir"
		case mode.IsRegular():
			entry.Type = "file"
			entry.Size = info.Size()
			digest, err := contentSnapshotFileDigest(path)
			if err != nil {
				return err
			}
			entry.SHA256 = digest
		case mode&os.ModeSymlink != 0:
			entry.Type = "symlink"
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			entry.Target = target
		default:
			return fmt.Errorf("unsupported file type at %s", path)
		}
		entries = append(entries, entry)
		return nil
	}); err != nil {
		return "", err
	}
	canonical, err := store.CanonicalSandboxContractPayload(map[string]any{
		"version": 1,
		"entries": entries,
	})
	if err != nil {
		return "", err
	}
	return store.SandboxContractDigest(canonical), nil
}

func contentSnapshotFileDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil)), nil
}
