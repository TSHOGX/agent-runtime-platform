package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimePostStartProofRecordsContainerAndNetworkEvidence(t *testing.T) {
	dir := t.TempDir()
	runscRoot := filepath.Join(dir, "runsc-root")
	details := testGenerationDetails(dir, "gen_post_start")
	details.RunscNetwork = "sandbox"
	details.NetnsName = "harness-gen-post-start"
	details.HostVeth = "hgenpsh"
	tableName := mustGenerationNftTableName(details)
	pin := runscPin{
		Platform:     "systrap",
		Version:      "runsc proof",
		BinaryPath:   "/usr/local/bin/runsc-proof",
		BinaryDigest: "sha256:runsc-proof",
	}
	containerID := details.RunscContainerID
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			pin.BinaryPath + " -root " + runscRoot + " state " + containerID: []byte(`{"id":"` + containerID + `","status":"running"}`),
			"ip netns list":                    []byte(details.NetnsName + "\n"),
			"ip link show " + details.HostVeth: []byte("42: " + details.HostVeth + ": <BROADCAST,UP>"),
			"nft list table inet " + tableName: []byte("table inet " + tableName),
		},
	}
	rt := New(Config{
		RunscRoot:     runscRoot,
		RunscNetwork:  "sandbox",
		CommandRunner: runner,
	})

	proof, err := rt.runtimePostStartProof(context.Background(), details, pin, containerID)
	if err != nil {
		t.Fatalf("runtime post-start proof: %v", err)
	}
	if proof.GenerationID != details.GenerationID ||
		proof.RunscContainerID != containerID ||
		proof.RunscPlatform != pin.Platform ||
		proof.RunscVersion != pin.Version ||
		proof.RunscBinaryPath != pin.BinaryPath ||
		proof.RunscBinaryDigest != pin.BinaryDigest {
		t.Fatalf("post-start proof missing runsc identity: %+v", proof)
	}
	for label, value := range map[string]string{
		"runsc":  proof.RunscState,
		"netns":  proof.IPNetns,
		"iplink": proof.IPLink,
		"nft":    proof.NFT,
	} {
		if strings.TrimSpace(value) == "" {
			t.Fatalf("post-start proof missing %s evidence: %+v", label, proof)
		}
	}
	if !strings.Contains(proof.RunscState, containerID) || !strings.Contains(proof.RunscState, "running") {
		t.Fatalf("post-start proof did not record running container: %+v", proof)
	}
}

func TestRunscRunningEvidenceRetriesTransientStateMiss(t *testing.T) {
	dir := t.TempDir()
	runscRoot := filepath.Join(dir, "runsc-root")
	containerID := "harness-gen-gen_retry"
	runscBinary := "/usr/local/bin/runsc-proof"
	command := runscBinary + " -root " + runscRoot + " state " + containerID
	runner := &recordingCommandRunner{
		sequence: map[string][]commandResult{
			command: {
				{out: []byte("FetchSpec failed: loading container: file does not exist"), err: errors.New("exit status 128")},
				{out: []byte(`{"id":"` + containerID + `","status":"running"}`)},
			},
		},
	}
	rt := New(Config{
		RunscRoot:     runscRoot,
		CommandRunner: runner,
	})

	evidence, err := rt.runscContainerRunningEvidence(context.Background(), runscBinary, containerID)
	if err != nil {
		t.Fatalf("runsc running evidence: %v", err)
	}
	if !strings.Contains(evidence, containerID) || !strings.Contains(evidence, "running") {
		t.Fatalf("unexpected evidence: %s", evidence)
	}
	if got := runner.Commands(); len(got) != 2 || got[0] != command || got[1] != command {
		t.Fatalf("unexpected retry commands: %v", got)
	}
}
