package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/store"
)

const runscRunningProofTimeout = 2 * time.Second
const runscRunningProofPollInterval = 25 * time.Millisecond

func (r *Runtime) runtimePostStartProof(ctx context.Context, details store.RuntimeGenerationDetails, pin runscPin, containerID string) (store.RuntimeResourcePostStartProof, error) {
	runscState, err := r.runscContainerRunningEvidence(ctx, pin.BinaryPath, containerID)
	if err != nil {
		return store.RuntimeResourcePostStartProof{}, err
	}
	ipNetns, ipLink, nft := "network_namespace:present; check=runsc_network_not_sandbox", "host_veth:present; check=runsc_network_not_sandbox", "nft_table:present; check=runsc_network_not_sandbox"
	runscNetwork, err := r.runscNetwork(details)
	if err != nil {
		return store.RuntimeResourcePostStartProof{}, err
	}
	if strings.EqualFold(runscNetwork, "sandbox") {
		ipNetns, err = r.netnsPresenceEvidence(ctx, details.NetnsName)
		if err != nil {
			return store.RuntimeResourcePostStartProof{}, err
		}
		ipLink, err = r.ipLinkPresenceEvidence(ctx, details.HostVeth)
		if err != nil {
			return store.RuntimeResourcePostStartProof{}, err
		}
		tableName, err := generationNftTableName(details)
		if err != nil {
			return store.RuntimeResourcePostStartProof{}, err
		}
		nft, err = r.nftTablePresenceEvidence(ctx, tableName)
		if err != nil {
			return store.RuntimeResourcePostStartProof{}, err
		}
	}
	return store.RuntimeResourcePostStartProof{
		GenerationID:      details.GenerationID,
		RunscContainerID:  containerID,
		RunscState:        runscState,
		RunscPlatform:     pin.Platform,
		RunscVersion:      pin.Version,
		RunscBinaryPath:   pin.BinaryPath,
		RunscBinaryDigest: pin.BinaryDigest,
		IPNetns:           ipNetns,
		IPLink:            ipLink,
		NFT:               nft,
	}, nil
}

func (r *Runtime) runscContainerRunningEvidence(ctx context.Context, runscBinary, containerID string) (string, error) {
	runscBinary, err := requiredRunscBinary(runscBinary)
	if err != nil {
		return "", err
	}
	deadline := time.Now().Add(runscRunningProofTimeout)
	var lastErr error
	for {
		output, err := r.runner.CombinedOutput(ctx, runscBinary, "-root", r.cfg.RunscRoot, "state", containerID)
		trimmed := strings.TrimSpace(string(output))
		if err != nil {
			lastErr = fmt.Errorf("verify runsc container %s running: %w: %s", containerID, err, trimmed)
		} else if trimmed == "" {
			return "runsc_container:" + containerID + ":running; check=" + runscBinary + " state " + containerID, nil
		} else {
			lower := strings.ToLower(trimmed)
			if strings.Contains(trimmed, containerID) && strings.Contains(lower, "running") {
				return "runsc_container:" + containerID + ":running; check=" + runscBinary + " state " + containerID + "; output=" + trimmed, nil
			}
			lastErr = fmt.Errorf("verify runsc container %s running: unexpected state %q", containerID, trimmed)
		}
		if time.Now().After(deadline) {
			return "", lastErr
		}
		timer := time.NewTimer(runscRunningProofPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", fmt.Errorf("verify runsc container %s running: %w", containerID, ctx.Err())
		case <-timer.C:
		}
	}
}

func (r *Runtime) netnsPresenceEvidence(ctx context.Context, netnsName string) (string, error) {
	netnsName = strings.TrimSpace(netnsName)
	if netnsName == "" {
		return "", fmt.Errorf("verify network namespace presence: netns name is required")
	}
	output, err := r.runner.CombinedOutput(ctx, "ip", "netns", "list")
	if err != nil {
		return "", fmt.Errorf("verify network namespace %s presence: %w: %s", netnsName, err, strings.TrimSpace(string(output)))
	}
	if !netnsListContains(string(output), netnsName) {
		return "", fmt.Errorf("verify network namespace %s presence: namespace not found", netnsName)
	}
	return "netns:present; check=ip netns list " + netnsName, nil
}

func (r *Runtime) ipLinkPresenceEvidence(ctx context.Context, linkName string) (string, error) {
	linkName = strings.TrimSpace(linkName)
	if linkName == "" {
		return "", fmt.Errorf("verify host veth presence: link name is required")
	}
	output, err := r.runner.CombinedOutput(ctx, "ip", "link", "show", linkName)
	if err != nil {
		return "", fmt.Errorf("verify host veth %s presence: %w: %s", linkName, err, strings.TrimSpace(string(output)))
	}
	return "host_veth:present; check=ip link show " + linkName + "; output=" + strings.TrimSpace(string(output)), nil
}

func (r *Runtime) nftTablePresenceEvidence(ctx context.Context, tableName string) (string, error) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return "", fmt.Errorf("verify nft table presence: table name is required")
	}
	output, err := r.runner.CombinedOutput(ctx, "nft", "list", "table", "inet", tableName)
	if err != nil {
		return "", fmt.Errorf("verify nft table %s presence: %w: %s", tableName, err, strings.TrimSpace(string(output)))
	}
	return "nft_table:present; check=nft list table inet " + tableName + "; output=" + strings.TrimSpace(string(output)), nil
}
