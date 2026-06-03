package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
)

type runtimeResourceIdentityPayload struct {
	HostID                 string            `json:"host_id"`
	SessionID              string            `json:"session_id"`
	GenerationID           string            `json:"generation_id"`
	ContractID             string            `json:"contract_id"`
	SandboxContractVersion string            `json:"sandbox_contract_version"`
	RunscContainerID       string            `json:"runsc_container_id"`
	RunscPlatform          string            `json:"runsc_platform"`
	RunscVersion           string            `json:"runsc_version"`
	RunscBinaryPath        string            `json:"runsc_binary_path"`
	RunscBinaryDigest      string            `json:"runsc_binary_digest"`
	NetworkProfileID       string            `json:"network_profile_id"`
	NetnsName              string            `json:"netns_name"`
	NetnsPath              string            `json:"netns_path"`
	HostVeth               string            `json:"host_veth"`
	SandboxVeth            string            `json:"sandbox_veth"`
	HostGatewayIP          string            `json:"host_gateway_ip"`
	SandboxIP              string            `json:"sandbox_ip"`
	SandboxIPCIDR          string            `json:"sandbox_ip_cidr"`
	HostSideCIDR           string            `json:"host_side_cidr"`
	NftTableName           string            `json:"nft_table_name"`
	ControlDirPath         string            `json:"control_dir_path"`
	ControlManifestPath    string            `json:"control_manifest_path"`
	BundleDirPath          string            `json:"bundle_dir_path"`
	SpecPath               string            `json:"spec_path"`
	CheckpointPath         string            `json:"checkpoint_path,omitempty"`
	BridgeDirPath          string            `json:"bridge_dir_path"`
	NetworkHostsPath       string            `json:"network_hosts_path,omitempty"`
	LogDirPath             string            `json:"log_dir_path"`
	RootPrefixes           map[string]string `json:"root_prefixes"`
}

func RuntimeResourceIdentityForParams(p RuntimeResourceInstanceParams) ([]byte, string, error) {
	p.SandboxContractVersion = strings.TrimSpace(p.SandboxContractVersion)
	if err := validateRuntimeResourceInstanceParams(p); err != nil {
		return nil, "", err
	}
	return runtimeResourceIdentity(p)
}

func validateRuntimeResourceInstanceParams(p RuntimeResourceInstanceParams) error {
	required := map[string]string{
		"generation id":            p.GenerationID,
		"session id":               p.SessionID,
		"contract id":              p.ContractID,
		"sandbox contract version": p.SandboxContractVersion,
		"host id":                  p.HostID,
		"runsc container id":       p.RunscContainerID,
		"runsc platform":           p.RunscPlatform,
		"runsc version":            p.RunscVersion,
		"runsc binary path":        p.RunscBinaryPath,
		"runsc binary digest":      p.RunscBinaryDigest,
		"network profile id":       p.NetworkProfileID,
		"netns name":               p.NetnsName,
		"netns path":               p.NetnsPath,
		"host veth":                p.HostVeth,
		"sandbox veth":             p.SandboxVeth,
		"host gateway ip":          p.HostGatewayIP,
		"sandbox ip":               p.SandboxIP,
		"sandbox ip cidr":          p.SandboxIPCIDR,
		"host side cidr":           p.HostSideCIDR,
		"nft table name":           p.NftTableName,
		"control dir path":         p.ControlDirPath,
		"control manifest path":    p.ControlManifestPath,
		"bundle dir path":          p.BundleDirPath,
		"spec path":                p.SpecPath,
		"bridge dir path":          p.BridgeDirPath,
		"log dir path":             p.LogDirPath,
	}
	for label, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("runtime resource %s is required", label)
		}
	}
	if p.SandboxContractVersion != SandboxContractVersion {
		return fmt.Errorf("unsupported runtime resource sandbox contract version %q", p.SandboxContractVersion)
	}
	requiredPaths := []struct {
		label string
		path  string
	}{
		{"runsc binary path", p.RunscBinaryPath},
		{"netns path", p.NetnsPath},
		{"control dir path", p.ControlDirPath},
		{"control manifest path", p.ControlManifestPath},
		{"bundle dir path", p.BundleDirPath},
		{"spec path", p.SpecPath},
		{"bridge dir path", p.BridgeDirPath},
		{"log dir path", p.LogDirPath},
	}
	for _, field := range requiredPaths {
		if !runtimeResourceInstancePathIsCanonical(field.path) {
			return fmt.Errorf("runtime resource %s must be canonical absolute", field.label)
		}
	}
	optionalPaths := []struct {
		label string
		path  string
	}{
		{"checkpoint path", p.CheckpointPath},
		{"network hosts path", p.NetworkHostsPath},
	}
	for _, field := range optionalPaths {
		if field.path == "" {
			continue
		}
		if !runtimeResourceInstancePathIsCanonical(field.path) {
			return fmt.Errorf("runtime resource %s must be canonical absolute", field.label)
		}
	}
	if err := validateRuntimeResourceRootPrefixes(p.RootPrefixes, "runtime resource"); err != nil {
		return err
	}
	return nil
}

func runtimeResourceInstancePathIsCanonical(path string) bool {
	return strings.TrimSpace(path) == path && filepath.IsAbs(path) && filepath.Clean(path) == path
}

func runtimeResourceIdentity(p RuntimeResourceInstanceParams) ([]byte, string, error) {
	payload := runtimeResourceIdentityPayload{
		HostID:                 strings.TrimSpace(p.HostID),
		SessionID:              strings.TrimSpace(p.SessionID),
		GenerationID:           strings.TrimSpace(p.GenerationID),
		ContractID:             strings.TrimSpace(p.ContractID),
		SandboxContractVersion: strings.TrimSpace(p.SandboxContractVersion),
		RunscContainerID:       strings.TrimSpace(p.RunscContainerID),
		RunscPlatform:          strings.TrimSpace(p.RunscPlatform),
		RunscVersion:           strings.TrimSpace(p.RunscVersion),
		RunscBinaryPath:        strings.TrimSpace(p.RunscBinaryPath),
		RunscBinaryDigest:      strings.TrimSpace(p.RunscBinaryDigest),
		NetworkProfileID:       strings.TrimSpace(p.NetworkProfileID),
		NetnsName:              strings.TrimSpace(p.NetnsName),
		NetnsPath:              strings.TrimSpace(p.NetnsPath),
		HostVeth:               strings.TrimSpace(p.HostVeth),
		SandboxVeth:            strings.TrimSpace(p.SandboxVeth),
		HostGatewayIP:          strings.TrimSpace(p.HostGatewayIP),
		SandboxIP:              strings.TrimSpace(p.SandboxIP),
		SandboxIPCIDR:          strings.TrimSpace(p.SandboxIPCIDR),
		HostSideCIDR:           strings.TrimSpace(p.HostSideCIDR),
		NftTableName:           strings.TrimSpace(p.NftTableName),
		ControlDirPath:         strings.TrimSpace(p.ControlDirPath),
		ControlManifestPath:    strings.TrimSpace(p.ControlManifestPath),
		BundleDirPath:          strings.TrimSpace(p.BundleDirPath),
		SpecPath:               strings.TrimSpace(p.SpecPath),
		CheckpointPath:         strings.TrimSpace(p.CheckpointPath),
		BridgeDirPath:          strings.TrimSpace(p.BridgeDirPath),
		NetworkHostsPath:       strings.TrimSpace(p.NetworkHostsPath),
		LogDirPath:             strings.TrimSpace(p.LogDirPath),
		RootPrefixes:           sortedStringMap(p.RootPrefixes),
	}
	data, err := canonicalDataVolumeJSON(payload)
	if err != nil {
		return nil, "", err
	}
	return data, SandboxContractDigest(data), nil
}

func verifyRuntimeResourceIdentityPayload(instance RuntimeResourceInstance) (runtimeResourceIdentityPayload, error) {
	canonical, err := canonicalDataVolumeJSONBytes(instance.ResourceIdentityPayload)
	if err != nil {
		return runtimeResourceIdentityPayload{}, err
	}
	if !bytes.Equal(canonical, instance.ResourceIdentityPayload) {
		return runtimeResourceIdentityPayload{}, fmt.Errorf("runtime resource identity payload is not canonical")
	}
	if got := SandboxContractDigest(instance.ResourceIdentityPayload); got != instance.ResourceIdentityDigest {
		return runtimeResourceIdentityPayload{}, fmt.Errorf("runtime resource identity digest mismatch: got %s want %s", got, instance.ResourceIdentityDigest)
	}
	var payload runtimeResourceIdentityPayload
	if err := json.Unmarshal(instance.ResourceIdentityPayload, &payload); err != nil {
		return runtimeResourceIdentityPayload{}, err
	}
	if err := validateRuntimeResourceIdentityPayloadPaths(payload); err != nil {
		return runtimeResourceIdentityPayload{}, err
	}
	return payload, nil
}

func validateRuntimeResourceIdentityPayloadPaths(payload runtimeResourceIdentityPayload) error {
	requiredPaths := []struct {
		label string
		path  string
	}{
		{"runsc binary path", payload.RunscBinaryPath},
		{"netns path", payload.NetnsPath},
		{"control dir path", payload.ControlDirPath},
		{"control manifest path", payload.ControlManifestPath},
		{"bundle dir path", payload.BundleDirPath},
		{"spec path", payload.SpecPath},
		{"bridge dir path", payload.BridgeDirPath},
		{"log dir path", payload.LogDirPath},
	}
	for _, field := range requiredPaths {
		if !runtimeResourceInstancePathIsCanonical(field.path) {
			return fmt.Errorf("runtime resource identity %s must be canonical absolute", field.label)
		}
	}
	optionalPaths := []struct {
		label string
		path  string
	}{
		{"checkpoint path", payload.CheckpointPath},
		{"network hosts path", payload.NetworkHostsPath},
	}
	for _, field := range optionalPaths {
		if field.path == "" {
			continue
		}
		if !runtimeResourceInstancePathIsCanonical(field.path) {
			return fmt.Errorf("runtime resource identity %s must be canonical absolute", field.label)
		}
	}
	if err := validateRuntimeResourceRootPrefixes(payload.RootPrefixes, "runtime resource identity"); err != nil {
		return err
	}
	return nil
}

func validateRuntimeResourceRootPrefixes(prefixes map[string]string, scope string) error {
	seen := map[string]struct{}{}
	for key, path := range prefixes {
		name := strings.TrimSpace(key)
		if name == "" {
			return fmt.Errorf("%s root prefix key is required", scope)
		}
		if name != key {
			return fmt.Errorf("%s root prefix %q key must not contain surrounding whitespace", scope, name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("%s root prefix %q is duplicated", scope, name)
		}
		seen[name] = struct{}{}
		if !runtimeResourceInstancePathIsCanonical(path) {
			return fmt.Errorf("%s root prefix %s must be canonical absolute", scope, name)
		}
	}
	return nil
}

func verifyRuntimeResourceIdentity(instance RuntimeResourceInstance) error {
	payload, err := verifyRuntimeResourceIdentityPayload(instance)
	if err != nil {
		return err
	}
	if payload.HostID != instance.HostID ||
		payload.SessionID != instance.SessionID ||
		payload.GenerationID != instance.GenerationID ||
		payload.ContractID != instance.ContractID ||
		payload.SandboxContractVersion != instance.SandboxContractVersion ||
		payload.RunscContainerID != instance.RunscContainerID ||
		payload.RunscPlatform != instance.RunscPlatform ||
		payload.RunscVersion != instance.RunscVersion ||
		payload.RunscBinaryPath != instance.RunscBinaryPath ||
		payload.RunscBinaryDigest != instance.RunscBinaryDigest ||
		payload.NetworkProfileID != instance.NetworkProfileID ||
		payload.NetnsName != instance.NetnsName ||
		payload.NetnsPath != instance.NetnsPath ||
		payload.HostVeth != instance.HostVeth ||
		payload.SandboxVeth != instance.SandboxVeth ||
		payload.HostGatewayIP != instance.HostGatewayIP ||
		payload.SandboxIP != instance.SandboxIP ||
		payload.SandboxIPCIDR != instance.SandboxIPCIDR ||
		payload.HostSideCIDR != instance.HostSideCIDR ||
		payload.NftTableName != instance.NftTableName ||
		payload.ControlDirPath != instance.ControlDirPath ||
		payload.ControlManifestPath != instance.ControlManifestPath ||
		payload.BundleDirPath != instance.BundleDirPath ||
		payload.SpecPath != instance.SpecPath ||
		payload.CheckpointPath != instance.CheckpointPath ||
		payload.BridgeDirPath != instance.BridgeDirPath ||
		payload.NetworkHostsPath != instance.NetworkHostsPath ||
		payload.LogDirPath != instance.LogDirPath {
		return fmt.Errorf("runtime resource identity payload does not match row mirrors")
	}
	return nil
}

func runtimeResourceInstanceFromIdentityPayload(instance RuntimeResourceInstance, payload runtimeResourceIdentityPayload) RuntimeResourceInstance {
	instance.HostID = payload.HostID
	instance.SessionID = payload.SessionID
	instance.GenerationID = payload.GenerationID
	instance.ContractID = payload.ContractID
	instance.SandboxContractVersion = payload.SandboxContractVersion
	instance.RunscContainerID = payload.RunscContainerID
	instance.RunscPlatform = payload.RunscPlatform
	instance.RunscVersion = payload.RunscVersion
	instance.RunscBinaryPath = payload.RunscBinaryPath
	instance.RunscBinaryDigest = payload.RunscBinaryDigest
	instance.NetworkProfileID = payload.NetworkProfileID
	instance.NetnsName = payload.NetnsName
	instance.NetnsPath = payload.NetnsPath
	instance.HostVeth = payload.HostVeth
	instance.SandboxVeth = payload.SandboxVeth
	instance.HostGatewayIP = payload.HostGatewayIP
	instance.SandboxIP = payload.SandboxIP
	instance.SandboxIPCIDR = payload.SandboxIPCIDR
	instance.HostSideCIDR = payload.HostSideCIDR
	instance.NftTableName = payload.NftTableName
	instance.ControlDirPath = payload.ControlDirPath
	instance.ControlManifestPath = payload.ControlManifestPath
	instance.BundleDirPath = payload.BundleDirPath
	instance.SpecPath = payload.SpecPath
	instance.CheckpointPath = payload.CheckpointPath
	instance.BridgeDirPath = payload.BridgeDirPath
	instance.NetworkHostsPath = payload.NetworkHostsPath
	instance.LogDirPath = payload.LogDirPath
	return instance
}
