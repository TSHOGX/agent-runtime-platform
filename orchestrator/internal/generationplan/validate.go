package generationplan

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

type ValidateParams struct {
	Payload any
}

type VerifyFrozenEvidenceParams struct {
	Payload                         any
	SessionID                       string
	GenerationID                    string
	DriverID                        string
	OutputFormat                    string
	DriverStateDigest               string
	DriverStateVersion              int
	NetworkProfileID                string
	AgentRuntimeProfileID           string
	RunscPlatform                   string
	RunscVersion                    string
	RunscBinaryPath                 string
	RunscBinaryDigest               string
	ProjectionDigests               map[string]string
	ProjectionVersions              map[string]int
	ContentSnapshotDigests          map[string]string
	CheckpointBundleDigest          string
	CheckpointRuntimeConfigDigest   string
	CheckpointControlManifestDigest string
	CheckpointDriverStatesDigest    string
	CheckpointPlanDigest            string
}

type VerifyDataVolumeEvidenceParams struct {
	Payload                         any
	WorkspaceHostPath               string
	WorkspaceRuntimeIdentityDigest  string
	DriverHomeHostPath              string
	DriverHomeRuntimeIdentityDigest string
}

type VerifySourceDigestEvidenceParams struct {
	Payload             any
	RuntimeConfigDigest string
	AgentManifestDigest string
	AdapterInputDigests map[string]string
}

type VerifyNetworkEvidenceParams struct {
	Payload            any
	NetworkProfileID   string
	RunscNetwork       string
	RunscOverlay2      string
	SandboxIP          string
	SandboxIPCIDR      string
	HostGatewayIP      string
	SandboxBaseURL     string
	HostProxyBindURL   string
	ProxyPort          int
	NetnsName          string
	NetnsPath          string
	HostVeth           string
	SandboxVeth        string
	HostSideCIDR       string
	NftTableName       string
	EgressPolicyID     string
	EgressPolicyDigest string
	DNSPolicy          string
}

type VerifyRuntimeArtifactPathEvidenceParams struct {
	Payload             any
	ControlDirPath      string
	ControlManifestPath string
	BundleDirPath       string
	SpecPath            string
	BridgeDirPath       string
	LogDirPath          string
	NetworkHostsPath    string
}

type VerifyMountPlanEvidenceParams struct {
	Payload   any
	MountPlan runtime.MountPlan
}

type VerifySandboxContractEvidenceParams struct {
	Payload          any
	ContractID       string
	ContractDigest   string
	ProjectionDigest string
}

type VerifyRuntimeResourceEvidenceParams struct {
	Payload                any
	ResourceIdentityDigest string
}

func Validate(p ValidateParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
		return err
	}
	if err := validatePlanVersion(object); err != nil {
		return err
	}
	identity, err := requireObject(object, "identity")
	if err != nil {
		return err
	}
	driver, err := requireObject(object, "driver")
	if err != nil {
		return err
	}
	provider, err := requireObject(object, "runtime_provider")
	if err != nil {
		return err
	}
	if err := validateIdentity(identity); err != nil {
		return err
	}
	driverSpec, err := validateDriver(driver)
	if err != nil {
		return err
	}
	providerSpec, err := validateRuntimeProvider(provider)
	if err != nil {
		return err
	}
	if err := validateRunscPin(object); err != nil {
		return err
	}
	if err := validateImage(object); err != nil {
		return err
	}
	if err := validateNetwork(object); err != nil {
		return err
	}
	if err := validateDataVolumes(object); err != nil {
		return err
	}
	if err := validateRuntimeArtifacts(object, driverSpec); err != nil {
		return err
	}
	if err := validateMountEvidence(object); err != nil {
		return err
	}
	featurePolicy, err := requireObject(object, "feature_policy")
	if err != nil {
		return err
	}
	if err := validateFeaturePolicy(featurePolicy, driverSpec, providerSpec); err != nil {
		return err
	}
	if err := validateContentSnapshots(object, featurePolicy); err != nil {
		return err
	}
	if err := validateSourceDigests(object); err != nil {
		return err
	}
	if err := validateProjectionDigests(object); err != nil {
		return err
	}
	return nil
}

func VerifyRuntimeResourceEvidence(p VerifyRuntimeResourceEvidenceParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
		return err
	}
	artifacts, err := requireObject(object, "runtime_artifacts")
	if err != nil {
		return err
	}
	expectedDigest := strings.TrimSpace(p.ResourceIdentityDigest)
	if expectedDigest == "" {
		return nil
	}
	if strings.TrimSpace(stringField(artifacts, "resource_identity_digest")) != expectedDigest {
		return fmt.Errorf("generation plan runtime_artifacts.resource_identity_digest mismatch")
	}
	return nil
}

func VerifyRuntimeArtifactPathEvidence(p VerifyRuntimeArtifactPathEvidenceParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
		return err
	}
	artifacts, err := requireObject(object, "runtime_artifacts")
	if err != nil {
		return err
	}
	checks := []struct {
		label string
		got   string
		want  string
	}{
		{"runtime_artifacts.control_dir_path", stringField(artifacts, "control_dir_path"), p.ControlDirPath},
		{"runtime_artifacts.control_manifest_path", stringField(artifacts, "control_manifest_path"), p.ControlManifestPath},
		{"runtime_artifacts.bundle_dir_path", stringField(artifacts, "bundle_dir_path"), p.BundleDirPath},
		{"runtime_artifacts.spec_path", stringField(artifacts, "spec_path"), p.SpecPath},
		{"runtime_artifacts.bridge_dir_path", stringField(artifacts, "bridge_dir_path"), p.BridgeDirPath},
		{"runtime_artifacts.log_dir_path", stringField(artifacts, "log_dir_path"), p.LogDirPath},
		{"runtime_artifacts.network_hosts_path", optionalStringField(artifacts, "network_hosts_path"), p.NetworkHostsPath},
	}
	for _, check := range checks {
		got := strings.TrimSpace(check.got)
		want := strings.TrimSpace(check.want)
		if got == "" && want == "" {
			continue
		}
		if got != want {
			return fmt.Errorf("generation plan %s mismatch", check.label)
		}
	}
	return nil
}

func VerifyMountPlanEvidence(p VerifyMountPlanEvidenceParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
		return err
	}
	mounts, err := requireObject(object, "mounts")
	if err != nil {
		return err
	}
	runtimeMounts, err := runtimeMountPlanMountsByName(p.MountPlan)
	if err != nil {
		return err
	}
	for _, name := range []string{"workspace", "agent_home", "control", "bridge"} {
		mount, err := requireObject(mounts, name)
		if err != nil {
			return err
		}
		if err := verifyPlanMountAgainstRuntime("mounts."+name, mount, runtimeMounts[name]); err != nil {
			return err
		}
	}
	if err := verifyNetworkHostsMountPlanEvidence(mounts, runtimeMounts); err != nil {
		return err
	}
	if err := verifyContentSnapshotMountPlanEvidence(mounts, runtimeMounts); err != nil {
		return err
	}
	return verifyDriverConfigMountPlanEvidence(object, mounts, runtimeMounts)
}

func VerifySandboxContractEvidence(p VerifySandboxContractEvidenceParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
		return err
	}
	artifacts, err := requireObject(object, "runtime_artifacts")
	if err != nil {
		return err
	}
	contractID := strings.TrimSpace(p.ContractID)
	if contractID == "" {
		return fmt.Errorf("generation plan sandbox contract id evidence is required")
	}
	if strings.TrimSpace(stringField(artifacts, "sandbox_contract_id")) != contractID {
		return fmt.Errorf("generation plan runtime_artifacts.sandbox_contract_id mismatch")
	}
	contractDigest := strings.TrimSpace(p.ContractDigest)
	if !isSha256(contractDigest) {
		return fmt.Errorf("generation plan sandbox contract digest evidence is required")
	}
	if strings.TrimSpace(stringField(artifacts, "sandbox_contract_payload_digest")) != contractDigest {
		return fmt.Errorf("generation plan runtime_artifacts.sandbox_contract_payload_digest mismatch")
	}
	projectionDigest := strings.TrimSpace(p.ProjectionDigest)
	if !isSha256(projectionDigest) {
		return fmt.Errorf("generation plan sandbox_contract projection digest is required")
	}
	if projectionDigest != contractDigest {
		return fmt.Errorf("generation plan sandbox_contract projection digest mismatch")
	}
	return nil
}

func VerifyNetworkEvidence(p VerifyNetworkEvidenceParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
		return err
	}
	network, err := requireObject(object, "network")
	if err != nil {
		return err
	}
	checks := []struct {
		label string
		got   string
		want  string
	}{
		{"network.network_profile_id", stringField(network, "network_profile_id"), p.NetworkProfileID},
		{"network.runsc_network", stringField(network, "runsc_network"), p.RunscNetwork},
		{"network.runsc_overlay2", stringField(network, "runsc_overlay2"), p.RunscOverlay2},
		{"network.sandbox_ip", stringField(network, "sandbox_ip"), p.SandboxIP},
		{"network.sandbox_ip_cidr", stringField(network, "sandbox_ip_cidr"), p.SandboxIPCIDR},
		{"network.host_gateway_ip", stringField(network, "host_gateway_ip"), p.HostGatewayIP},
		{"network.sandbox_base_url", stringField(network, "sandbox_base_url"), p.SandboxBaseURL},
		{"network.host_proxy_bind_url", stringField(network, "host_proxy_bind_url"), p.HostProxyBindURL},
		{"network.netns_name", stringField(network, "netns_name"), p.NetnsName},
		{"network.netns_path", stringField(network, "netns_path"), p.NetnsPath},
		{"network.host_veth", stringField(network, "host_veth"), p.HostVeth},
		{"network.sandbox_veth", stringField(network, "sandbox_veth"), p.SandboxVeth},
		{"network.host_side_cidr", stringField(network, "host_side_cidr"), p.HostSideCIDR},
		{"network.nft_table_name", stringField(network, "nft_table_name"), p.NftTableName},
		{"network.egress_policy_id", stringField(network, "egress_policy_id"), p.EgressPolicyID},
		{"network.egress_policy_digest", stringField(network, "egress_policy_digest"), p.EgressPolicyDigest},
		{"network.dns_policy", stringField(network, "dns_policy"), p.DNSPolicy},
	}
	for _, check := range checks {
		want := strings.TrimSpace(check.want)
		if want == "" {
			continue
		}
		if strings.TrimSpace(check.got) != want {
			return fmt.Errorf("generation plan %s mismatch", check.label)
		}
	}
	if p.ProxyPort > 0 && numberField(network, "proxy_port") != int64(p.ProxyPort) {
		return fmt.Errorf("generation plan network.proxy_port mismatch")
	}
	return nil
}

func runtimeMountPlanMountsByName(plan runtime.MountPlan) (map[string]runtime.MountPlanMount, error) {
	mounts := map[string]runtime.MountPlanMount{}
	for _, mount := range append(append([]runtime.MountPlanMount{}, plan.Content...), plan.Scratch...) {
		name := strings.TrimSpace(mount.Name)
		if name == "" {
			return nil, fmt.Errorf("runtime mount plan mount name is required")
		}
		if _, ok := mounts[name]; ok {
			return nil, fmt.Errorf("runtime mount plan mount %s is duplicated", name)
		}
		mounts[name] = mount
	}
	return mounts, nil
}

func verifyNetworkHostsMountPlanEvidence(mounts map[string]any, runtimeMounts map[string]runtime.MountPlanMount) error {
	path := strings.TrimSpace(optionalStringField(mounts, "network_hosts_path"))
	runtimeMount, ok := runtimeMounts["network_hosts"]
	if path == "" {
		if ok {
			return fmt.Errorf("generation plan mounts.network_hosts_path runtime mount must be absent")
		}
		return nil
	}
	if !ok {
		return fmt.Errorf("generation plan mounts.network_hosts_path runtime mount is required")
	}
	expected := map[string]any{"source": path, "destination": "/etc/hosts", "mode": "ro", "type": "bind", "exact": true}
	return verifyPlanMountAgainstRuntime("mounts.network_hosts_path", expected, runtimeMount)
}

func verifyContentSnapshotMountPlanEvidence(mounts map[string]any, runtimeMounts map[string]runtime.MountPlanMount) error {
	contentSnapshots, ok := mounts["content_snapshots"].(map[string]any)
	if !ok || len(contentSnapshots) == 0 {
		for _, name := range []string{"skills_snapshot", "managed_settings_snapshot"} {
			if _, ok := runtimeMounts[name]; ok {
				return fmt.Errorf("generation plan mounts.content_snapshots runtime mount %s must be absent", name)
			}
		}
		return nil
	}
	kinds := sortedMapKeys(contentSnapshots)
	for _, kind := range kinds {
		mount, ok := contentSnapshots[kind].(map[string]any)
		if !ok {
			return fmt.Errorf("generation plan mounts.content_snapshots.%s must be an object", kind)
		}
		mountName := strings.TrimSpace(stringField(mount, "mount_name"))
		if mountName == "" {
			return fmt.Errorf("generation plan mounts.content_snapshots.%s.mount_name is required", kind)
		}
		if err := verifyPlanMountAgainstRuntime("mounts.content_snapshots."+kind, mount, runtimeMounts[mountName]); err != nil {
			return err
		}
	}
	return nil
}

func verifyDriverConfigMountPlanEvidence(object, mounts map[string]any, runtimeMounts map[string]runtime.MountPlanMount) error {
	driver, err := requireObject(object, "driver")
	if err != nil {
		return err
	}
	driverID := strings.TrimSpace(stringField(driver, "driver_id"))
	specs := agents.DriverConfigMaterializationSpecsFor(agents.ID(driverID))
	if len(specs) == 0 {
		return verifyNoDriverConfigRuntimeMounts(runtimeMounts)
	}
	materializedMounts, ok := mounts["driver_config_materializations"].(map[string]any)
	if !ok || len(materializedMounts) == 0 {
		return fmt.Errorf("generation plan mounts.driver_config_materializations is required for driver %s", driverID)
	}
	artifacts, err := requireObject(object, "runtime_artifacts")
	if err != nil {
		return err
	}
	entries, err := materializedDriverConfigArtifacts(artifacts, strings.TrimSpace(stringField(artifacts, "control_dir_path")))
	if err != nil {
		return err
	}
	entriesByName := map[string]runtime.DriverConfigMaterialization{}
	for _, entry := range entries {
		entriesByName[entry.Name] = entry
	}
	specsByName := map[string]agents.DriverConfigMaterializationSpec{}
	for _, spec := range specs {
		specsByName[spec.Name] = spec
	}
	names := sortedMapKeys(materializedMounts)
	for _, name := range names {
		mount, ok := materializedMounts[name].(map[string]any)
		if !ok {
			return fmt.Errorf("generation plan mounts.driver_config_materializations.%s must be an object", name)
		}
		entry, ok := entriesByName[name]
		if !ok {
			return fmt.Errorf("generation plan mounts.driver_config_materializations.%s runtime artifact is required", name)
		}
		spec, ok := specsByName[name]
		if !ok {
			return fmt.Errorf("generation plan mounts.driver_config_materializations.%s is unsupported for driver %s", name, driverID)
		}
		expected := map[string]any{
			"source":      entry.HostSourcePath,
			"destination": entry.SandboxDestination,
			"mode":        stringField(mount, "mode"),
			"type":        stringField(mount, "type"),
			"exact":       boolField(mount, "exact"),
		}
		if err := verifyPlanMountAgainstRuntime("mounts.driver_config_materializations."+name, expected, runtimeMounts[spec.MountName]); err != nil {
			return err
		}
	}
	if len(names) != len(specs) {
		return fmt.Errorf("generation plan mounts.driver_config_materializations missing required driver %s mounts", driverID)
	}
	return nil
}

func verifyNoDriverConfigRuntimeMounts(runtimeMounts map[string]runtime.MountPlanMount) error {
	for _, spec := range agents.AllDriverConfigMaterializationSpecs() {
		if _, ok := runtimeMounts[spec.MountName]; ok {
			return fmt.Errorf("generation plan mounts.driver_config_materializations runtime mount %s must be absent", spec.MountName)
		}
	}
	return nil
}

func verifyPlanMountAgainstRuntime(label string, planMount map[string]any, runtimeMount runtime.MountPlanMount) error {
	if strings.TrimSpace(runtimeMount.Name) == "" {
		return fmt.Errorf("generation plan %s runtime mount is required", label)
	}
	checks := []struct {
		field string
		got   string
		want  string
	}{
		{"source", runtimeMount.Source, stringField(planMount, "source")},
		{"destination", runtimeMount.Destination, stringField(planMount, "destination")},
		{"mode", runtimeMount.Mode, stringField(planMount, "mode")},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.got) != strings.TrimSpace(check.want) {
			return fmt.Errorf("generation plan %s.%s mismatch", label, check.field)
		}
	}
	if _, ok := planMount["type"]; ok && strings.TrimSpace(runtimeMount.Type) != strings.TrimSpace(stringField(planMount, "type")) {
		return fmt.Errorf("generation plan %s.type mismatch", label)
	}
	if _, ok := planMount["exact"]; ok && runtimeMountExact(runtimeMount) != boolField(planMount, "exact") {
		return fmt.Errorf("generation plan %s.exact mismatch", label)
	}
	return nil
}

func runtimeMountExact(mount runtime.MountPlanMount) bool {
	if mount.Type != "bind" {
		return false
	}
	for _, option := range mount.Options {
		if option == "rbind" {
			return false
		}
	}
	return true
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func VerifySourceDigestEvidence(p VerifySourceDigestEvidenceParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
		return err
	}
	sourceDigests, err := requireObject(object, "source_digests")
	if err != nil {
		return err
	}
	checks := []struct {
		field string
		got   string
		want  string
	}{
		{"runtime_config_digest", p.RuntimeConfigDigest, stringField(sourceDigests, "runtime_config_digest")},
		{"agent_manifest_digest", p.AgentManifestDigest, stringField(sourceDigests, "agent_manifest_digest")},
	}
	for _, check := range checks {
		got, want := strings.TrimSpace(check.got), strings.TrimSpace(check.want)
		if !isSha256(got) {
			return fmt.Errorf("generation plan source_digests.%s evidence is required", check.field)
		}
		if got != want {
			return fmt.Errorf("generation plan source_digests.%s mismatch", check.field)
		}
	}
	if len(p.AdapterInputDigests) > 0 {
		adapterInputDigests, err := requireObject(sourceDigests, "adapter_input_digests")
		if err != nil {
			return err
		}
		for _, kind := range AdapterInputDigestKinds() {
			got := strings.TrimSpace(p.AdapterInputDigests[kind])
			want := strings.TrimSpace(stringField(adapterInputDigests, kind))
			if !isSha256(got) {
				return fmt.Errorf("generation plan source_digests.adapter_input_digests.%s evidence is required", kind)
			}
			if got != want {
				return fmt.Errorf("generation plan source_digests.adapter_input_digests.%s mismatch", kind)
			}
		}
	}
	return nil
}

func VerifyDataVolumeEvidence(p VerifyDataVolumeEvidenceParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
		return err
	}
	dataVolumes, err := requireObject(object, "data_volumes")
	if err != nil {
		return err
	}
	workspace, err := requireObject(dataVolumes, "workspace")
	if err != nil {
		return err
	}
	agentHome, err := requireObject(dataVolumes, "agent_home")
	if err != nil {
		return err
	}
	checks := []struct {
		label string
		got   string
		want  string
	}{
		{"data_volumes.workspace.host_path", stringField(workspace, "host_path"), p.WorkspaceHostPath},
		{"data_volumes.workspace.runtime_identity_digest", stringField(workspace, "runtime_identity_digest"), p.WorkspaceRuntimeIdentityDigest},
		{"data_volumes.agent_home.host_path", stringField(agentHome, "host_path"), p.DriverHomeHostPath},
		{"data_volumes.agent_home.runtime_identity_digest", stringField(agentHome, "runtime_identity_digest"), p.DriverHomeRuntimeIdentityDigest},
	}
	for _, check := range checks {
		want := strings.TrimSpace(check.want)
		if want == "" {
			continue
		}
		if strings.TrimSpace(check.got) != want {
			return fmt.Errorf("generation plan %s mismatch", check.label)
		}
	}
	return nil
}

func VerifyFrozenEvidence(p VerifyFrozenEvidenceParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
		return err
	}
	if err := verifyFrozenIdentityEvidence(object, p); err != nil {
		return err
	}
	runsc, err := requireObject(object, "runsc_pin")
	if err != nil {
		return err
	}
	if strings.TrimSpace(p.RunscPlatform) != stringField(runsc, "platform") ||
		strings.TrimSpace(p.RunscVersion) != stringField(runsc, "version") ||
		strings.TrimSpace(p.RunscBinaryPath) != stringField(runsc, "binary_path") ||
		strings.TrimSpace(p.RunscBinaryDigest) != stringField(runsc, "binary_digest") {
		return fmt.Errorf("generation plan runsc pin mismatch")
	}
	for _, kind := range store.GenerationPlanProjectionKinds() {
		expectedDigest := strings.TrimSpace(p.ProjectionDigests[kind])
		if expectedDigest == "" {
			return fmt.Errorf("generation plan projection %s digest is required", kind)
		}
		expectedVersion := p.ProjectionVersions[kind]
		if expectedVersion <= 0 {
			if version, ok := store.GenerationPlanProjectionVersionFor(kind); ok {
				expectedVersion = version
			}
		}
		if expectedVersion <= 0 {
			return fmt.Errorf("generation plan projection %s version is required", kind)
		}
		registeredVersion, ok := store.GenerationPlanProjectionVersionFor(kind)
		if !ok {
			return fmt.Errorf("generation plan projection %s version is required", kind)
		}
		if expectedVersion != registeredVersion {
			return fmt.Errorf("generation plan projection %s version mismatch", kind)
		}
	}
	if p.CheckpointBundleDigest != "" && strings.TrimSpace(p.CheckpointBundleDigest) != strings.TrimSpace(p.ProjectionDigests[store.GenerationPlanProjectionBundle]) {
		return fmt.Errorf("generation plan checkpoint bundle digest mismatch")
	}
	if p.CheckpointRuntimeConfigDigest != "" && strings.TrimSpace(p.CheckpointRuntimeConfigDigest) != strings.TrimSpace(p.ProjectionDigests[store.GenerationPlanProjectionRuntimeConfig]) {
		return fmt.Errorf("generation plan checkpoint runtime config digest mismatch")
	}
	if p.CheckpointControlManifestDigest != "" && strings.TrimSpace(p.CheckpointControlManifestDigest) != strings.TrimSpace(p.ProjectionDigests[store.GenerationPlanProjectionControlManifestProjected]) {
		return fmt.Errorf("generation plan checkpoint control manifest digest mismatch")
	}
	if checkpointEvidencePresent(p) {
		if !isSha256(p.CheckpointPlanDigest) {
			return fmt.Errorf("generation plan checkpoint plan digest is required")
		}
		canonical, err := store.CanonicalGenerationPlanPayload(p.Payload)
		if err != nil {
			return err
		}
		if strings.TrimSpace(p.CheckpointPlanDigest) != store.GenerationPlanDigest(canonical) {
			return fmt.Errorf("generation plan checkpoint plan digest mismatch")
		}
		if !isSha256(p.CheckpointDriverStatesDigest) {
			return fmt.Errorf("generation plan checkpoint driver-state digest is required")
		}
	}
	snapshots, err := requireObject(object, "content_snapshots")
	if err != nil {
		return err
	}
	for kind, value := range snapshots {
		snapshot, ok := value.(map[string]any)
		if !ok {
			continue
		}
		expectedDigest := strings.TrimSpace(p.ContentSnapshotDigests[kind])
		if expectedDigest == "" {
			return fmt.Errorf("generation plan content snapshot %s digest is required", kind)
		}
		if expectedDigest != stringField(snapshot, "digest") {
			return fmt.Errorf("generation plan content snapshot %s digest mismatch", kind)
		}
	}
	return nil
}

func verifyFrozenIdentityEvidence(object map[string]any, p VerifyFrozenEvidenceParams) error {
	identity, err := requireObject(object, "identity")
	if err != nil {
		return err
	}
	driver, err := requireObject(object, "driver")
	if err != nil {
		return err
	}
	provider, err := requireObject(object, "runtime_provider")
	if err != nil {
		return err
	}
	network, err := requireObject(object, "network")
	if err != nil {
		return err
	}
	checks := []struct {
		label string
		got   string
		want  string
	}{
		{"identity.session_id", stringField(identity, "session_id"), p.SessionID},
		{"identity.generation_id", stringField(identity, "generation_id"), p.GenerationID},
		{"driver.driver_id", stringField(driver, "driver_id"), p.DriverID},
		{"driver.output_format", stringField(driver, "output_format"), p.OutputFormat},
		{"driver.initial_state_digest", stringField(driver, "initial_state_digest"), p.DriverStateDigest},
		{"runtime_provider.agent_runtime_profile_id", stringField(provider, "agent_runtime_profile_id"), p.AgentRuntimeProfileID},
		{"network.network_profile_id", stringField(network, "network_profile_id"), p.NetworkProfileID},
	}
	for _, check := range checks {
		want := strings.TrimSpace(check.want)
		if want == "" {
			continue
		}
		if strings.TrimSpace(check.got) != want {
			return fmt.Errorf("generation plan %s mismatch", check.label)
		}
	}
	if p.DriverStateVersion > 0 && numberField(driver, "initial_state_version") != int64(p.DriverStateVersion) {
		return fmt.Errorf("generation plan driver.initial_state_version mismatch")
	}
	return nil
}

func checkpointEvidencePresent(p VerifyFrozenEvidenceParams) bool {
	return strings.TrimSpace(p.CheckpointBundleDigest) != "" ||
		strings.TrimSpace(p.CheckpointRuntimeConfigDigest) != "" ||
		strings.TrimSpace(p.CheckpointControlManifestDigest) != ""
}

func decodePlanObject(payload any) (map[string]any, error) {
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		return nil, err
	}
	var object map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(canonical)))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("decode generation plan payload: %w", err)
	}
	return object, nil
}

func validatePlanVersion(object map[string]any) error {
	version, ok := object["plan_version"].(json.Number)
	if !ok || version.String() != fmt.Sprint(store.GenerationPlanVersion) {
		return fmt.Errorf("generation plan plan_version = %v, want %d", object["plan_version"], store.GenerationPlanVersion)
	}
	return nil
}

func validateIdentity(identity map[string]any) error {
	for _, key := range []string{"session_id", "generation_id", "product_mode"} {
		if strings.TrimSpace(stringField(identity, key)) == "" {
			return fmt.Errorf("generation plan identity.%s is required", key)
		}
	}
	return nil
}

func validateDriver(driver map[string]any) (agents.DriverSpec, error) {
	driverID := stringField(driver, "driver_id")
	spec, ok := agents.DriverSpecFor(driverID)
	if !ok {
		return agents.DriverSpec{}, fmt.Errorf("generation plan unsupported driver %q", driverID)
	}
	if stringField(driver, "driver_kind") != string(spec.Kind) {
		return agents.DriverSpec{}, fmt.Errorf("generation plan driver %s kind mismatch", driverID)
	}
	if stringField(driver, "bridge_protocol") != spec.BridgeProtocol ||
		numberField(driver, "bridge_protocol_version") != int64(spec.BridgeProtocolVersion) ||
		stringField(driver, "turn_input_schema") != spec.TurnInputSchema ||
		stringField(driver, "output_schema") != spec.OutputSchema {
		return agents.DriverSpec{}, fmt.Errorf("generation plan driver %s protocol facts mismatch", driverID)
	}
	if !hasString(driver, "output_format") {
		return agents.DriverSpec{}, fmt.Errorf("generation plan driver.output_format is required")
	}
	if !isSha256(stringField(driver, "initial_state_digest")) {
		return agents.DriverSpec{}, fmt.Errorf("generation plan driver.initial_state_digest is required")
	}
	if numberField(driver, "initial_state_version") <= 0 {
		return agents.DriverSpec{}, fmt.Errorf("generation plan driver.initial_state_version is required")
	}
	capabilities, err := requireObject(driver, "capability_snapshot")
	if err != nil {
		return agents.DriverSpec{}, err
	}
	if err := validateDriverCapabilitySnapshot(capabilities, spec); err != nil {
		return agents.DriverSpec{}, err
	}
	return spec, nil
}

func validateRuntimeProvider(provider map[string]any) (agents.RuntimeProviderSpec, error) {
	providerID := stringField(provider, "provider_id")
	spec, ok := agents.RuntimeProviderSpecFor(providerID)
	if !ok {
		return agents.RuntimeProviderSpec{}, fmt.Errorf("generation plan unsupported runtime provider %q", providerID)
	}
	for _, key := range []string{"provider_config_id", "provider_profile_id", "isolation_kind", "template_ref", "capability_vocab_version", "agent_runtime_profile_id", "runtime_profile_provider_ref"} {
		if strings.TrimSpace(stringField(provider, key)) == "" {
			return agents.RuntimeProviderSpec{}, fmt.Errorf("generation plan runtime_provider.%s is required", key)
		}
	}
	if stringField(provider, "provider_profile_id") != spec.ProviderProfileID ||
		stringField(provider, "isolation_kind") != spec.IsolationKind ||
		stringField(provider, "template_ref") != spec.TemplateRef ||
		stringField(provider, "capability_vocab_version") != spec.CapabilityVocabulary {
		return agents.RuntimeProviderSpec{}, fmt.Errorf("generation plan runtime provider %s facts mismatch", providerID)
	}
	if !isSha256(stringField(provider, "capability_digest")) {
		return agents.RuntimeProviderSpec{}, fmt.Errorf("generation plan runtime_provider.capability_digest is required")
	}
	capabilities, err := requireObject(provider, "capability_snapshot")
	if err != nil {
		return agents.RuntimeProviderSpec{}, err
	}
	if err := validateProviderCapabilitySnapshot(capabilities, spec); err != nil {
		return agents.RuntimeProviderSpec{}, err
	}
	if _, err := requireObject(provider, "snapshot_policy"); err != nil {
		return agents.RuntimeProviderSpec{}, err
	}
	return spec, nil
}

func validateRunscPin(object map[string]any) error {
	runsc, err := requireObject(object, "runsc_pin")
	if err != nil {
		return err
	}
	for _, key := range []string{"platform", "version", "binary_path"} {
		if strings.TrimSpace(stringField(runsc, key)) == "" {
			return fmt.Errorf("generation plan runsc_pin.%s is required", key)
		}
	}
	if !isSha256(stringField(runsc, "binary_digest")) {
		return fmt.Errorf("generation plan runsc_pin.binary_digest is required")
	}
	if !isCanonicalAbsolutePath(stringField(runsc, "binary_path")) {
		return fmt.Errorf("generation plan runsc_pin.binary_path must be canonical absolute")
	}
	return nil
}

func validateImage(object map[string]any) error {
	image, err := requireObject(object, "image")
	if err != nil {
		return err
	}
	if !isSha256(stringField(image, "agent_manifest_digest")) {
		return fmt.Errorf("generation plan image.agent_manifest_digest is required")
	}
	if !isCanonicalAbsolutePath(stringField(image, "rootfs_path")) {
		return fmt.Errorf("generation plan image.rootfs_path must be canonical absolute")
	}
	if value := image["rootfs_image_digest"]; value != nil && !isSha256(fmt.Sprint(value)) {
		return fmt.Errorf("generation plan image.rootfs_image_digest must be sha256 when present")
	}
	return nil
}

func validateNetwork(object map[string]any) error {
	network, err := requireObject(object, "network")
	if err != nil {
		return err
	}
	for _, key := range []string{"network_profile_id", "runsc_network", "runsc_overlay2", "sandbox_ip", "sandbox_ip_cidr", "host_gateway_ip", "sandbox_base_url", "host_proxy_bind_url", "netns_name", "netns_path", "host_veth", "sandbox_veth", "host_side_cidr", "nft_table_name", "egress_policy_id", "dns_policy"} {
		if strings.TrimSpace(stringField(network, key)) == "" {
			return fmt.Errorf("generation plan network.%s is required", key)
		}
	}
	if !hasString(network, "egress_policy_digest") {
		return fmt.Errorf("generation plan network.egress_policy_digest is required")
	}
	if _, err := netip.ParseAddr(stringField(network, "sandbox_ip")); err != nil {
		return fmt.Errorf("generation plan network.sandbox_ip is invalid: %w", err)
	}
	if _, err := netip.ParsePrefix(stringField(network, "sandbox_ip_cidr")); err != nil {
		return fmt.Errorf("generation plan network.sandbox_ip_cidr is invalid: %w", err)
	}
	if !isCanonicalAbsolutePath(stringField(network, "netns_path")) {
		return fmt.Errorf("generation plan network.netns_path must be canonical absolute")
	}
	return nil
}

func validateDataVolumes(object map[string]any) error {
	dataVolumes, err := requireObject(object, "data_volumes")
	if err != nil {
		return err
	}
	for _, name := range []string{"workspace", "agent_home"} {
		volume, err := requireObject(dataVolumes, name)
		if err != nil {
			return err
		}
		for _, key := range []string{"session_id", "host_path", "runtime_identity_digest", "provisioning_marker_path", "provisioning_marker_digest", "sandbox_destination"} {
			if strings.TrimSpace(stringField(volume, key)) == "" {
				return fmt.Errorf("generation plan data_volumes.%s.%s is required", name, key)
			}
		}
		if numberField(volume, "layout_version") <= 0 {
			return fmt.Errorf("generation plan data_volumes.%s.layout_version is required", name)
		}
		for _, key := range []string{"host_path", "provisioning_marker_path"} {
			if !isCanonicalAbsolutePath(stringField(volume, key)) {
				return fmt.Errorf("generation plan data_volumes.%s.%s must be canonical absolute", name, key)
			}
		}
		for _, key := range []string{"runtime_identity_digest", "provisioning_marker_digest"} {
			if !isSha256(stringField(volume, key)) {
				return fmt.Errorf("generation plan data_volumes.%s.%s is required", name, key)
			}
		}
		if numberField(volume, "sandbox_uid") < 0 || numberField(volume, "sandbox_gid") < 0 {
			return fmt.Errorf("generation plan data_volumes.%s sandbox ownership is required", name)
		}
	}
	return nil
}

func validateRuntimeArtifacts(object map[string]any, driver agents.DriverSpec) error {
	artifacts, err := requireObject(object, "runtime_artifacts")
	if err != nil {
		return err
	}
	for _, key := range []string{"control_dir_path", "control_manifest_path", "bundle_dir_path", "spec_path", "bridge_dir_path", "log_dir_path"} {
		if !isCanonicalAbsolutePath(stringField(artifacts, key)) {
			return fmt.Errorf("generation plan runtime_artifacts.%s must be canonical absolute", key)
		}
	}
	for _, key := range []string{"control_manifest_digest", "projected_control_manifest_digest", "bundle_digest", "runtime_config_digest", "spec_digest", "resource_identity_digest", "sandbox_contract_payload_digest"} {
		if !hasString(artifacts, key) {
			return fmt.Errorf("generation plan runtime_artifacts.%s is required", key)
		}
	}
	if strings.TrimSpace(stringField(artifacts, "sandbox_contract_id")) == "" {
		return fmt.Errorf("generation plan runtime_artifacts.sandbox_contract_id is required")
	}
	return validateDriverConfigMaterializationEvidence(object, artifacts, driver.ID)
}

func requireObject(object map[string]any, key string) (map[string]any, error) {
	child, ok := object[key].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("generation plan missing %s object", key)
	}
	return child, nil
}

func hasString(object map[string]any, key string) bool {
	return strings.TrimSpace(stringField(object, key)) != ""
}

func stringField(object map[string]any, key string) string {
	value, ok := object[key].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func optionalStringField(object map[string]any, key string) string {
	value, ok := object[key]
	if !ok || value == nil {
		return ""
	}
	stringValue, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(stringValue)
}

func boolField(object map[string]any, key string) bool {
	value, ok := object[key].(bool)
	return ok && value
}

func numberField(object map[string]any, key string) int64 {
	switch value := object[key].(type) {
	case json.Number:
		parsed, err := value.Int64()
		if err == nil {
			return parsed
		}
	case float64:
		return int64(value)
	case int:
		return int64(value)
	case int64:
		return value
	}
	return 0
}

func isSha256(value string) bool {
	return strings.HasPrefix(strings.TrimSpace(value), "sha256:")
}
