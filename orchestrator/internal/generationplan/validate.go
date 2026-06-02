package generationplan

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/agents"
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
	if !filepath.IsAbs(stringField(runsc, "binary_path")) {
		return fmt.Errorf("generation plan runsc_pin.binary_path must be absolute")
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
	if !filepath.IsAbs(stringField(image, "rootfs_path")) {
		return fmt.Errorf("generation plan image.rootfs_path must be absolute")
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
	if !filepath.IsAbs(stringField(network, "netns_path")) {
		return fmt.Errorf("generation plan network.netns_path must be absolute")
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
			if !filepath.IsAbs(stringField(volume, key)) {
				return fmt.Errorf("generation plan data_volumes.%s.%s must be absolute", name, key)
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
		if !filepath.IsAbs(stringField(artifacts, key)) {
			return fmt.Errorf("generation plan runtime_artifacts.%s must be absolute", key)
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

func validateDriverConfigMaterializationEvidence(object map[string]any, artifacts map[string]any, driverID agents.ID) error {
	specs := agents.DriverConfigMaterializationSpecsFor(driverID)
	entries, ok := artifacts["materialized_driver_config"].([]any)
	if !ok {
		return fmt.Errorf("generation plan runtime_artifacts.materialized_driver_config must be an array")
	}
	if len(specs) == 0 {
		if len(entries) != 0 {
			return fmt.Errorf("generation plan driver %s does not support driver config materialization", driverID)
		}
		if mounts, ok := object["mounts"].(map[string]any); ok {
			if mountMaterializations, ok := mounts["driver_config_materializations"].(map[string]any); ok && len(mountMaterializations) != 0 {
				return fmt.Errorf("generation plan driver %s does not support driver config materialization", driverID)
			}
		}
		return nil
	}
	mounts, err := requireObject(object, "mounts")
	if err != nil {
		return err
	}
	mountMaterializations, ok := mounts["driver_config_materializations"].(map[string]any)
	if !ok {
		return fmt.Errorf("generation plan mounts.driver_config_materializations is required for driver %s", driverID)
	}
	expected := map[string]agents.DriverConfigMaterializationSpec{}
	for _, spec := range specs {
		expected[spec.Name] = spec
	}
	if len(entries) != len(expected) || len(mountMaterializations) != len(expected) {
		return fmt.Errorf("generation plan driver %s config materialization must contain exactly %d projections", driverID, len(expected))
	}
	seen := map[string]struct{}{}
	for _, value := range entries {
		entry, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("generation plan runtime_artifacts.materialized_driver_config entries must be objects")
		}
		name := stringField(entry, "name")
		want, ok := expected[name]
		if !ok {
			return fmt.Errorf("generation plan unsupported %s driver config materialization %q", driverID, name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("generation plan duplicate %s driver config materialization %q", driverID, name)
		}
		seen[name] = struct{}{}
		if err := validateDriverConfigRuntimeEntry(driverID, name, want, entry); err != nil {
			return err
		}
		mountEntry, ok := mountMaterializations[name].(map[string]any)
		if !ok {
			return fmt.Errorf("generation plan mounts.driver_config_materializations.%s is required", name)
		}
		if err := validateDriverConfigMountEntry(driverID, name, want, mountEntry); err != nil {
			return err
		}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("generation plan driver %s config materialization missing required projections", driverID)
	}
	return nil
}

func validateDriverConfigRuntimeEntry(driverID agents.ID, name string, want agents.DriverConfigMaterializationSpec, entry map[string]any) error {
	if stringField(entry, "projection_materialization_kind") != "driver_config" {
		return fmt.Errorf("generation plan %s runtime %s projection_materialization_kind must be driver_config", driverID, name)
	}
	if source := stringField(entry, "source_projection_path"); source != want.SourceProjectionPath {
		return fmt.Errorf("generation plan %s runtime %s source_projection_path = %q", driverID, name, source)
	}
	if digest := stringField(entry, "source_digest"); !isSha256(digest) {
		return fmt.Errorf("generation plan %s runtime %s source_digest is required", driverID, name)
	}
	if destination := stringField(entry, "sandbox_destination"); destination != want.SandboxDestination {
		return fmt.Errorf("generation plan %s runtime %s sandbox_destination = %q", driverID, name, destination)
	}
	if mutable := boolField(entry, "destination_mutable_by_sandbox"); mutable != want.DestinationMutableBySandbox {
		return fmt.Errorf("generation plan %s runtime %s destination mutability mismatch", driverID, name)
	}
	return nil
}

func validateDriverConfigMountEntry(driverID agents.ID, name string, want agents.DriverConfigMaterializationSpec, entry map[string]any) error {
	if typ := stringField(entry, "type"); typ != want.MountType {
		return fmt.Errorf("generation plan %s mount %s type = %q", driverID, name, typ)
	}
	if mode := stringField(entry, "mode"); mode != want.MountMode {
		return fmt.Errorf("generation plan %s mount %s mode = %q", driverID, name, mode)
	}
	if exact := boolField(entry, "exact"); exact != want.MountExact {
		return fmt.Errorf("generation plan %s mount %s exactness mismatch", driverID, name)
	}
	if source := stringField(entry, "source_projection_path"); source != want.SourceProjectionPath {
		return fmt.Errorf("generation plan %s mount %s source_projection_path = %q", driverID, name, source)
	}
	if destination := stringField(entry, "sandbox_destination"); destination != want.SandboxDestination {
		return fmt.Errorf("generation plan %s mount %s sandbox_destination = %q", driverID, name, destination)
	}
	if mutable := boolField(entry, "destination_mutable_by_sandbox"); mutable != want.DestinationMutableBySandbox {
		return fmt.Errorf("generation plan %s mount %s destination mutability mismatch", driverID, name)
	}
	return nil
}

func validateMountEvidence(object map[string]any) error {
	mounts, err := requireObject(object, "mounts")
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
	artifacts, err := requireObject(object, "runtime_artifacts")
	if err != nil {
		return err
	}
	checks := []struct {
		name        string
		source      string
		destination string
		mode        string
	}{
		{name: "workspace", source: stringField(workspace, "host_path"), destination: stringField(workspace, "sandbox_destination"), mode: "rw"},
		{name: "agent_home", source: stringField(agentHome, "host_path"), destination: stringField(agentHome, "sandbox_destination"), mode: "rw"},
		{name: "control", source: stringField(artifacts, "control_dir_path"), destination: "/harness-control", mode: "ro"},
		{name: "bridge", source: stringField(artifacts, "bridge_dir_path"), destination: "/harness-control/bridge", mode: "rw"},
	}
	for _, check := range checks {
		mount, err := requireObject(mounts, check.name)
		if err != nil {
			return err
		}
		if err := validateMountEvidenceEntry(check.name, mount, check.source, check.destination, check.mode); err != nil {
			return err
		}
	}
	if optionalStringField(mounts, "network_hosts_path") != optionalStringField(artifacts, "network_hosts_path") {
		return fmt.Errorf("generation plan mounts.network_hosts_path mismatch")
	}
	return nil
}

func validateMountEvidenceEntry(name string, mount map[string]any, source, destination, mode string) error {
	checks := []struct {
		field string
		got   string
		want  string
	}{
		{"source", stringField(mount, "source"), source},
		{"destination", stringField(mount, "destination"), destination},
		{"mode", stringField(mount, "mode"), mode},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.got) != strings.TrimSpace(check.want) {
			return fmt.Errorf("generation plan mounts.%s.%s mismatch", name, check.field)
		}
	}
	return nil
}

func validateFeaturePolicy(policy map[string]any, driver agents.DriverSpec, provider agents.RuntimeProviderSpec) error {
	typedPolicy := agents.FeaturePolicy{}
	for _, feature := range agents.AllFeatureIDs() {
		value := stringField(policy, string(feature))
		if value == "" {
			return fmt.Errorf("generation plan feature_policy.%s is required", feature)
		}
		typedPolicy[feature] = agents.FeaturePolicyState(value)
	}
	if err := agents.ValidateFeaturePolicy(typedPolicy, driver, provider); err != nil {
		return fmt.Errorf("generation plan feature policy invalid: %w", err)
	}
	if numberField(policy, "capability_schema_version") != int64(agents.DriverCapabilitySchemaVersion) {
		return fmt.Errorf("generation plan feature_policy.capability_schema_version is required")
	}
	if stringField(policy, "capability_vocab_version") != provider.CapabilityVocabulary {
		return fmt.Errorf("generation plan feature_policy.capability_vocab_version mismatch")
	}
	if boolField(policy, "unsupported_features_fail") != true {
		return fmt.Errorf("generation plan feature_policy.unsupported_features_fail must be true")
	}
	if stringField(policy, "credential_bearing_mcp_scope") != "out_of_scope" {
		return fmt.Errorf("generation plan feature_policy.credential_bearing_mcp_scope must be out_of_scope")
	}
	driverCapabilities, err := requireObject(policy, "driver_capabilities")
	if err != nil {
		return err
	}
	if err := validateDriverCapabilitySnapshot(driverCapabilities, driver); err != nil {
		return err
	}
	providerCapabilities, err := requireObject(policy, "runtime_provider_capabilities")
	if err != nil {
		return err
	}
	if err := validateProviderCapabilitySnapshot(providerCapabilities, provider); err != nil {
		return err
	}
	return nil
}

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
	if !filepath.IsAbs(stringField(snapshot, "immutable_host_path")) {
		return fmt.Errorf("generation plan content_snapshots.%s.immutable_host_path must be absolute", name)
	}
	if !strings.HasPrefix(stringField(snapshot, "mount_destination"), "/") {
		return fmt.Errorf("generation plan content_snapshots.%s.mount_destination must be absolute", name)
	}
	if destination, ok := contentSnapshotMountDestination(name); ok && stringField(snapshot, "mount_destination") != destination {
		return fmt.Errorf("generation plan content_snapshots.%s.mount_destination must be %s", name, destination)
	}
	return nil
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

func validateSourceDigests(object map[string]any) error {
	sourceDigests, err := requireObject(object, "source_digests")
	if err != nil {
		return err
	}
	for _, key := range []string{"runtime_config_digest", "agent_manifest_digest"} {
		if !isSha256(stringField(sourceDigests, key)) {
			return fmt.Errorf("generation plan source_digests.%s is required", key)
		}
	}
	return nil
}

func validateProjectionDigests(object map[string]any) error {
	value, ok := object["projection_digests"]
	if !ok || value == nil {
		return nil
	}
	projections, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("generation plan projection_digests must be an object")
	}
	for _, kind := range store.GenerationPlanProjectionKinds() {
		projection, err := requireObject(projections, kind)
		if err != nil {
			return err
		}
		version, _ := store.GenerationPlanProjectionVersionFor(kind)
		if numberField(projection, "projection_version") != int64(version) {
			return fmt.Errorf("generation plan projection_digests.%s.projection_version = %d, want %d", kind, numberField(projection, "projection_version"), version)
		}
		if !isSha256(stringField(projection, "payload_digest")) {
			return fmt.Errorf("generation plan projection_digests.%s.payload_digest is required", kind)
		}
		if path := optionalStringField(projection, "materialized_path"); path != "" && !filepath.IsAbs(path) {
			return fmt.Errorf("generation plan projection_digests.%s.materialized_path must be absolute", kind)
		}
	}
	return nil
}

func validateDriverCapabilitySnapshot(snapshot map[string]any, spec agents.DriverSpec) error {
	if numberField(snapshot, "schema_version") != int64(agents.DriverCapabilitySchemaVersion) {
		return fmt.Errorf("generation plan driver capability schema_version is required")
	}
	features, err := requireObject(snapshot, "features")
	if err != nil {
		return err
	}
	for _, feature := range agents.AllFeatureIDs() {
		if stringField(features, string(feature)) != string(spec.Capabilities.Features[feature]) {
			return fmt.Errorf("generation plan driver capability feature %s mismatch", feature)
		}
	}
	subCapabilities, err := requireObject(snapshot, "sub_capabilities")
	if err != nil {
		return err
	}
	for _, capability := range agents.AllSubCapabilityIDs() {
		if stringField(subCapabilities, string(capability)) != string(spec.Capabilities.SubCapabilities[capability]) {
			return fmt.Errorf("generation plan driver sub-capability %s mismatch", capability)
		}
	}
	return nil
}

func validateProviderCapabilitySnapshot(snapshot map[string]any, spec agents.RuntimeProviderSpec) error {
	if stringField(snapshot, "vocabulary_version") != spec.CapabilitySnapshot.VocabularyVersion {
		return fmt.Errorf("generation plan runtime provider capability vocabulary mismatch")
	}
	capabilities, ok := snapshot["capabilities"].([]any)
	if !ok || len(capabilities) != len(spec.CapabilitySnapshot.Capabilities) {
		return fmt.Errorf("generation plan runtime provider capability list mismatch")
	}
	for i, expected := range spec.CapabilitySnapshot.Capabilities {
		if fmt.Sprint(capabilities[i]) != expected {
			return fmt.Errorf("generation plan runtime provider capability list mismatch")
		}
	}
	return nil
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
