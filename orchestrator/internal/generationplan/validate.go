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
	RunscPlatform                   string
	RunscVersion                    string
	RunscBinaryPath                 string
	RunscBinaryDigest               string
	ProjectionDigests               map[string]string
	ContentSnapshotDigests          map[string]string
	CheckpointBundleDigest          string
	CheckpointRuntimeConfigDigest   string
	CheckpointControlManifestDigest string
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
	if err := validateRuntimeArtifacts(object); err != nil {
		return err
	}
	featurePolicy, err := requireObject(object, "feature_policy")
	if err != nil {
		return err
	}
	if err := validateFeaturePolicy(featurePolicy, driverSpec, providerSpec); err != nil {
		return err
	}
	if err := validateContentSnapshots(object); err != nil {
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

func VerifyFrozenEvidence(p VerifyFrozenEvidenceParams) error {
	object, err := decodePlanObject(p.Payload)
	if err != nil {
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
	projections, err := requireObject(object, "projection_digests")
	if err != nil {
		return err
	}
	for kind, expectedDigest := range p.ProjectionDigests {
		kind = strings.TrimSpace(kind)
		if kind == "" {
			return fmt.Errorf("generation plan projection kind is required")
		}
		projection, err := requireObject(projections, kind)
		if err != nil {
			return err
		}
		if strings.TrimSpace(expectedDigest) != stringField(projection, "payload_digest") {
			return fmt.Errorf("generation plan projection %s digest mismatch", kind)
		}
	}
	if p.CheckpointBundleDigest != "" && strings.TrimSpace(p.CheckpointBundleDigest) != projectionDigest(projections, "bundle") {
		return fmt.Errorf("generation plan checkpoint bundle digest mismatch")
	}
	if p.CheckpointRuntimeConfigDigest != "" && strings.TrimSpace(p.CheckpointRuntimeConfigDigest) != projectionDigest(projections, "runtime_config") {
		return fmt.Errorf("generation plan checkpoint runtime config digest mismatch")
	}
	if p.CheckpointControlManifestDigest != "" && strings.TrimSpace(p.CheckpointControlManifestDigest) != projectionDigest(projections, "control_manifest_projected") {
		return fmt.Errorf("generation plan checkpoint control manifest digest mismatch")
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

func projectionDigest(projections map[string]any, kind string) string {
	projection, ok := projections[kind].(map[string]any)
	if !ok {
		return ""
	}
	return stringField(projection, "payload_digest")
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

func validateRuntimeArtifacts(object map[string]any) error {
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

func validateContentSnapshots(object map[string]any) error {
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
	projections, err := requireObject(object, "projection_digests")
	if err != nil {
		return err
	}
	for _, kind := range []string{"sandbox_contract", "control_manifest", "control_manifest_projected", "oci_spec", "bundle", "runtime_config"} {
		projection, err := requireObject(projections, kind)
		if err != nil {
			return err
		}
		if numberField(projection, "projection_version") <= 0 {
			return fmt.Errorf("generation plan projection_digests.%s.projection_version is required", kind)
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
