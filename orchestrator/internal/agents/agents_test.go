package agents

import "testing"

func TestPiDriverSpecIsRegistered(t *testing.T) {
	spec, ok := DriverSpecFor("pi")
	if !ok {
		t.Fatalf("pi driver spec missing")
	}
	if spec.ID != Pi ||
		spec.Kind != DriverKindAgent ||
		spec.BridgeProtocolVersion != 2 ||
		spec.TurnInputSchema != "RunTurn" ||
		spec.OutputSchema != PiEventSchemaVersion ||
		!spec.ModelAccess ||
		len(spec.ConfigMaterializationSpecs) != 2 ||
		spec.RuntimeLayoutSpec == nil {
		t.Fatalf("unexpected pi spec: %+v", spec)
	}
	if PiPackageName != "@earendil-works/pi-coding-agent" ||
		PiPackageVersion != "0.77.0" ||
		PiPackageShasum == "" ||
		PiPackageIntegrity == "" {
		t.Fatalf("unexpected pi release pin: %s %s %s %s", PiPackageName, PiPackageVersion, PiPackageShasum, PiPackageIntegrity)
	}
	if _, err := CanonicalDriverID("pi"); err != nil {
		t.Fatalf("canonical pi driver rejected: %v", err)
	}
	if public, ok := PublicAgentForDriver("pi"); !ok || public != "pi" {
		t.Fatalf("public pi mapping = %q/%v", public, ok)
	}
	def, ok := Lookup("pi")
	if !ok || def.Protocol != ProtocolPiRPC {
		t.Fatalf("lookup pi = %+v/%v", def, ok)
	}
}

func TestPiDriverConfigMaterializationSpecs(t *testing.T) {
	spec, ok := DriverSpecFor("pi")
	if !ok {
		t.Fatalf("pi driver spec missing")
	}
	if len(spec.ConfigMaterializationSpecs) != 2 {
		t.Fatalf("unexpected pi config materialization specs: %+v", spec.ConfigMaterializationSpecs)
	}
	if spec.ConfigMaterializationSpecs[0].Name != "models" ||
		spec.ConfigMaterializationSpecs[0].SourceProjectionPath != PiModelsConfigPath ||
		spec.ConfigMaterializationSpecs[0].SandboxDestination != PiModelsSandboxPath ||
		spec.ConfigMaterializationSpecs[1].Name != "settings" ||
		spec.ConfigMaterializationSpecs[1].SourceProjectionPath != PiSettingsConfigPath ||
		spec.ConfigMaterializationSpecs[1].SandboxDestination != PiSettingsSandboxPath {
		t.Fatalf("unexpected pi config materialization specs: %+v", spec.ConfigMaterializationSpecs)
	}

	helperSpecs := DriverConfigMaterializationSpecsFor(Pi)
	if len(helperSpecs) != 2 ||
		helperSpecs[0] != spec.ConfigMaterializationSpecs[0] ||
		helperSpecs[1] != spec.ConfigMaterializationSpecs[1] {
		t.Fatalf("helper specs = %+v, driver specs = %+v", helperSpecs, spec.ConfigMaterializationSpecs)
	}

	allSpecs := AllDriverConfigMaterializationSpecs()
	if len(allSpecs) != 2 ||
		allSpecs[0] != spec.ConfigMaterializationSpecs[0] ||
		allSpecs[1] != spec.ConfigMaterializationSpecs[1] {
		t.Fatalf("all specs = %+v, driver specs = %+v", allSpecs, spec.ConfigMaterializationSpecs)
	}

	spec.ConfigMaterializationSpecs[0].MountName = "mutated"
	if spec.RuntimeLayoutSpec == nil {
		t.Fatalf("pi runtime layout missing")
	}
	spec.RuntimeLayoutSpec.Env[0].Value = "mutated"
	specAgain, ok := DriverSpecFor("pi")
	if !ok ||
		specAgain.ConfigMaterializationSpecs[0].MountName != "pi_models_config" ||
		specAgain.RuntimeLayoutSpec == nil ||
		specAgain.RuntimeLayoutSpec.Env[0].Value != PiCodingAgentDir {
		t.Fatalf("driver spec should be cloned, got %+v/%v", specAgain, ok)
	}

	helperSpecs[0].MountName = "mutated"
	helperAgain := DriverConfigMaterializationSpecsFor(Pi)
	if helperAgain[0].MountName != "pi_models_config" {
		t.Fatalf("helper specs should be cloned, got %+v", helperAgain)
	}
}

func TestPiRuntimeLayoutSpec(t *testing.T) {
	layout, ok := DriverRuntimeLayoutSpecFor(Pi)
	if !ok {
		t.Fatalf("pi runtime layout spec missing")
	}
	env := map[string]string{}
	for _, item := range layout.Env {
		env[item.Name] = item.Value
	}
	if env["PI_CODING_AGENT_DIR"] != PiCodingAgentDir ||
		env["PI_CODING_AGENT_SESSION_DIR"] != PiSessionDir ||
		env["PI_OFFLINE"] != "1" ||
		env["PI_SKIP_VERSION_CHECK"] != "1" ||
		env["PI_TELEMETRY"] != "0" {
		t.Fatalf("unexpected pi env layout: %+v", layout.Env)
	}
	if len(layout.HomeDirs) != 3 ||
		layout.HomeDirs[0].AgentHomeRelativePath != ".pi" ||
		layout.HomeDirs[1].AgentHomeRelativePath != ".pi/agent" ||
		layout.HomeDirs[2].AgentHomeRelativePath != ".pi/agent/sessions" {
		t.Fatalf("unexpected pi home dir layout: %+v", layout.HomeDirs)
	}
	if layout.ControlManifest.Fields["pi_coding_agent_dir"] != PiCodingAgentDir ||
		layout.ControlManifest.Fields["pi_coding_agent_session_dir"] != PiSessionDir ||
		layout.ControlManifest.Fields["pi_offline"] != true ||
		layout.ControlManifest.Fields["pi_skip_version_check"] != true ||
		layout.ControlManifest.Fields["pi_telemetry_disabled"] != true {
		t.Fatalf("unexpected pi manifest layout: %+v", layout.ControlManifest)
	}

	layout.Env[0].Value = "mutated"
	layout.ControlManifest.Fields["pi_coding_agent_dir"] = "mutated"
	layoutAgain, ok := DriverRuntimeLayoutSpecFor(Pi)
	if !ok ||
		layoutAgain.Env[0].Value != PiCodingAgentDir ||
		layoutAgain.ControlManifest.Fields["pi_coding_agent_dir"] != PiCodingAgentDir {
		t.Fatalf("runtime layout spec should be cloned, got %+v/%+v/%v", layoutAgain.Env, layoutAgain.ControlManifest, ok)
	}
}
