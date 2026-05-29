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
		!spec.ModelAccess {
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
