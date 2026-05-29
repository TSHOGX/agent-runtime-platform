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
		spec.OutputSchema != "pi_rpc_events_v1.0" ||
		!spec.ModelAccess {
		t.Fatalf("unexpected pi spec: %+v", spec)
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
