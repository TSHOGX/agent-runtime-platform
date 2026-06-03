package generationplan

import (
	"fmt"
	"strings"
)

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
