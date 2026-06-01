import { describe, expect, it } from "vitest";

import { newSessionOptions } from "./agents";
import type { DeploymentCapabilities } from "./types";

describe("newSessionOptions", () => {
  it("fails closed when deployment capabilities are unavailable", () => {
    expect(newSessionOptions(null)).toEqual([]);
  });

  it("uses real deployment capabilities for visible create options", () => {
    const capabilities: DeploymentCapabilities = {
      schema_version: 1,
      default_mode: "shell",
      session_modes: [
        { mode: "agent", label: "Agent", visible: true, create_enabled: true, disabled_reason: null },
        {
          mode: "shell",
          label: "Shell",
          visible: true,
          create_enabled: false,
          disabled_reason: "Shell is unavailable"
        }
      ]
    };

    expect(newSessionOptions(capabilities).map(({ value, label, createEnabled, disabledReason }) => ({
      value,
      label,
      createEnabled,
      disabledReason
    }))).toEqual([
      { value: "shell", label: "Shell", createEnabled: false, disabledReason: "Shell is unavailable" },
      { value: "agent", label: "Agent", createEnabled: true, disabledReason: null }
    ]);
  });

  it("omits invisible deployment modes", () => {
    const capabilities: DeploymentCapabilities = {
      schema_version: 1,
      default_mode: "agent",
      session_modes: [
        { mode: "agent", label: "Agent", visible: true, create_enabled: true, disabled_reason: null },
        { mode: "shell", label: "Shell", visible: false, create_enabled: true, disabled_reason: null }
      ]
    };

    expect(newSessionOptions(capabilities).map((option) => option.value)).toEqual(["agent"]);
  });
});
