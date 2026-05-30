# Release Qualification

This is the single entry point for "how we qualify a release." Release
qualification is run by one generic engine with per-concern **suites**, behind
one dispatcher. Adding a future concern is a new suite file, not a forked
script.

- Engine: `tools/release/engine.py` (generic; no phase identifiers).
- Suites: `tools/release/suites/*.py` (each declares its gates, evidence model,
  and static checks).
- Dispatcher: `tools/release/run.py --suite <name> [args...]`.

## Suites

| Suite | Concern | Gate detail | Status |
|-------|---------|-------------|--------|
| `control_plane` | Durable control-plane gates: orchestrator package tests, turn-start latency bench, pinned proxy contract, gVisor bridge durability lab, secret-permission lab, live latency. | [phase7/release-qualification.md](./phase7/release-qualification.md) | **Blocking** per guardrail below. |
| `sandbox_isolation` | The `sandbox-isolation-v1` runtime-isolation contract: deterministic repo gates, supplied-evidence model, static release scans, doc-inventory cross-check. | [phase8/release-gates.md](./phase8/release-gates.md), [phase8 boundary](./phase8/README.md#phase-7-boundary) | **Live release harness.** |
| `driver_contract` | Phase 9 driver/provider contract. | Reserved placeholder — phase 9 qualification is `go test ./...`, already run by `sandbox_isolation`. | Reserved. |
| `agent_capability` | Phase 10 agent capability / UX. | Reserved — exposes the phase 10 static-check subset as a focused view. | Reserved. |

The detailed adversarial gate listing lives in
[phase8/release-gates.md](./phase8/release-gates.md); the `sandbox_isolation`
suite parses that document into its gate inventory, so its section/bullet
structure is a stable contract.

## Commands

```bash
# Single suite (preferred):
python3 tools/release/run.py --suite sandbox_isolation            # full run
python3 tools/release/run.py --suite sandbox_isolation --static-only
python3 tools/release/run.py --suite sandbox_isolation --list
python3 tools/release/run.py --suite control_plane

# Compose all registered suites (mainly --list):
python3 tools/release/run.py --suite all --list

# Legacy shim aliases (retained, identical behavior):
python3 tools/phase8/release-gates.py --static-only      # == --suite sandbox_isolation
python3 tools/phase7/release-gates.py                    # == --suite control_plane
```

Supplied evidence and release completion (sandbox_isolation):

```bash
python3 tools/release/run.py --suite sandbox_isolation \
  --include-cutover-inventory --include-reconciliation --include-rootfs-inspection \
  --include-proxy --include-adversarial-lab --include-prior-release \
  --evidence cutover=PATH --evidence reconciliation=PATH ... \
  --require-release-evidence --output evidence.json
```

## Guardrail: control-plane gates stay blocking

> Keep Phase 7 release gates blocking for runtime, proxy, or config changes
> until a later phase explicitly retires or replaces a gate.

The former Phase 7 gates are carried verbatim by the `control_plane` suite
(same names, categories, commands, and exit semantics). They are not retired or
weakened by the move to suites; `--suite control_plane` (and the
`tools/phase7/release-gates.py` shim) is their carrier. See
[PLAN.md](./PLAN.md) and [phase8/README.md](./phase8/README.md#phase-7-boundary).

## Pinned proxy contract re-pin

The proxy contract gate runs
`/root/claude-code-proxy/tests/test_harness_probe_contract.py` and records the
proxy commit in the evidence `context.proxy`. To re-pin after a proxy contract
change: update the proxy test, re-run the proxy contract gate
(`.venv/bin/python -m pytest -q tests/test_harness_probe_contract.py`), and
record the new proxy commit alongside the regenerated release evidence.
