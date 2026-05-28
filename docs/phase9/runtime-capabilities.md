# Runtime Capability Vocabulary v1

Runtime capabilities are product-level driver/provider requirements. They are
not Linux capability names and they are not a deny list.

Rules:

- The vocabulary is versioned as `capability_vocab_version: "1"`.
- A driver declares positive requirements.
- A provider declares positive support.
- Omitted provider capability means unsupported.
- Unsupported is explicit: if a driver requires an unsupported capability,
  allocation fails before data-volume provisioning, MountPlan generation, or
  runtime creation.
- Silent no-op behavior is not allowed.

## Vocabulary

| Capability | Semantic | Phase 9 `local_runsc` | Unsupported means |
| --- | --- | --- | --- |
| `exec_stream` | Stream stdout/stderr from a process | yes | Driver cannot run |
| `pty` | Allocate a PTY for a process | yes | TUI or PTY-bound driver cannot run |
| `stdin` | Write to process stdin | yes | One-way output-only drivers only |
| `signal` | Send a signal to a process | yes | No graceful shutdown or interrupt |
| `kill` | Force-kill a process | yes | Cleanup depends only on sandbox destroy |
| `resize_pty` | Resize a PTY | yes | TUI driver must use fixed window size |
| `filesystem_rw` | Writable sandbox filesystem paths | yes | Read-only drivers only |
| `watch` | Filesystem change notifications | no | Polling only |
| `network_policy` | Apply egress allow-list policy | yes | Networked drivers cannot run safely |
| `port_expose` | Expose a sandbox port to the host | no | Service-style drivers unsupported |
| `tunnel` | Public preview URL or tunnel | no | Future only |
| `wake_on_http` | Wake a paused sandbox on HTTP request | no | Future only |
| `metrics` | Provider exposes resource metrics | no | Provider metrics unavailable |
| `logs` | Provider exposes runtime logs | yes | Driver must self-report logs |
| `snapshot_disk` | Disk checkpoint/restore | yes | No checkpoint/restore |
| `snapshot_memory` | Memory snapshot/restore | no | Disk-only restart semantics |
| `branch` | Base sandbox can produce independent children | no | Fanout unavailable |
| `secret_gateway` | Brokered short-lived secret URL | no | Only model proxy path is available |
| `mcp_gateway` | Remote MCP gateway managed by control plane | no | Local or driver-native MCP only |

## Allocation Enforcement

The orchestrator performs the check before allocation:

```text
required = driver.required_runtime_capabilities
provided = {capability | runtime_provider.capabilities[capability] == true}

if required is not subset of provided:
    fail before allocation
```

The failure should name the driver, provider, vocabulary version, and missing
capabilities. It should not allocate runtime resources that then fail later.

## Initial Driver Requirements

These requirements should be verified while implementing the registry. They are
initial targets, not a substitute for driver smoke evidence.

| Driver | Initial required runtime capabilities |
| --- | --- |
| `claude_code` | `exec_stream`, `stdin`, `kill`, `filesystem_rw`, `network_policy`, `logs`, `snapshot_disk` |
| `sh` | `exec_stream`, `pty`, `stdin`, `signal`, `kill`, `resize_pty`, `filesystem_rw`, `logs`, `snapshot_disk` |
| `pi` | `exec_stream`, `pty`, `stdin`, `signal`, `kill`, `filesystem_rw`, `network_policy`, `logs`, `snapshot_disk` |

Pi is listed with `pty` until its pinned RPC mode proves pure pipe operation is
sufficient. If Pi can run without PTY, remove `pty` from the driver spec with
paired smoke evidence.

## Snapshot Semantics

Capability booleans are not enough for snapshot behavior. Contract v2 also
records `snapshot_policy.snapshot_semantic`:

- `generation_checkpoint_restore`: current `local_runsc` behavior. It supports
  crash/restart within the same generation lineage.
- `base_branch_fanout`: future behavior where one base sandbox produces N
  independent children.
- `pause_resume_only`: provider can pause and resume but not checkpoint disk or
  branch.

Phase 9 uses `generation_checkpoint_restore`. Fanout objects and child-result
models belong to a later phase after a provider proves `branch: true`.

## Provider API Families

Phase 9 only needs the spec and capability digest for `local_runsc`, but the
provider contract should reserve stable interface-family names:

| Family | Interface names |
| --- | --- |
| lifecycle | `Create`, `Start`, `Pause`, `Resume`, `Destroy` |
| process | `Exec`, `Stream`, `SendStdin`, `Signal`, `Kill`, `ResizePTY`, `ListProcesses`, `Reconnect` |
| filesystem | `ReadFile`, `WriteFile`, `Stat`, `List`, `Watch`, `Upload`, `Download` |
| network | `ExposePort`, `ClosePort`, `SetEgressPolicy` |
| snapshot | `Checkpoint`, `Restore`, `Snapshot`, `Branch` |

Concrete Go provider interfaces for all families are Phase 11 or later work.
The Phase 9 registry should still use these names so future providers do not
invent incompatible terminology.
