# Phase 8 Mounts and Runtime

`MountPlan` is the only API that produces sandbox content mounts: exact binds
and tmp/cache mounts that expose product data to the sandbox. Runtime code,
Phase 9 content mounts, manifest rendering, restore validation, and release
gates consume the typed plan instead of rebuilding paths locally.

OCI pseudo mounts such as `/proc`, `/dev`, and `/sys` are owned by the
`RuntimeAdapter`, not by product `MountPlan` content binds. They come from a
separate static allow-list in this document and cannot introduce host path binds
or product data.

## Root Layout

Before any `sandbox-isolation-v1` allocation, the worker validates all configured
host roots after symlink-safe canonicalization.

Required canonical roots and reserved host-only roots:

- `<sessions_root>`
- `<agent_homes_root>`
- `<run_dir>`
- checkpoint root
- prepared-bundle root
- rootfs/image content root
- schema-pack root, if configured
- future skills/settings content roots
- DB/control-plane state root
- DataVolume provisioning evidence root, normally under the DB/control-plane
  state root as a reserved host-only subroot
- provider credential root, if file-backed provider credentials are used
- proxy-internal socket root, normally `<run_dir>/proxy-internal`

Configured top-level roots must be mutually disjoint: no top-level root may
equal, contain, or be contained by another top-level root. When the
proxy-internal socket root or DataVolume provisioning evidence root is derived
under another configured top-level root, it is validated as a reserved
host-only subroot rather than a separate top-level root. Generated subroots
under `<run_dir>` are reserved by type: `control`, `bridge`, `logs`, `network`,
and `proxy-internal` must be mutually disjoint siblings. Bundle paths are
validated under the prepared-bundle root, or as a distinct reserved subroot if a
worker explicitly places them under `<run_dir>`.

A sandbox-bindable root means an exact source root that `MountPlan` may expose
to the sandbox. `<run_dir>` itself is not sandbox-bindable; only typed
generation subdirectories such as `control`, `bridge`, and generated network
projection files can become exact bind sources. No sandbox content bind may
equal, contain, or be contained by `proxy-internal`, the DataVolume provisioning
evidence root, or a provider credential root.

The only intentional nested mount path is the empty bridge placeholder
`<run_dir>/control/gen-<generation_id>/bridge`, which is mounted over by the
real bridge source. It is not a trust root and never stores proxy or credential
material.

The orchestrator DB must live outside every sandbox-bindable root.

Phase 8 removes the legacy sandbox secret root contract. Config keys such as
`harness.secrets.root` and `harness.secrets.readers_gid` are rejected by Phase 8
config loading. Any code that still understands those keys is pre-Phase-8
compatibility tooling and is quarantined from release evidence. Old secret roots
are deleted or quarantined outside all Phase 8 roots during cutover. Provider
credentials move to host/proxy-only storage until a future Phase 10 credential
storage design exists.

## Sandbox Content Mounts

| Surface | Sandbox path | Mode | Required options |
| --- | --- | --- | --- |
| Session workspace | `/workspace` | rw exact bind | `nosuid,nodev`; `noexec` only if product-compatible |
| Driver home | `/agent-home` | rw exact bind | `nosuid,nodev`; `noexec` only if product-compatible |
| Control dir | `/harness-control` | ro exact bind | `nosuid,nodev,noexec` |
| Bridge queue | `/harness-control/bridge` | rw exact bind | `nosuid,nodev,noexec` plus runsc exclusive annotation |
| Network alias projection | `/etc/hosts` | ro exact bind, when model access aliasing is enabled | generated file, `nosuid,nodev,noexec`, content digest |
| Schema pack | `/schema-pack` | ro exact bind | `nosuid,nodev,noexec` |
| Phase 9 skills | `/harness-skills` | ro exact bind | `nosuid,nodev,noexec` |
| Phase 9 settings | configured target | ro exact bind | `nosuid,nodev,noexec` |
| Scratch/cache | `/tmp`, `/var/tmp`, tool caches | tmpfs or explicit bind | size limit, `nosuid,nodev`; `noexec` where compatible |

Parent `/sessions` and `/agent-homes` mounts are forbidden. `/harness-secrets`
is forbidden in Phase 8.

## Runtime Pseudo Mounts

`RuntimeAdapter` may add only these OCI pseudo mounts. The adapter owns a pinned
fixture with exact destination, type, source, options, and read/write mode; this
table defines the allowed surface and minimum constraints.

| Destination | Type | Source | Option rule |
| --- | --- | --- | --- |
| `/proc` | `proc` | `proc` | no host source; adapter-pinned safe option list |
| `/dev` | `tmpfs` | `tmpfs` | `nosuid`, size limit, gVisor-compatible device policy |
| `/dev/pts` | `devpts` | `devpts` | `nosuid,noexec`, new instance |
| `/dev/shm` | `tmpfs` | `shm` | `nosuid,nodev,noexec`, size limit |
| `/dev/mqueue` | `mqueue` | `mqueue` | `nosuid,nodev,noexec` |
| `/sys` | `sysfs` | `sysfs` | `ro,nosuid,nodev,noexec` |

Release gates validate sandbox content mounts against `MountPlan` and validate
pseudo mounts against this RuntimeAdapter allow-list. Any additional host bind,
device mount, `/dev/net/tun`, parent root mount, or adapter-generated product
data mount outside the typed `MountPlan` fails the gate.

## Exact Bind Semantics

An exact bind mounts the approved source path itself, not an arbitrary parent.
Use non-recursive bind semantics where supported.

Recursive fallback is allowed only when all are true:

- pre-existing nested mountpoints under every source are rejected;
- propagation is private/slave so later host submounts do not appear in the
  sandbox;
- a release gate creates a host submount after launch and proves the sandbox
  cannot see it;
- workers that cannot enforce those rules reject the plan.

For each source, validation uses symlink-safe traversal from the owning root and
requires the canonical result to equal the derived path.

## Derived Sources

Generation-owned sources are compiled outputs. Persistent DataVolume sources
are selected from verified provisioning rows:

- workspace: verified `session_workspaces[session_id].host_path`
- driver home: verified `session_driver_homes[session_id, driver].host_path`
- control: `<run_dir>/control/gen-<generation_id>`
- bridge: `<run_dir>/bridge/gen-<generation_id>`
- bridge placeholder: `<run_dir>/control/gen-<generation_id>/bridge`
- network alias projection: `<run_dir>/network/gen-<generation_id>/hosts`
- bundle/spec/log/checkpoint paths: generation-derived paths under their own
  disjoint roots

The first provisioning of a workspace or driver home derives the expected path
from configured roots and typed IDs, then writes host-trusted evidence to the
DataVolume row. Later launches, restores, artifact scans, and artifact downloads
load and verify that row instead of reconstructing root-plus-ID paths locally.
Persisted path mirrors outside the DataVolume rows are diagnostics. They are
accepted only when they match the verified canonical value exactly.

## Fresh Provisioning

Phase 8 does not import legacy workspace or agent-home content. Cutover wipes
old active roots, then creates fresh directories from configured roots plus
typed IDs.

Initial Phase 8 provisioning and destructive cutover must:

- reject pre-existing files, non-empty directories, symlink escapes, nested
  mountpoints, cross-session hardlinks, device nodes, FIFOs, sockets,
  capabilities, unsupported ACLs, unsupported xattrs, setuid/setgid bits, and
  root-owned legacy content;
- set ownership and permissions from the configured sandbox UID/GID/groups;
- write host-trusted evidence in both the DB row and a root-owned marker outside
  the sandbox-writable bind source, under the configured DataVolume
  provisioning evidence root.

Launch never trusts markers that the sandbox UID can edit inside `/workspace` or
`/agent-home`, or markers whose canonical path is outside the provisioning
evidence root.

The non-empty-directory rejection applies only when creating a new
`session_workspaces` or `session_driver_homes` row during cutover or first
provisioning. Later generations for the same session select the existing valid
row, verify the canonical path and host-trusted evidence, and preserve
workspace/home contents.

## Nested Bridge Mount

`/harness-control` is read-only and `/harness-control/bridge` is writable. The
host prepares the placeholder before rendering the OCI spec:

- create `<run_dir>/control/gen-<generation_id>/bridge`;
- require it to be an empty directory, not a symlink, not a mountpoint, and not a
  hardlink trick;
- mount `/harness-control` first and `/harness-control/bridge` second;
- store protocol files only in the real bridge source
  `<run_dir>/bridge/gen-<generation_id>`.

The runsc adapter must render:

```json
{
  "destination": "/harness-control/bridge",
  "type": "bind",
  "options": ["bind", "rw", "nosuid", "nodev", "noexec"],
  "annotations": {
    "dev.gvisor.spec.mount./harness-control/bridge.type": "bind",
    "dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive"
  }
}
```

A bridge bind without the pinned gVisor exclusive/share annotation is not a
valid `sandbox-isolation-v1` spec.

## Process Identity

Preferred target: OCI starts the long-lived process directly as the configured
sandbox UID/GID/groups after the host has prepared all mountpoints and
permissions.

Temporary compatibility path: OCI may start a minimal root entrypoint only to
validate mounts and drop privileges. The root phase must not:

- create `/sessions/$SESSION_ID` or `/agent-homes/$SESSION_ID`;
- rewrite `/workspace` or `/agent-home`;
- run `chown -R`;
- read provider credentials;
- read, validate, or materialize any legacy `harness.secrets.root`;
- fall back to hardcoded `65534:65534`;
- keep a root bridge claim loop alive.

Shell, Claude, the bridge claim loop, and turn runners must run as the configured
non-root identity. The rendered OCI spec must also set `noNewPrivileges`, clear
ambient capabilities, and exclude `CAP_NET_ADMIN`, `CAP_NET_RAW`, and
`CAP_SYS_ADMIN` from every capability set.

## Bridge ACLs

The queue is directional:

```text
/harness-control/bridge/
  inbox/       read-only sandbox mount; host publishes from .host/inbox
  host-tmp/    read-only sandbox mount; host temp lives under .host/host-tmp
  outbox/      sandbox publishes, host reads/cleans
  tmp/         sandbox-only temp area
  heartbeat/
    bridge     sandbox-owned heartbeat
  host-heartbeat/
    (empty sandbox placeholder; host heartbeat lives under .host/host-heartbeat)
```

Equivalent layouts are allowed only if:

- the sandbox cannot create, replace, rename, or unlink host-owned inbox/temp or
  host-heartbeat files;
- the host does not depend on world-writable bridge directories;
- each writer publishes via writer-owned temp paths and atomic rename;
- stale-file cleanup is host-owned.

## Read-Only Rootfs

Set the OCI rootfs read-only. Writable surfaces are explicit:

- `/workspace`
- `/agent-home`
- `/harness-control/bridge`
- `/tmp`
- `/var/tmp`
- declared tool caches

Image hygiene gate:

- `/workspace` and `/agent-home` exist as real empty directories, not symlinks;
- `/etc/hosts` exists as a real file, not a symlink, so the generated network
  alias projection can be exact-bound over it when enabled;
- baked `/sessions` and `/agent-homes` are absent, or empty, root-owned,
  non-writable by the sandbox UID, and unused;
- baked `/root/.claude` and `/root/.cache` are absent or proven empty of
  credentials, settings, session state, and behavior-affecting caches;
- release evidence records the inspected image digest.
