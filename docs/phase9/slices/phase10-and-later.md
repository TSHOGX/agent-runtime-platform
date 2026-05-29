# Phase 10 and Later

> Parent: [Phase 9 implementation slices](../implementation-slices.md).

Phase 10 features must land through driver adapters:

```text
10a system prompt -> DriverSystemPromptAdapter
10b compaction    -> DriverCompactionAdapter
10c skills        -> shared /harness-skills mount + DriverSkillsAdapter
10d hooks/MCP     -> DriverPolicyAdapter + DriverMCPAdapter
interrupt         -> DriverControlAdapter
output            -> OutputNormalizer
```

Fanout and base-to-N child branch semantics are not Phase 9 work. They require
provider `branch: true` evidence and a separate object model after
`snapshot_policy.snapshot_semantic == base_branch_fanout` is real.
