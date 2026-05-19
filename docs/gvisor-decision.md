# 运行时选型决策记录：放弃 Firecracker，选择 gVisor

> 日期：2026-05-19
> 决策人：项目主理 + 本机 Phase 0 排查
> 状态：✅ 已生效，PLAN.md 已按此重写

## 1. 上下文

项目目标是给每个会话起一个独立的隔离环境跑 AI agent（Claude Code / OpenCode）+ Python 工具链，连远端只读 Doris 做数据分析。最初的方案是 Firecracker microVM + vsock + per-VM TAP。Phase 0 在本机（华为云 ECS）做环境检查时，发现 Firecracker 在这台机器上根本装不上，触发了一次彻底的运行时再选型。

## 2. 为什么本机装不了 Firecracker

| 项 | 实测结果 | 含义 |
|---|---|---|
| `dmesg` Hypervisor | `KVM`，sys_vendor `Huawei Cloud`，product `OpenStack Nova` | 当前机器本身就是个 KVM 虚机 |
| `/proc/cpuinfo` flags | 有 `vmmcall / sse4a / amd_dcm / topoext / clzero / xsaveerptr`，**无 `svm`** | 物理 CPU 是 AMD，但 SVM 标志被宿主 CPUID 主动屏蔽 |
| `modprobe kvm_intel` / `kvm_amd` | `Operation not supported` | 内核模块在，硬件能力不足，加载失败 |
| `/dev/kvm` | 不存在 | 直接断绝 Firecracker 启动条件 |

**guest 内部无法自助开启**：嵌套虚拟化是宿主在 vCPU CPUID 里暴露 `vmx`/`svm` 才能用，租户在 guest 里 `modprobe ... nested=1` 没有意义——内核根本看不到硬件支持。华为云文档也明确：默认安全策略禁止嵌套虚拟化，要在控制台或工单里申请支持嵌套虚拟化的规格族（kc1 / c6 / m6 等）。

**即便申请开通也不够**：[Firecracker issue #668](https://github.com/firecracker-microvm/firecracker/issues/668) 明确写 Firecracker **不支持在嵌套虚拟化下运行**，会以 `KVM_EXIT_FAIL_ENTRY` 崩溃。社区里 Intel L1 nested 有少量人跑通，AMD nested SVM 历史口碑很差，押注收益低。

## 3. 候选方案对照

| 方案 | 本机可行 | 隔离强度 | 启动延迟 | 工程复杂度 | 备注 |
|---|---|---|---|---|---|
| **Firecracker on 本机 ECS** | ❌ | 极强 | n/a | n/a | KVM 不可用 |
| **Firecracker on 华为云 BMS** | ✅（需采购裸金属） | 极强 | ~200 ms | 高（vsock 多路复用、TAP、snapshot 流水线） | 最稳但需换机器 |
| **Firecracker on 嵌套 ECS** | ⚠️（需工单 + 重建实例，AMD 风险大） | 极强（理论） | 未知 | 高 + 不确定性 | 不推荐 |
| **Kata Containers** | ❌ | 极强 | ~500 ms | 中 | 同样依赖 KVM |
| **gVisor `runsc` (systrap)** | ✅ | 强（用户态 syscall 隔离） | <100 ms warm | 低 | 本次选型 |
| **runc / 原生 Docker** | ✅ | 弱（共享 kernel） | <50 ms | 极低 | 隔离不足以承受 prompt injection 跑任意代码 |

## 4. gVisor 在本任务的能力对照

### 4.1 平台层

- gVisor 自 2023 起默认平台是 [**systrap**](https://gvisor.dev/blog/2023/04/28/systrap-release)：纯 seccomp `SECCOMP_RET_TRAP` 拦截 syscall，不依赖任何虚拟化指令。这台 ECS 直接装直接跑。
- 本机已确认满足前置条件：`unprivileged_userns_clone=1`、`max_user_namespaces=247758`、cgroup v2、`CONFIG_SECCOMP=y`、fuse 在。
- 唯一缺的是 overlayfs，需要后续用 `fuse-overlayfs` 或 per-session 目录复制（btrfs/rsync）替代。

### 4.2 通信层（与原 Firecracker 方案的对应）

| 原方案（Firecracker） | gVisor 下的等价物 | 工程改动 |
|---|---|---|
| vsock port 10000 跑 agent stdio | orchestrator 直接当 `runsc run` 的父进程，读写其 stdin/stdout | 简化，少写一个 vsock multiplexer |
| vsock port 10001 跑 artifact 事件 | bind-mount `/workspace/<session_id>` 到 host，host 端 `fanotify`/`inotify` 监听，**根本不需要 in-sandbox uploader** | 大幅简化 |
| per-VM TAP + iptables 出站白名单 | gVisor netstack（用户态 TCP/IP）出口走 host veth；nftables 挂在 veth 或 cgroup classid 上 | 等价 |
| bake snapshot → restore（vmstate + memfile） | OCI bundle + `runsc checkpoint`/`restore` + `--warm-sentry` | 等价或更简单 |
| 每会话凭据通过 vsock bootstrap 注入 | OCI `process.env` 在 `runsc run` 时直接塞 | 简化 |
| `rootfs.ext4` + Firecracker init | OCI rootfs 目录（debootstrap 或 skopeo+umoci）；**host 上不跑 Docker** | 工具链更熟 |
| `firecracker-go-sdk` 管 VM 生命周期 | `os/exec` 调 `runsc` | 选择更多 |

### 4.3 启动延迟

最新 [warm sentry mode](https://github.com/google/gvisor/issues/12809)（2026）+ checkpoint/restore，GCE n2-standard-8 实测：

| 场景 | 延迟 |
|---|---|
| Firecracker snapshot restore（原 PLAN 假设） | ~200 ms |
| gVisor cold restore `--network=sandbox` | 160 ms |
| gVisor warm restore（`runsc reset` 复用 sentry） | **79 ms** |
| [`gvisord`](https://github.com/shayonj/gvisord) 池化 | **<50 ms** |

Warm pool 在 gVisor 上是一等公民，**比 Firecracker 还简单**（不用管 vmstate/memfile 文件、不用管 TAP/CID 分配）。

### 4.4 安全模型

- **威胁模型**：内部同事使用，最坏情况是 prompt injection 让 agent 跑恶意代码尝试外发 Doris 数据 / 越权调系统。
- **gVisor 的隔离**：用户态二次实现 Linux syscall（~200 个），sandbox 内 syscall 不直接进 host kernel。
- **相对位置**：比 runc/Docker 强一档（Docker CVE 大头在共享 kernel），比 Firecracker 弱一档（Firecracker 攻击面是 ~50 syscall 白名单 + KVM ioctl）。
- **关键补强**：叠上 host 端 nftables 出站白名单（只放 Doris FE `172.16.0.138:9030` 和本地 LLM proxy `127.0.0.1:8082`），即便 sandbox 被打穿，数据也出不去。
- **结论**：对"内部同事 + 只读 DB + 出站白名单"的具体场景，gVisor 余裕。

## 5. 不选 Docker 的原因

`runsc` 本身就是个 OCI runtime，输入是 OCI bundle：

```
my-sandbox/
├── config.json    ← OCI 运行配置（命令、env、mount、cgroup、netns）
└── rootfs/        ← 一个根文件系统目录
```

`cd my-sandbox && runsc run my-id` 就够了。Docker daemon、镜像层、registry 这些**不是 gVisor 的硬依赖**，只是常见打包工具。

不装 Docker 的好处：

- host 依赖最小（不跑 dockerd / containerd），故障面缩小
- rootfs 是普通目录，per-session copy-on-write 直接用 btrfs/overlay 子卷或 rsync 即可
- 凭据不会沾上 docker 镜像层缓存
- 排查时 strace / lsns / `runsc debug` 都直接对 runsc 进程做

替代方案：用 `debootstrap` 直建 Ubuntu rootfs，或 `skopeo copy docker://python:3.12-slim oci:./img && umoci unpack --image ./img ./bundle` 拿 OCI 标准镜像，全程不需要 Docker daemon。

## 6. 真正的代价与已知风险

1. **`io_uring` 等新 syscall gVisor 实现不全**：Claude Code / Node 18+ 默认 epoll，不踩这个坑；要留意 Python 库不要用 `aio_uring`。Phase 1 用 `runsc --strace` 留一次完整 trace 提前发现 ENOSYS。
2. **vsock 协议方案废弃**：原 PLAN.md 的"双 vsock 通道"重写为 stdio + bind-mount，整体复杂度反而降低。
3. **没有 overlayfs**：要装 `fuse-overlayfs` 或换 rootfs 目录复制方案（btrfs subvol / rsync）。
4. **`runsc` 是 Google 一家维护**：偶有 corner case，但 GKE Sandbox / Cloud Run gen1 是大规模生产用户，长期可信。
5. **隔离强度不及 Firecracker**：未来若要面向外部用户，可以把整套 orchestrator 平移到 BMS 上换回 Firecracker（接口形态需要保持兼容，方便切换）。

## 7. 决策

✅ **本项目第一阶段（Phase 1–6）使用 gVisor `runsc` + `systrap` 平台 + 手工 rootfs 目录 + 无 Docker。**

迁移路径预留：

- orchestrator 设计 `runtime.Driver` 抽象（启动/exec/kill/checkpoint），第一版实现 `runscDriver`，未来可加 `firecrackerDriver`。
- artifact 事件协议、agent stdio 协议与 runtime 无关。
- sandbox network namespace + nftables 出站规则与 runtime 无关。

下一步：进入 Phase 1，装 `runsc` 二进制，手工构造一个最小 rootfs，用单个 sandbox 跑通"Claude Code → Doris → CSV / PNG / report.md"端到端 DEMO。
