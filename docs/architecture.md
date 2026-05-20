# Harness Platform 架构文档

> 最后更新：2026-05-20

## 概述

Harness Platform 是一个基于 gVisor sandbox 的 AI Agent 执行平台。每个会话运行在独立的 gVisor 容器中，Agent（Claude Code）通过 stdin/stdout 与 orchestrator 通信，支持长生命周期容器和 checkpoint/restore 机制。

## 核心架构

### 长生命周期容器模型

当前架构采用**长生命周期容器 + checkpoint 持久化**的混合模型：

```
状态机：
created ──▶ running_active ──▶ running_idle ──30min──▶ checkpointing ──▶ checkpointed
            (处理消息)        (等待消息)                                  (已销毁)
            ▲                                                                │
            └────────────────────── restore ────────────────────────────────┘
```

**优势：**
- 活跃对话：通过 stdin/stdout pipe 通信，延迟 ~10ms
- 上下文持久化：checkpoint 保存完整容器状态
- 资源优化：idle 30 分钟后自动 checkpoint 并释放内存
- 架构简化：容器自包含，无需复杂的 bind mount

### 组件架构

```
┌─────────────────────────────────────────────────────────────┐
│                         Frontend                             │
│                    (Next.js + WebSocket)                     │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP + WebSocket
┌──────────────────────────▼──────────────────────────────────┐
│                      Orchestrator                            │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │   Server    │  │  Event Hub   │  │  Store (SQLite)  │   │
│  │  (HTTP/WS)  │  │  (pub/sub)   │  │  (sessions/msgs) │   │
│  └──────┬──────┘  └──────┬───────┘  └──────────────────┘   │
│         │                │                                   │
│  ┌──────▼────────────────▼──────┐                           │
│  │         Runtime              │                           │
│  │  (Container lifecycle)       │                           │
│  └──────────────┬────────────────┘                          │
└─────────────────┼───────────────────────────────────────────┘
                  │ runsc commands
┌─────────────────▼───────────────────────────────────────────┐
│              gVisor Containers                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Container (harness-sess_xxx)                        │   │
│  │  ┌────────────────────────────────────────────────┐  │   │
│  │  │  Claude CLI (--permission-mode bypassPermissions) │  │
│  │  │  - Interactive mode (stdin/stdout)             │  │   │
│  │  │  - Session workspace: /workspace               │  │   │
│  │  │  - Network: sandbox mode (10.200.1.1 bridge)   │  │   │
│  │  └────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

## 关键技术决策

### 1. 长生命周期容器

**决策：** 容器在消息之间保持运行，通过 stdin/stdout pipe 通信。

**理由：**
- 热路径延迟低（~10ms vs ~200ms restore）
- 保持 Claude CLI 会话上下文
- 简化消息路由逻辑

**实现：**
- `Runtime.containers` map 存储活跃容器
- `sendMessage()` 直接写入容器 stdin
- goroutines 持续读取 stdout/stderr

### 2. Checkpoint/Restore 机制

**决策：** idle 30 分钟后自动 checkpoint 并销毁容器。

**理由：**
- 释放内存资源
- 保持会话上下文（可恢复）
- 平衡性能和资源利用

**实现：**
- `MonitorIdleSessions()` 每 5 分钟检查一次
- `Checkpoint()` 创建 checkpoint 镜像到 `/var/lib/harness/checkpoints/{session_id}`
- `resumeFromCheckpoint()` 使用 `runsc restore` 恢复

### 3. 网络配置

**决策：** 使用 `-network sandbox` 模式，通过宿主机桥接访问服务。

**配置：**
- 宿主机桥接接口：`hv-phase1` (10.200.1.1/24)
- Claude Code proxy：`http://10.200.1.1:8082`
- 沙箱内无法使用 `0.0.0.0` 或 `127.0.0.1` 访问宿主机

**CCA 配置（烘焙到镜像）：**
```json
{
  "env": {
    "ANTHROPIC_AUTH_TOKEN": "123",
    "ANTHROPIC_BASE_URL": "http://10.200.1.1:8082"
  },
  "alwaysThinkingEnabled": true,
  "enabledPlugins": {
    "clangd-lsp@claude-plugins-official": true,
    "pyright-lsp@claude-plugins-official": true
  }
}
```

### 4. 权限模式

**决策：** Claude CLI 使用 `--permission-mode bypassPermissions`。

**理由：**
- 沙箱内环境已隔离，无需额外权限检查
- 简化用户交互，无需手动批准工具调用
- 提升响应速度

### 5. 输出流架构（当前问题）

**当前实现：** goroutines 使用第一次 `Start()` 调用时传入的 callback。

**问题：** 后续消息通过 `sendMessage()` 传入的新 callback 无法接收输出。

**原因：** goroutines 在 `startFresh()` 中启动，持续使用初始 `emit` 闭包，不感知后续调用的新 callback。

**影响：** 多轮对话时，第二条及后续消息的输出无法到达 stream parser。

## 数据模型

### Session 状态

```go
type Session struct {
    ID                string
    UserID            string
    Status            string  // created, running_active, running_idle, checkpointing, checkpointed, failed, destroyed
    Agent             string
    Workspace         string
    RestoreID         string
    ClaudeSessionUUID string
    RestoreMS         *int64
    LastActivityAt    *time.Time
    CheckpointPath    string
    CreatedAt         time.Time
    UpdatedAt         time.Time
}
```

### Container 结构

```go
type Container struct {
    SessionID string
    RestoreID string
    Cmd       *exec.Cmd
    Stdin     io.WriteCloser
    Stdout    io.ReadCloser
    Stderr    io.ReadCloser
    Cancel    context.CancelFunc
}
```

## 关键流程

### 1. 创建新会话并发送首条消息

```
1. POST /api/sessions → 创建 session (status: created)
2. POST /api/sessions/{id}/messages → 发送消息
3. Server.sendMessage() 检查状态，更新为 running_active
4. Server.runSession() 调用 Runtime.Start()
5. Runtime.startFresh():
   - 写入 control file 到 /var/lib/harness/control/phase2-template/session.env
   - 执行 runsc run 启动容器
   - 启动 goroutines 读取 stdout/stderr
   - 写入首条消息到 stdin
6. Stream parser 解析输出，发布事件到 Event Hub
7. 完成后更新状态为 running_idle
```

### 2. 发送后续消息（热路径）

```
1. POST /api/sessions/{id}/messages
2. Server.sendMessage() 检查状态 (running_idle)
3. Server.runSession() 调用 Runtime.Start()
4. Runtime.sendMessage():
   - 从 containers map 获取已有容器
   - 写入消息到 stdin
   - ⚠️ 问题：输出仍发送到旧 callback，新 callback 收不到
5. 状态更新为 running_idle
```

### 3. Checkpoint 流程

```
1. MonitorIdleSessions() 检测到 session idle > 30 分钟
2. 更新状态为 checkpointing
3. Runtime.Checkpoint():
   - 从 containers map 移除容器
   - 执行 runsc checkpoint -image-path {checkpoint_path}
   - 执行 runsc delete 清理容器
4. 更新状态为 checkpointed
```

### 4. 从 Checkpoint 恢复

```
1. POST /api/sessions/{id}/messages (status: checkpointed)
2. Runtime.resumeFromCheckpoint():
   - 执行 runsc restore -image-path {checkpoint_path}
   - 重新建立 stdin/stdout/stderr pipes
   - 启动 goroutines 读取输出
   - 写入消息到 stdin
3. 容器恢复运行，状态更新为 running_idle
```

## 已知问题

### 1. 输出流路由问题（核心问题）

**症状：** 第二条及后续消息发送后，前端收不到 Agent 输出。

**根因：** 
- `startFresh()` 中的 goroutines 使用初始 `emit` 闭包
- `sendMessage()` 传入的新 `output` callback 从未被调用
- 输出持续发送到第一次调用的 callback，但该 callback 对应的 stream parser 已结束

**影响范围：** 所有多轮对话场景

**临时缓解：** 无

**推荐方案：** 实现 per-container Event Hub 架构（见下文）

## 推荐优化方案

### Per-Container Event Hub 架构

**目标：** 解耦输出生产者（goroutines）和消费者（stream parser），支持多个订阅者。

**设计：**

```go
type OutputEvent struct {
    Stream string
    Line   string
}

type OutputHub struct {
    mu          sync.RWMutex
    subscribers map[chan OutputEvent]struct{}
}

type Container struct {
    // ... 现有字段 ...
    OutputHub *OutputHub  // 新增
}
```

**实现步骤：**

1. **Container 添加 OutputHub**
   - 每个 Container 创建时初始化 OutputHub
   - goroutines 发布到 hub 而非调用 callback

2. **修改 scanLines()**
   ```go
   func scanLines(wg *sync.WaitGroup, r io.Reader, stream string, hub *OutputHub) {
       defer wg.Done()
       scanner := bufio.NewScanner(r)
       for scanner.Scan() {
           hub.Publish(OutputEvent{Stream: stream, Line: scanner.Text()})
       }
   }
   ```

3. **修改 sendMessage()**
   ```go
   func (r *Runtime) sendMessage(ctx context.Context, container *Container, message string, output func(Output)) Result {
       // 订阅容器的 output hub
       outputCh, cancel := container.OutputHub.Subscribe()
       defer cancel()
       
       // 转发事件到 callback
       go func() {
           for event := range outputCh {
               if output != nil {
                   output(Output{Stream: event.Stream, Line: event.Line})
               }
           }
       }()
       
       // 写入消息
       fmt.Fprintln(container.Stdin, message)
       return Result{}
   }
   ```

4. **修改 startFresh()**
   - 创建 OutputHub
   - goroutines 发布到 hub
   - 订阅 hub 并转发到初始 callback

**优势：**
- 每次 `runSession()` 都能接收到输出
- 支持多个订阅者（如监控、日志）
- 生产者和消费者完全解耦
- 容器生命周期内输出持续可用

## 文件结构

```
orchestrator/
├── cmd/orchestrator/
│   └── main.go                    # 入口，启动 HTTP server 和监控
├── internal/
│   ├── config/
│   │   └── config.go              # 配置加载
│   ├── events/
│   │   └── hub.go                 # 全局事件 pub/sub
│   ├── runtime/
│   │   └── runtime.go             # 容器生命周期管理
│   ├── server/
│   │   ├── server.go              # HTTP/WebSocket handlers
│   │   └── stream_parser.go      # Claude stream-json 解析
│   └── store/
│       ├── store.go               # SQLite 数据访问
│       └── store_test.go          # 单元测试
```

## 配置

### 环境变量

- `RUNSC_ROOT`: runsc 状态目录（默认：`/var/run/runsc`）
- `SESSIONS_ROOT`: 会话工作区根目录（默认：`/var/lib/harness/sessions`）
- `CHECKPOINTS_ROOT`: checkpoint 镜像目录（默认：`/var/lib/harness/checkpoints`）
- `BUNDLE_ROOT`: OCI bundle 目录（默认：`{repo}/bundle/out`）
- `DEFAULT_AGENT`: 默认 agent（默认：`claude`）

### 数据库

SQLite 数据库位于 `/var/lib/harness/orchestrator.db`，包含：
- `sessions` 表：会话元数据
- `messages` 表：消息历史
- `artifacts` 表：artifact 元数据

## 性能指标

- **冷启动（首条消息）**：~200ms（runsc run）
- **热路径（后续消息）**：~10ms（stdin write）
- **Checkpoint 恢复**：~200ms（runsc restore）
- **Idle 超时**：30 分钟
- **监控周期**：5 分钟

## 安全考虑

1. **沙箱隔离**：gVisor 提供 syscall 级别隔离
2. **网络隔离**：sandbox 模式，仅通过桥接访问指定服务
3. **权限模式**：bypassPermissions 仅在沙箱内有效
4. **资源限制**：通过 OCI config 设置 cgroup 限制（待实现）
5. **超时保护**：idle 超时自动清理

## 未来优化

1. **实现 per-container Event Hub**（优先级：高）
2. **添加资源限制**（CPU/内存 cgroup）
3. **优化 checkpoint 策略**（基于活跃度动态调整）
4. **添加监控指标**（Prometheus）
5. **实现 warm pool**（预热容器池）
6. **支持多 agent**（OpenCode 等）
