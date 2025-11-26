# SDQ - Simple Delay Queue

简洁高效的延迟队列，基于 Go 实现，受 beanstalkd 启发。

## 特性

- ✅ **多 Topic 隔离** - 支持多个命名队列
- ✅ **优先级调度** - 数字越小优先级越高
- ✅ **延迟执行** - 支持任务延迟执行和指定时间执行（一次性）
- ✅ **TTR 保护** - Time-To-Run 机制防止任务丢失
- ✅ **任务管理** - 支持 Bury/Kick/Release/Touch 操作
- ✅ **持久化** - 可选的 Memory/SQLite 存储
- ✅ **高性能** - 批量写入优化，内存队列调度
- ✅ **Pull 模式** - Worker 主动拉取，支持多 worker 并发
- ✅ **类型安全** - 高级 Task API 提供编译时类型检查

## 快速开始

### 安装

```bash
go get go-slim.dev/sdq
```

### 基础用法

```go
package main

import (
    "context"
    "time"
    "go-slim.dev/sdq"
)

func main() {
    // 创建队列
    config := sdq.DefaultConfig()
    config.Storage = sdq.NewMemoryStorage() // 或 NewSQLiteStorage("./jobs.db")

    q, _ := sdq.New(config)
    _ = q.Start()
    defer q.Stop()

    // 发布任务
    jobID, _ := q.Put("email", []byte("send email to user@example.com"),
        10,              // priority (数字越小优先级越高)
        0,               // delay (0 表示立即执行)
        60*time.Second,  // TTR (超时时间)
    )

    // Worker 消费任务
    job, _ := q.Reserve([]string{"email"}, 5*time.Second)

    // 处理任务
    body := job.Body()
    println(string(body))

    // 完成任务
    _ = job.Delete()
}
```

### 高级 Task API

提供类型安全、自动序列化的高级接口：

```go
package main

import (
    "context"
    "go-slim.dev/sdq"
    "go-slim.dev/sdq/task"
)

// 定义任务数据结构
type EmailTask struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

// 注册任务处理器
var sendEmail = task.Register("send-email", &task.Config{
    Handler: func(ctx context.Context, data EmailTask) error {
        // 发送邮件逻辑
        println("Sending email to:", data.To)
        return nil
    },
    Priority: 10,
    TTR:      60 * time.Second,
})

func main() {
    // 创建队列
    config := sdq.DefaultConfig()
    q, _ := sdq.New(config)
    _ = q.Start()
    defer q.Stop()

    // 设置队列
    sendEmail.SetQueue(q)

    // 发布任务（自动序列化）
    _ = sendEmail.Publish(context.Background(), EmailTask{
        To:      "user@example.com",
        Subject: "Hello",
        Body:    "Welcome!",
    })

    // 启动 Worker（自动反序列化并调用 handler）
    worker := task.NewWorker(q, "send-email")
    _ = worker.Start(3) // 3 个并发 worker
    defer worker.Stop()

    // Worker 会自动处理任务
}
```

## 核心概念

### Topic (队列)

Topic 是命名队列，用于隔离不同类型的任务（类似 beanstalkd 的 tube）。

**特性：**

- 每个 topic 是一个独立的命名队列
- 自动创建，无需预先定义
- 持久存在，不会自动删除
- 支持多 worker 并发消费同一 topic

**命名规则：**

- 允许字符：字母（a-z, A-Z）、数字（0-9）、下划线（\_）、中划线（-）
- 长度限制：1-200 字符
- 不能为空字符串

**示例：**

```go
// 有效的 topic 名称
"email", "sms_queue", "order-processing", "user_2024", "default"

// 无效的 topic 名称
"email@queue", "queue#1", "中文队列", "" // 空字符串
```

### Job 状态

**Pull Mode: Worker 主动通过 Reserve 拉取任务**

```text
                           Put
                            |
                    +---------------+
                    |   Enqueued    | (initial, transient)
                    +-------+-------+
                            |
                   Persist to Storage
                            |
                +-----------+-----------+
                |                       |
           delay=0                  delay>0
                |                       |
                v                       v
        +---------------+       +--------------+
        |     Ready     |       |   Delayed    |
        +-------+-------+       +------+-------+
                |                      |
                |                      | Expire (Ticker)
                |                      |
                |              +-------+
                |              |
                v              v
        +------------------------+
        |        Ready           |
        +----------+-------------+
                   |
                   | Reserve (Worker pulls)
                   |
                   v
        +-------------------+
        |     Reserved      | (Worker processing)
        +----------+--------+
                   |
      +------------+------------+-----------+
      |            |            |           |
   Delete      Release        Bury     TTR timeout
      |            |            |           |
      |            |            |           |
      v            |            |           |
    [Done]         |            |           |
                   |            |           |
           +-------+-------+    |           |
           |               |    |           |
      delay=0         delay>0   |           |
           |               |    |           |
           v               v    v           |
        +------+  +----------+  +----------+
        |Ready |  | Delayed  |  | Buried   |
        +------+  +----------+  +----+-----+
           ^                         |
           |                         |
           |          Kick           |
           +-------------------------+
```

**状态说明：**

| 状态         | 说明                   | 存储位置          |
| ------------ | ---------------------- | ----------------- |
| **Enqueued** | 已入队，等待加载到内存 | Storage (临时)    |
| **Ready**    | 就绪，等待 Reserve     | Topic.ReadyHeap   |
| **Delayed**  | 延迟中，等待到期       | Topic.DelayedHeap |
| **Reserved** | Worker 处理中          | Topic.ReservedMap |
| **Buried**   | 已埋葬，暂时搁置       | Topic.BuriedHeap  |

**状态转换详解：**

- **Put** → 创建任务，初始为 Enqueued 状态
- **Enqueued** → 持久化后立即转为 Ready 或 Delayed
- **Delayed** → 到期后由 Ticker 自动转为 Ready
- **Ready** → Worker 调用 Reserve 主动拉取（Pull 模式）
- **Reserved** → Worker 处理中，受 TTR 保护
- **TTR timeout** → 超时后自动转回 Ready，防止任务丢失
- **Delete** → 任务完成，从系统中删除
- **Release** → 重新入队（可设置新的 delay）
- **Bury** → 暂时搁置，需要手动 Kick 才能恢复
- **Touch** → 延长 TTR（不改变状态）

### 操作

**基础操作：**

- `Put` - 发布任务
- `Reserve` - 获取任务（阻塞等待）
- `Delete` - 删除任务
- `Release` - 释放任务（重新入队或延迟）
- `Bury` - 埋葬任务
- `Kick` - 唤醒埋葬的任务
- `Touch` - 延长 TTR

**查询操作：**

- `Peek` - 查看任务详情
- `PeekReady/PeekDelayed/PeekBuried` - 查看队列头部
- `Stats/StatsTopic/StatsJob` - 获取统计信息
- `ListTopics` - 列出所有 topic

## 配置

### 基础配置

```go
config := sdq.Config{
    // === 基础参数 ===
    DefaultTTR:       60 * time.Second,  // 默认超时时间
    MaxJobSize:       64 * 1024,         // 最大任务大小 64KB

    // === Touch 限制配置 ===
    MaxTouches:           10,                        // Touch 最大次数
    MaxTouchDuration:     10 * 60 * time.Second,    // 累计最大延长时间 (10分钟)
    MinTouchInterval:     5 * time.Second,          // 最小 Touch 间隔

    // === Topic 配置 ===
    MaxTopics:            0,                         // 最大 topic 数量，0 表示无限制
    MaxJobsPerTopic:      0,                         // 每个 topic 最大任务数，0 表示无限制
    EnableTopicCleanup:   false,                     // 是否启用 Topic 自动清理
    TopicCleanupInterval: 1 * time.Hour,            // Topic 清理间隔（默认 1 小时）
}

// 或者使用默认配置
config := sdq.DefaultConfig()
```

**配置说明：**

- **DefaultTTR**: 任务的默认执行超时时间，Put 时如果未指定 TTR 则使用此值
- **MaxJobSize**: 任务 Body 的最大字节数，超过此大小的任务将被拒绝
- **MaxTouches**: 单个任务允许 Touch 的最大次数，防止任务无限延长
- **MaxTouchDuration**: 单个任务累计可延长的最大时间
- **MinTouchInterval**: 两次 Touch 之间的最小间隔，防止频繁 Touch
- **MaxTopics**: 最大 topic 数量限制，0 表示无限制
- **MaxJobsPerTopic**: 每个 topic 的最大任务数，0 表示无限制
- **EnableTopicCleanup**: 是否启用定期清理空 topic（默认关闭）
- **TopicCleanupInterval**: 清理空 topic 的检查间隔

### Storage 配置

**Memory Storage:**

```go
storage := sdq.NewMemoryStorage()
```

**SQLite Storage:**

SQLite Storage 使用 `database/sql` 标准接口，需要用户自己引入 SQLite 驱动。这样可以根据需求选择是否使用 CGO。

**选项 1：使用 CGO 驱动（推荐，性能更好）**

```bash
go get github.com/mattn/go-sqlite3
```

```go
import (
    _ "github.com/mattn/go-sqlite3"  // 引入 SQLite 驱动
    "go-slim.dev/sdq"
)

// 默认配置
storage, _ := sdq.NewSQLiteStorage("./jobs.db")

// 自定义批量配置（用于性能调优）
storage, _ := sdq.NewSQLiteStorage("./jobs.db",
    sdq.WithMaxBatchSize(500),           // 批量操作最大数量
    sdq.WithMaxBatchBytes(8*1024*1024),  // 批量最大字节数
)
```

**选项 2：使用纯 Go 驱动（无需 CGO）**

```bash
go get modernc.org/sqlite
```

```go
import (
    _ "modernc.org/sqlite"  // 引入纯 Go SQLite 驱动
    "go-slim.dev/sdq"
)

storage, _ := sdq.NewSQLiteStorage("./jobs.db")
```

**配置建议：**

- 高并发场景：增大 `MaxBatchSize` 和 `MaxBatchBytes`
- 低延迟场景：减小批量参数
- 大任务场景：根据任务平均大小调整 `MaxBatchBytes`
- 跨平台编译：使用 `modernc.org/sqlite`（纯 Go，无需 CGO）
- 性能优先：使用 `github.com/mattn/go-sqlite3`（CGO，性能更好）

### Ticker 配置

```go
// 使用默认 Ticker (自动选择)
config.Ticker = nil

// 使用 TimeWheel Ticker (固定时间轮询)
config.Ticker = sdq.NewTimeWheelTicker(10*time.Millisecond, 100)

// 使用 DynamicSleep Ticker (动态睡眠)
config.Ticker = sdq.NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
```

## Task API

Task API 提供类型安全、自动序列化的高级接口，简化任务的注册、发布和处理。

### 注册任务

```go
var processOrder = task.Register("process-order", &task.Config{
    Handler: func(ctx context.Context, data OrderData) error {
        // 处理订单
        return nil
    },
    Priority:   10,              // 默认优先级
    TTR:        60 * time.Second, // 默认超时时间
    MaxRetries: 3,                // Worker 会自动处理重试，达到此次数后埋葬任务
})
```

**Task.Config 字段说明：**

- **Handler**: 任务处理函数，签名必须是 `func(ctx context.Context, data T) error`
- **FallbackHandler**: 失败处理函数（可选），当 Handler 返回错误时调用
- **Priority**: 默认优先级（数字越小优先级越高）
- **TTR**: 默认超时时间
- **MaxRetries**: 最大重试次数（可选，Worker 会自动处理重试，达到此次数后埋葬任务）

### 发布任务

```go
// 立即执行
_ = processOrder.Publish(ctx, OrderData{OrderID: 123})

// 延迟执行
_ = processOrder.Publish(ctx, OrderData{OrderID: 124},
    task.Delay(5*time.Minute))

// 指定时间执行
_ = processOrder.Publish(ctx, OrderData{OrderID: 125},
    task.DelayUntil(time.Now().Add(1*time.Hour)))

// 自定义优先级
_ = processOrder.Publish(ctx, OrderData{OrderID: 126},
    task.Priority(1))
```

### Worker

`task.Worker` 是任务执行的核心，它负责从队列中拉取任务、执行处理函数、并根据执行结果和任务配置自动管理重试和失败策略。

#### 工作原理

1.  **并发消费**：`worker.Start(N)` 会启动 `N` 个并发的 Goroutine 来持续从队列中 `Reserve` 任务。
2.  **任务超时控制 (TTR)**：`Worker` 会根据任务注册时配置的 `TTR` (`Time-To-Run`) 为每个任务创建一个独立的上下文 (`context.WithTimeout`)。这确保了任务在服务器的 TTR 到期之前，`Worker` 能主动检测并处理任务超时。
    - 如果任务在 `TTR` 设定的时间内未能完成，`Worker` 会捕获 `context.DeadlineExceeded` 错误，并将其视为一次任务处理失败。
3.  **智能重试**：
    - 当任务处理器返回错误（包括 TTR 超时）时，`Worker` 不会立即丢弃任务。
    - 它会检查 `job.Meta.Releases`（任务已经被 `Release` 的次数）是否小于 `task.Config.MaxRetries`。
    - 如果**还有重试机会**，`Worker` 会调用 `job.Release()` 将任务重新放回队列。此时，`Worker` 会自动计算一个**指数退避 (Exponential Backoff)** 延迟（每次失败后等待的时间会逐渐增长，例如 5s, 10s, 20s...），以避免对下游服务造成冲击，并给系统恢复时间。
    - 如果 `job.Meta.Releases` 已经**达到或超过** `task.Config.MaxRetries`，`Worker` 会调用 `job.Bury()` 将任务埋葬。被埋葬的任务将不再被自动调度，需要手动 `kick` 才能恢复，或进行人工排查。
4.  **成功处理**：如果任务处理器成功执行且未返回错误，`Worker` 会调用 `job.Delete()` 将任务从队列中彻底移除。

#### 使用示例

```go
// 启动 Worker 处理多个任务类型
worker := task.NewWorker(q,
    "process-order",
    "send-notification",
    "generate-report",
)

// 启动 5 个并发 worker
// 这些 Worker 会自动根据任务配置的 MaxRetries 和 TTR 处理任务的生命周期
_ = worker.Start(5)
defer worker.Stop()
```

defer worker.Stop()

````

## 持久化与恢复

### 数据分离存储

SDQ 采用**元数据与数据分离**的存储架构，显著提升性能和可扩展性。

**问题背景：**

- 任务数据（Body）大小不可控，可能达到 KB 甚至 MB
- 启动时加载所有任务会消耗大量内存
- 频繁读取大 Body 影响性能

**解决方案：**

```text
Job Metadata (轻量级，约 200 字节):
├── ID, Topic, Priority
├── State, Delay, TTR
├── 时间戳 (CreatedAt, ReadyAt, ReservedAt, etc.)
└── 统计信息 (Reserves, Timeouts, Releases, etc.)

Job Body (可能很大):
└── Body []byte (按需加载)
````

**存储策略：**

```text
启动时：
1. 只加载所有 JobMeta 到内存
2. 重建队列结构（Ready/Delayed/Reserved/Buried）
3. Body 延迟加载（Reserve 时才读取）

运行时：
Put:     SaveJobMeta + SaveJobBody → 加载 Meta 到内存队列
Reserve: 从内存队列获取 Meta → 按需加载 Body
Delete:  DeleteJobMeta + DeleteJobBody
```

**性能优势：**

| 场景                      | 传统方式              | 分离存储              |
| ------------------------- | --------------------- | --------------------- |
| 100K 任务，平均 10KB Body | 1GB 内存              | 20MB 内存             |
| 启动时间                  | 慢（需加载所有 Body） | 快（只加载 Meta）     |
| 调度性能                  | 受 Body 大小影响      | 只操作 Meta，性能稳定 |

### 自动恢复

启动时自动从 Storage 恢复任务：

```go
q, _ := sdq.New(config)
_ = q.Start() // 自动异步恢复任务

// 可选：等待恢复完成
err := q.WaitForRecovery(30 * time.Second)
if err == sdq.ErrTimeout {
    log.Println("Recovery timeout, but queue is operational")
}

// 可选：监听恢复进度
_ = q.StartWithOptions(sdq.StartOptions{
    RecoveryCallback: func(progress *sdq.RecoveryProgress) {
        if progress.Phase == sdq.RecoveryPhaseComplete {
            log.Printf("Recovered %d jobs in total", progress.TotalJobs)
            log.Printf("Loaded: %d, Failed: %d",
                progress.LoadedJobs, progress.FailedJobs)
        }
    },
})
```

**快速启动流程：**

```text
1. 快速获取 MaxID，初始化 ID 生成器（毫秒级）
   ↓
2. 立即返回，队列开始服务（新任务可以 Put/Reserve）
   ↓
3. 后台异步恢复任务（加载 JobMeta）
   ↓
4. 按状态分配到对应队列：
   - Enqueued  → 重新入队
   - Ready     → Topic.ReadyHeap
   - Delayed   → Topic.DelayedHeap
   - Reserved  → 转为 Ready（崩溃恢复）
   - Buried    → Topic.BuriedHeap
   ↓
5. 恢复完成，旧任务也可以被 Reserve
```

### 数据持久化

**Storage 类型：**

| Storage    | 持久化        | 性能 | 使用场景               |
| ---------- | ------------- | ---- | ---------------------- |
| **Memory** | ❌ 不持久化   | 最高 | 开发测试、临时队列     |
| **SQLite** | ✅ 自动持久化 | 高   | 生产环境、需要崩溃恢复 |

**SQLite Storage 特性：**

- WAL 模式：读写并发，性能优秀
- 批量写入：内置 batchSaveLoop 自动合并并发请求
- 崩溃安全：支持自动恢复

**恢复策略：**

| 原状态   | 恢复后状态       | 说明                       |
| -------- | ---------------- | -------------------------- |
| Enqueued | Ready 或 Delayed | 重新分配到队列             |
| Ready    | Ready            | 保持原状态                 |
| Delayed  | Delayed          | 保持原状态，继续等待到期   |
| Reserved | Ready            | 崩溃前处理中的任务重新分配 |
| Buried   | Buried           | 保持埋葬状态               |

**数据一致性保证：**

- Put 操作：先持久化 Meta 和 Body，再加入内存队列
- Delete 操作：先从内存队列移除，再删除持久化数据
- 崩溃恢复：Reserved 任务自动转为 Ready，防止任务丢失

## 错误处理

```go
var (
    // 任务相关错误
    ErrNotFound         // 任务不存在
    ErrNotReserved      // 任务未被保留
    ErrNotBuried        // 任务未被埋葬
    ErrInvalidState     // 任务状态无效
    ErrJobExists        // 任务已存在（SQLite Storage）

    // Topic 相关错误
    ErrInvalidTopic     // topic 名称无效
    ErrTopicRequired    // topic 不能为空
    ErrMaxTopicsReached // 达到最大 topic 数量

    // 操作相关错误
    ErrTimeout          // 操作超时
    ErrMaxJobsReached   // 达到最大任务数量

    // Touch 相关错误
    ErrTouchLimitExceeded // Touch 次数超限
    ErrInvalidTouchTime   // Touch 时间无效

    // Reserve 相关错误
    ErrTooManyWaiters   // 等待队列已满
)
```

**错误处理示例：**

```go
// 检查特定错误
job, err := q.Reserve([]string{"email"}, 5*time.Second)
if err == sdq.ErrTimeout {
    // 超时，没有可用任务
    log.Println("No jobs available")
    return
}
if err == sdq.ErrNotFound {
    // 任务不存在
    log.Println("Job not found")
    return
}

// 处理 Touch 错误
err = q.Touch(jobID)
if err == sdq.ErrTouchLimitExceeded {
    // Touch 次数超限，无法继续延长
    log.Println("Touch limit reached")
}
```

## 使用场景

### 异步任务处理

```go
// 发送邮件
emailTask.Publish(ctx, EmailData{...})

// 生成报表
reportTask.Publish(ctx, ReportData{...}, task.Priority(20))

// 数据同步
syncTask.Publish(ctx, SyncData{...}, task.Delay(5*time.Minute))
```

### 延迟执行

```go
// 1 小时后执行
cleanupTask.Publish(ctx, CleanupData{...}, task.Delay(1*time.Hour))

// 指定时间执行（一次性）
executeAt := time.Date(2024, 12, 25, 0, 0, 0, 0, time.Local)
cleanupTask.Publish(ctx, CleanupData{...}, task.DelayUntil(executeAt))
```

**注意：SDQ 是延迟队列，不是定时任务调度器**

- ✅ 支持：延迟 N 秒/分钟/小时后执行
- ✅ 支持：在指定时间点执行（一次性）
- ❌ **不支持**：周期性任务（如每天凌晨执行）
- ❌ **不支持**：cron 表达式

**实现周期性任务的方法：**

```go
// 方式 1：在任务处理器中重新提交
var dailyCleanup = task.Register("daily-cleanup", &task.Config{
    Handler: func(ctx context.Context, data CleanupData) error {
        // 执行清理
        err := performCleanup(data)

        // 处理完后，提交明天的任务
        tomorrow := time.Now().Add(24 * time.Hour).Truncate(24 * time.Hour)
        _ = dailyCleanup.Publish(ctx, data, task.DelayUntil(tomorrow))

        return err
    },
})

// 方式 2：使用专门的定时任务库（推荐）
// 使用 robfig/cron 等库来调度，用 SDQ 执行
c := cron.New()
c.AddFunc("0 0 * * *", func() {
    // 每天凌晨提交清理任务到 SDQ
    cleanupTask.Publish(ctx, CleanupData{...})
})
c.Start()
```

### 重试机制

`sdq/task` 模块内置了强大的自动重试机制，您无需手动编写复杂的重试逻辑。`task.Worker` 会根据您在注册任务时 `Task.Config` 中设定的 `MaxRetries` 字段，自动处理任务失败后的重试流程。

**工作原理：**

1.  **配置 `MaxRetries`**：在 `task.Register` 时，为任务配置 `MaxRetries`（例如 `MaxRetries: 3` 表示最多重试 3 次）。
2.  **错误捕获**：当 `Task` 的 `Handler` 返回错误，或者任务因 `TTR` (Time-To-Run) 超时而被 `Worker` 主动终止时，`Worker` 会捕获此失败。
3.  **判断重试**：`Worker` 会检查任务的 `job.Meta.Releases` 计数（即任务已被 `Release` 的次数）是否小于 `MaxRetries`。
4.  **智能 `Release`**：
    - 如果 `job.Meta.Releases < MaxRetries`，`Worker` 会将任务 `Release` 回队列。为了避免对下游服务造成冲击，`Worker` 会自动计算一个**指数退避 (Exponential Backoff)** 延迟时间（例如第一次失败后等待 5 秒，第二次 10 秒，第三次 20 秒，依此类推），然后将任务放入延迟队列。
    - 每次 `Release` 后，`job.Meta.Releases` 计数会自动递增。
5.  **自动 `Bury`**：如果 `job.Meta.Releases` 达到或超过 `MaxRetries`，`Worker` 会将任务 `Bury`（埋葬）。被埋葬的任务将不再被自动调度，需要人工介入（例如通过 `kick` 命令）进行排查和恢复。

**您需要做的：**

- 在 `task.Config` 中配置 `MaxRetries`。
- 确保您的 `Handler` 在业务逻辑失败时返回 `error`。
- `Task Worker` 会自动处理后续的重试和埋葬。

**示例（注册时配置 MaxRetries）：**

```go
var processOrder = task.Register("process-order", &task.Config{
    Handler: func(ctx context.Context, data OrderData) error {
        // ... 任务处理逻辑 ...
        // 如果处理失败，返回 error
        return fmt.Errorf("模拟处理失败")
    },
    MaxRetries: 5, // Worker 会自动处理最多 5 次重试
    TTR:        30 * time.Second, // 任务处理超时时间
})

// Worker 消费此任务，将自动处理重试逻辑
// 无需在 Worker 中手动编写 job.Release() 或 job.Bury()
worker := task.NewWorker(q, "process-order")
_ = worker.Start(1)
defer worker.Stop()
```

    }

    // 处理成功
    _ = job.Delete()

}

````

### 失败处理

```go
// Worker 处理失败时
if err := job.Delete(); err != nil {
    // 任务已删除或不存在
}

// 处理失败，重新入队
if err := process(job.Body()); err != nil {
    _ = job.Release(job.Meta.Priority, 5*time.Minute) // 5 分钟后重试
    return
}

// 无法处理，埋葬等待人工介入
if isCriticalError(err) {
    _ = job.Bury(job.Meta.Priority)
    return
}
```

## 监控与统计

```go
// 整体统计
stats := q.Stats()
fmt.Printf("Topics: %d, Total Jobs: %d\n", stats.Topics, stats.TotalJobs)

// Topic 统计
topicStats, _ := q.StatsTopic("email")
fmt.Printf("Ready: %d, Delayed: %d, Reserved: %d\n",
    topicStats.ReadyJobs,
    topicStats.DelayedJobs,
    topicStats.ReservedJobs)

// 任务统计
jobMeta, _ := q.StatsJob(jobID)
fmt.Printf("State: %s, Priority: %d, Reserves: %d\n",
    jobMeta.State,
    jobMeta.Priority,
    jobMeta.Reserves)

// 等待的 Worker 统计
waitingStats := q.StatsWaiting()
for _, ws := range waitingStats {
    fmt.Printf("Topic: %s, Waiting Workers: %d\n",
        ws.Topic, ws.WaitingWorkers)
}
```

## 最佳实践

### 1. 使用 Task API

```go
// ✅ 推荐：类型安全，自动序列化
var emailTask = task.Register("send-email", &task.Config{
    Handler: func(ctx context.Context, data EmailData) error {
        return sendEmail(data)
    },
})
emailTask.Publish(ctx, EmailData{To: "user@example.com"})

// ❌ 不推荐：手动序列化，容易出错
body, _ := json.Marshal(EmailData{...})
q.Put("send-email", body, 10, 0, 60*time.Second)
```

### 2. 合理设置 TTR

```go
// 根据任务处理时间设置合理的 TTR
config.DefaultTTR = 5 * time.Minute  // 大多数任务 5 分钟内完成

// 长时间任务使用 Touch 延长
for i := 0; i < 10; i++ {
    processChunk(i)
    if i < 9 {
        _ = job.Touch() // 延长 TTR
    }
}
```

### 3. 控制并发

```go
// 根据系统资源调整 worker 数量
worker := task.NewWorker(q, "process-order")

// CPU 密集型任务：worker 数 = CPU 核心数
_ = worker.Start(runtime.NumCPU())

// IO 密集型任务：worker 数 = CPU 核心数 * 2~4
_ = worker.Start(runtime.NumCPU() * 2)
```

### 4. 监控队列

```go
// 定期检查队列状态
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        stats := q.Stats()

        // 警告：Ready 任务堆积
        if stats.ReadyJobs > 10000 {
            log.Printf("Warning: %d ready jobs pending", stats.ReadyJobs)
        }

        // 警告：Buried 任务过多
        if stats.BuriedJobs > 100 {
            log.Printf("Warning: %d buried jobs need attention", stats.BuriedJobs)
        }
    }
}()
```

### 5. 处理埋葬任务

```go
// 定期检查并处理 Buried 任务
func processBuriedJobs(q *sdq.Queue, topic string) {
    stats, _ := q.StatsTopic(topic)

    if stats.BuriedJobs > 0 {
        log.Printf("Found %d buried jobs in %s", stats.BuriedJobs, topic)

        // 选项 1：批量 Kick 恢复
        kicked, _ := q.Kick(topic, 10) // 每次最多 10 个
        log.Printf("Kicked %d jobs", kicked)

        // 选项 2：人工审查后单独处理
        job, _ := q.PeekBuried(topic)
        if shouldRetry(job) {
            _ = q.KickJob(job.Meta.ID)
        } else {
            _ = q.Delete(job.Meta.ID) // 删除无法处理的任务
        }
    }
}
```

### 6. 使用优先级

```go
// 区分任务重要程度
const (
    PriorityUrgent   = 0   // 紧急任务（立即处理）
    PriorityHigh     = 10  // 高优先级
    PriorityNormal   = 50  // 普通任务
    PriorityLow      = 100 // 低优先级（后台任务）
)

// 紧急通知
q.Put("notification", urgentData, PriorityUrgent, 0, 30*time.Second)

// 普通邮件
q.Put("email", emailData, PriorityNormal, 0, 5*time.Minute)

// 后台清理
q.Put("cleanup", cleanupData, PriorityLow, 0, 30*time.Minute)
```

### 7. 持久化生产环境

```go
// ✅ 生产环境：使用 SQLite Storage
storage, _ := sdq.NewSQLiteStorage("./jobs.db",
    sdq.WithMaxBatchSize(1000),
    sdq.WithMaxBatchBytes(16*1024*1024),
)
config.Storage = storage

// ❌ 开发环境：使用 Memory Storage（不持久化）
config.Storage = sdq.NewMemoryStorage()
```

### 8. 错误处理和重试

```go
// 实现指数退避重试
func processJob(q *sdq.Queue, job *sdq.Job) {
    maxRetries := 5
    retries := job.Meta.Releases

    if err := doWork(job.Body()); err != nil {
        if retries < maxRetries {
            // 指数退避：1min, 2min, 4min, 8min, 16min
            delay := time.Duration(1<<retries) * time.Minute
            _ = job.Release(job.Meta.Priority, delay)
            log.Printf("Job %d failed, retry %d/%d after %v",
                job.Meta.ID, retries+1, maxRetries, delay)
        } else {
            // 超过重试次数，埋葬任务
            _ = job.Bury(job.Meta.Priority)
            log.Printf("Job %d buried after %d retries", job.Meta.ID, retries)
        }
        return
    }

    // 成功，删除任务
    _ = job.Delete()
}
```

### 9. 优雅关闭

```go
// 监听系统信号，优雅关闭
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

<-sigChan
log.Println("Shutting down gracefully...")

// 停止 Worker（等待当前任务完成）
worker.Stop()

// 停止队列
q.Stop()

log.Println("Shutdown complete")
```

### 10. 性能调优

```go
// SQLite 高并发场景
storage, _ := sdq.NewSQLiteStorage("./jobs.db",
    sdq.WithMaxBatchSize(2000),           // 增大批量大小
    sdq.WithMaxBatchBytes(32*1024*1024),  // 增大批量字节数
)

// TimeWheel 适合大量延迟任务
config.Ticker = sdq.NewTimeWheelTicker(
    10*time.Millisecond,  // 精度
    3600,                  // 时间轮大小（支持更长延迟）
)

// DynamicSleep 适合少量延迟任务
config.Ticker = sdq.NewDynamicSleepTicker(
    10*time.Millisecond,  // 最小检查间隔
    5*time.Minute,        // 最大休眠时间
)
```

## 架构设计

### 核心组件

| 组件               | 职责         | 说明                                    |
| ------------------ | ------------ | --------------------------------------- |
| **Queue**          | 队列主体     | 协调各组件，提供统一 API                |
| **TopicHub**       | Topic 管理器 | 管理所有 topic，负责任务分发            |
| **Topic**          | 单个队列     | 管理 Ready/Delayed/Reserved/Buried 队列 |
| **Storage**        | 存储后端     | 持久化任务数据（Memory/SQLite）         |
| **Ticker**         | 定时器       | 处理延迟任务到期和 TTR 超时             |
| **ReserveManager** | Reserve 管理 | 管理阻塞等待的 Reserve 请求             |
| **RecoveryRunner** | 恢复管理     | 启动时从 Storage 恢复任务               |

### 数据流

**Put 操作流程：**

```text
Put(topic, body, priority, delay, ttr)
    ↓
1. 分配 ID
    ↓
2. 持久化 → Storage.SaveJob(meta, body)
    ↓
3. 加入内存队列
    ├─ delay=0  → Topic.ReadyHeap
    └─ delay>0  → Topic.DelayedHeap
    ↓
4. 通知等待的 Worker (如果有)
    ↓
返回 JobID
```

**Reserve 操作流程：**

```text
Reserve(topics, timeout)
    ↓
1. 尝试从指定 topics 获取 Ready 任务
    ├─ 有任务 → 2
    └─ 无任务 → 3
    ↓
2. 从 Storage 加载 Body（延迟加载）
    ↓
返回 Job
    ↓
3. 注册等待（阻塞）
    ├─ 有新任务 Put → 唤醒并返回
    ├─ Delayed 到期 → 唤醒并返回
    └─ 超时 → 返回 ErrTimeout
```

**整体数据流：**

```text
┌─────────┐
│  Put    │ → Storage.SaveJob
└────┬────┘              ↓
     │            ┌──────────────┐
     └──────────→ │  TopicHub    │
                  │  ├─ Topic A  │
                  │  ├─ Topic B  │
                  │  └─ Topic C  │
                  └──────┬───────┘
                         │
                    ┌────┴────┐
                    │ Ticker  │ (定时处理)
                    │ ├─ Delayed → Ready
                    │ └─ Reserved → Ready (TTR)
                    └─────────┘
                         │
                  ┌──────┴────────┐
                  │ ReserveManager│
                  └──────┬────────┘
                         │
                    ┌────┴────┐
                    │ Reserve │ → Storage.GetJobBody
                    └────┬────┘
                         │
                    ┌────┴────┐
                    │ Worker  │
                    └────┬────┘
                         │
                    ┌────┴────┐
                    │ Delete  │ → Storage.DeleteJob
                    └─────────┘
```

### Topic 数据结构

每个 Topic 独立管理自己的任务队列：

```text
Topic {
    Name: "email"

    ReadyHeap: [                    // 按 Priority 排序
        Job1(pri:1),
        Job3(pri:5),
        Job2(pri:10)
    ]

    DelayedHeap: [                  // 按 ReadyAt 排序
        Job4(ready_at: T1),
        Job5(ready_at: T2)
    ]

    ReservedMap: {                  // ID -> Deadline 映射
        100: 2024-01-01 10:05:00,
        101: 2024-01-01 10:06:00
    }

    BuriedHeap: [                   // 按 Priority 排序
        Job6(pri:1),
        Job7(pri:5)
    ]
}
```

**设计优势：**

- ✅ 每个 Topic 独立，无全局锁竞争
- ✅ 任务完全属于 Topic，状态转换清晰
- ✅ Ticker 只负责通知，不直接管理任务
- ✅ 支持灵活的 Reserve 策略（单 topic 或多 topic）

## License

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！
