# SDQ 使用指南

本文档提供 SDQ 的详细使用说明，包括配置、API、最佳实践和架构设计。

## 目录

- [核心概念](#核心概念)
- [配置](#配置)
- [Task API](#task-api)
- [持久化与恢复](#持久化与恢复)
- [错误处理](#错误处理)
- [使用场景](#使用场景)
- [最佳实践](#最佳实践)
- [架构设计](#架构设计)

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

```go
// 有效的 topic 名称
"email", "sms_queue", "order-processing", "user_2024", "default"

// 无效的 topic 名称
"email@queue", "queue#1", "中文队列", ""
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

SQLite Storage 使用 `database/sql` 标准接口，需要用户自己引入 SQLite 驱动。

**选项 1：使用 CGO 驱动（推荐，性能更好）**

```bash
go get github.com/mattn/go-sqlite3
```

```go
import (
    _ "github.com/mattn/go-sqlite3"
    "go-slim.dev/sdq"
)

// 默认配置
storage, _ := sdq.NewSQLiteStorage("./jobs.db")

// 自定义批量配置（用于性能调优）
storage, _ := sdq.NewSQLiteStorage("./jobs.db",
    sdq.WithMaxBatchSize(500),
    sdq.WithMaxBatchBytes(8*1024*1024),
)
```

**选项 2：使用纯 Go 驱动（无需 CGO）**

```bash
go get modernc.org/sqlite
```

```go
import (
    _ "modernc.org/sqlite"
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
    Priority:   10,               // 默认优先级
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

1. **并发消费**：`worker.Start(N)` 会启动 `N` 个并发的 Goroutine 来持续从队列中 `Reserve` 任务。
2. **任务超时控制 (TTR)**：`Worker` 会根据任务注册时配置的 `TTR` 为每个任务创建一个独立的上下文。
3. **智能重试**：
   - 当任务处理器返回错误时，检查 `job.Meta.Releases` 是否小于 `MaxRetries`
   - 如果还有重试机会，使用**指数退避**延迟（5s, 10s, 20s...）重新入队
   - 达到最大重试次数后，自动埋葬任务
4. **成功处理**：任务成功执行后，自动删除任务

#### 使用示例

```go
// 启动 Worker 处理多个任务类型
worker := task.NewWorker(q,
    "process-order",
    "send-notification",
    "generate-report",
)

// 启动 5 个并发 worker
_ = worker.Start(5)
defer worker.Stop()
```

## 持久化与恢复

### 数据分离存储

SDQ 采用**元数据与数据分离**的存储架构，显著提升性能和可扩展性。

```text
Job Metadata (轻量级，约 200 字节):
├── ID, Topic, Priority
├── State, Delay, TTR
├── 时间戳 (CreatedAt, ReadyAt, ReservedAt, etc.)
└── 统计信息 (Reserves, Timeouts, Releases, etc.)

Job Body (可能很大):
└── Body []byte (按需加载)
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
        }
    },
})
```

### 数据持久化

**Storage 类型：**

| Storage    | 持久化        | 性能 | 使用场景               |
| ---------- | ------------- | ---- | ---------------------- |
| **Memory** | ❌ 不持久化   | 最高 | 开发测试、临时队列     |
| **SQLite** | ✅ 自动持久化 | 高   | 生产环境、需要崩溃恢复 |

**恢复策略：**

| 原状态   | 恢复后状态       | 说明                       |
| -------- | ---------------- | -------------------------- |
| Enqueued | Ready 或 Delayed | 重新分配到队列             |
| Ready    | Ready            | 保持原状态                 |
| Delayed  | Delayed          | 保持原状态，继续等待到期   |
| Reserved | Ready            | 崩溃前处理中的任务重新分配 |
| Buried   | Buried           | 保持埋葬状态               |

## 错误处理

**任务相关错误：**

| 错误              | 说明                         |
| ----------------- | ---------------------------- |
| `ErrNotFound`     | 任务不存在                   |
| `ErrNotReserved`  | 任务未被保留                 |
| `ErrNotBuried`    | 任务未被埋葬                 |
| `ErrInvalidState` | 任务状态无效                 |
| `ErrJobExists`    | 任务已存在（SQLite Storage） |

**Topic 相关错误：**

| 错误                  | 说明                |
| --------------------- | ------------------- |
| `ErrInvalidTopic`     | topic 名称无效      |
| `ErrTopicRequired`    | topic 不能为空      |
| `ErrMaxTopicsReached` | 达到最大 topic 数量 |

**操作相关错误：**

| 错误                    | 说明             |
| ----------------------- | ---------------- |
| `ErrTimeout`            | 操作超时         |
| `ErrInvalidTimeout`     | 超时参数无效     |
| `ErrMaxJobsReached`     | 达到最大任务数量 |
| `ErrTouchLimitExceeded` | Touch 次数超限   |
| `ErrInvalidTouchTime`   | Touch 时间无效   |
| `ErrTooManyWaiters`     | 等待队列已满     |

**Storage 相关错误：**

| 错误               | 说明           |
| ------------------ | -------------- |
| `ErrStorageClosed` | Storage 已关闭 |

**错误处理示例：**

```go
job, err := q.Reserve([]string{"email"}, 5*time.Second)
if err == sdq.ErrTimeout {
    log.Println("No jobs available")
    return
}

err = q.Touch(jobID)
if err == sdq.ErrTouchLimitExceeded {
    log.Println("Touch limit reached")
}
```

## 使用场景

### 异步任务处理

```go
emailTask.Publish(ctx, EmailData{...})
reportTask.Publish(ctx, ReportData{...}, task.Priority(20))
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
        err := performCleanup(data)
        tomorrow := time.Now().Add(24 * time.Hour).Truncate(24 * time.Hour)
        _ = dailyCleanup.Publish(ctx, data, task.DelayUntil(tomorrow))
        return err
    },
})

// 方式 2：使用专门的定时任务库（推荐）
c := cron.New()
c.AddFunc("0 0 * * *", func() {
    cleanupTask.Publish(ctx, CleanupData{...})
})
c.Start()
```

### 重试机制

使用 Task API 时，Worker 会自动处理重试：

```go
var processOrder = task.Register("process-order", &task.Config{
    Handler: func(ctx context.Context, data OrderData) error {
        return fmt.Errorf("模拟处理失败")
    },
    MaxRetries: 5, // 自动重试 5 次，使用指数退避
    TTR:        30 * time.Second,
})
```

手动处理重试：

```go
func processJob(q *sdq.Queue, job *sdq.Job) {
    maxRetries := 5
    retries := job.Meta.Releases

    if err := doWork(job.Body()); err != nil {
        if retries < maxRetries {
            delay := time.Duration(1<<retries) * time.Minute
            _ = job.Release(job.Meta.Priority, delay)
        } else {
            _ = job.Bury(job.Meta.Priority)
        }
        return
    }
    _ = job.Delete()
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
config.DefaultTTR = 5 * time.Minute

// 长时间任务使用 Touch 延长
for i := 0; i < 10; i++ {
    processChunk(i)
    if i < 9 {
        _ = job.Touch()
    }
}
```

### 3. 控制并发

```go
worker := task.NewWorker(q, "process-order")

// CPU 密集型任务
_ = worker.Start(runtime.NumCPU())

// IO 密集型任务
_ = worker.Start(runtime.NumCPU() * 2)
```

### 4. 监控队列

```go
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        stats := q.Stats()
        if stats.ReadyJobs > 10000 {
            log.Printf("Warning: %d ready jobs pending", stats.ReadyJobs)
        }
        if stats.BuriedJobs > 100 {
            log.Printf("Warning: %d buried jobs need attention", stats.BuriedJobs)
        }
    }
}()
```

### 5. 处理埋葬任务

```go
func processBuriedJobs(q *sdq.Queue, topic string) {
    stats, _ := q.StatsTopic(topic)
    if stats.BuriedJobs > 0 {
        kicked, _ := q.Kick(topic, 10)
        log.Printf("Kicked %d jobs", kicked)
    }
}
```

### 6. 使用优先级

```go
const (
    PriorityUrgent   = 0   // 紧急任务
    PriorityHigh     = 10  // 高优先级
    PriorityNormal   = 50  // 普通任务
    PriorityLow      = 100 // 后台任务
)

q.Put("notification", urgentData, PriorityUrgent, 0, 30*time.Second)
q.Put("email", emailData, PriorityNormal, 0, 5*time.Minute)
```

### 7. 优雅关闭

```go
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

<-sigChan
log.Println("Shutting down gracefully...")

worker.Stop()
q.Stop()

log.Println("Shutdown complete")
```

### 8. 性能调优

```go
// SQLite 高并发场景
storage, _ := sdq.NewSQLiteStorage("./jobs.db",
    sdq.WithMaxBatchSize(2000),
    sdq.WithMaxBatchBytes(32*1024*1024),
)

// TimeWheel 适合大量延迟任务
config.Ticker = sdq.NewTimeWheelTicker(10*time.Millisecond, 3600)

// DynamicSleep 适合少量延迟任务
config.Ticker = sdq.NewDynamicSleepTicker(10*time.Millisecond, 5*time.Minute)
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

- 每个 Topic 独立，无全局锁竞争
- 任务完全属于 Topic，状态转换清晰
- Ticker 只负责通知，不直接管理任务
- 支持灵活的 Reserve 策略（单 topic 或多 topic）
