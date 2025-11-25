# SDQ - Simple Delay Queue

简洁高效的延迟队列，基于 Go 实现，受 beanstalkd 启发。

## 特性

- ✅ **多 Topic 隔离** - 支持多个命名队列
- ✅ **优先级调度** - 数字越小优先级越高
- ✅ **延迟执行** - 支持任务延迟和定时执行
- ✅ **TTR 保护** - Time-To-Run 机制防止任务丢失
- ✅ **任务管理** - 支持 Bury/Kick/Release/Touch 操作
- ✅ **持久化** - 可选的 Memory/SQLite 存储
- ✅ **高性能** - 异步批量写入，50,000+ QPS
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

- 每个 topic 是一个独立的命名队列
- 自动创建，无需预先定义
- 持久存在，不会自动删除

### Job 状态

```
Put → Enqueued → Ready → Reserved → Delete
                   ↓         ↓
                Delayed   Buried
```

- **Enqueued** - 已入队，等待处理
- **Ready** - 就绪，可以被 Reserve
- **Delayed** - 延迟中，到期后转为 Ready
- **Reserved** - 已被 worker 获取，正在处理
- **Buried** - 已埋葬，等待人工介入

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
    DefaultTTR:       60 * time.Second,  // 默认超时时间
    MaxJobSize:       64 * 1024,         // 最大任务大小 64KB
    MaxTouches:       10,                // Touch 最大次数
    MaxTopics:        0,                 // 最大 topic 数量，0 表示无限制
    AsyncPutWorkers:  2,                 // 异步 Put worker 数量
}
```

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

### 注册任务

```go
var processOrder = task.Register("process-order", &task.Config{
    Handler: func(ctx context.Context, data OrderData) error {
        // 处理订单
        return nil
    },
    Priority: 10,
    TTR:      60 * time.Second,
})
```

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

## 性能

**基准测试结果：**

- **Memory Storage**
  - 异步模式：~50,000 QPS
  - 同步模式：~10,000 QPS

- **SQLite Storage**
  - 异步模式：~10,000 QPS
  - 同步模式：~1,000 QPS

**优化建议：**

1. 使用异步 Put（默认启用）
2. 增加 `AsyncPutWorkers` 数量
3. SQLite 调整批量参数
4. 使用 Memory Storage 获得最佳性能

## 持久化与恢复

### 自动恢复

启动时自动从 Storage 恢复任务：

```go
q, _ := sdq.New(config)
_ = q.Start() // 自动恢复任务

// 可选：监听恢复进度
_ = q.StartWithOptions(sdq.StartOptions{
    RecoveryCallback: func(progress *sdq.RecoveryProgress) {
        if progress.Phase == sdq.RecoveryPhaseComplete {
            log.Printf("Recovered %d jobs", progress.TotalJobs)
        }
    },
})
```

### 数据持久化

- **Memory Storage** - 不持久化，重启后数据丢失
- **SQLite Storage** - 自动持久化，支持崩溃恢复

**恢复策略：**

- `Enqueued` → `Ready` (立即可用)
- `Reserved` → `Ready` (TTR 超时，重新分配)
- `Ready/Delayed/Buried` - 保持原状态

## 错误处理

```go
var (
    ErrNotFound         // 任务不存在
    ErrNotReserved      // 任务未被保留
    ErrNotBuried        // 任务未被埋葬
    ErrInvalidState     // 任务状态无效
    ErrTimeout          // 操作超时
    ErrJobExists        // 任务已存在
)
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

### 定时任务

```go
// 每天凌晨执行
tomorrow := time.Now().Add(24*time.Hour).Truncate(24*time.Hour)
cleanupTask.Publish(ctx, CleanupData{...}, task.DelayUntil(tomorrow))
```

### 重试机制

```go
handler := func(ctx context.Context, data JobData) error {
    if err := process(data); err != nil {
        // 5 分钟后重试
        return task.Retry(5 * time.Minute)
    }
    return nil
}
```

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

1. **使用 Task API** - 类型安全，自动序列化
2. **合理设置 TTR** - 根据任务处理时间设置超时
3. **控制并发** - 根据系统资源调整 worker 数量
4. **监控队列** - 定期检查 Ready/Delayed/Buried 任务数
5. **处理埋葬任务** - 定期 Kick 或人工处理
6. **使用优先级** - 区分任务重要程度
7. **持久化生产环境** - 使用 SQLite Storage 防止数据丢失

## 架构设计

### 核心组件

- **Queue** - 队列主体，协调各组件
- **TopicHub** - Topic 管理器，负责任务分发
- **Storage** - 存储后端（Memory/SQLite）
- **Ticker** - 定时器，处理延迟任务和 TTR 超时
- **ReserveManager** - Reserve 等待管理
- **RecoveryRunner** - 启动时恢复任务

### 数据流

```
Put → AsyncPutWorker → Storage → Topic → Reserve → Worker → Delete
                         ↓
                     持久化 (SQLite)
```

### 状态转换

```
            Put
             ↓
        [Enqueued]
             ↓
         [Ready] ←─────────────┐
             ↓                 │
        Reserve               │
             ↓                 │
       [Reserved] ──Release──→│
             ↓                 │
        Delete               TTR timeout
             ↓                 │
         Complete              │
                              │
         [Delayed] ───到期────→│
                              │
         [Buried] ───Kick────→│
```

## License

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！
