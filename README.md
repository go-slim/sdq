# SDQ - Simple Delay Queue

[![Go Reference](https://pkg.go.dev/badge/go-slim.dev/sdq.svg)](https://pkg.go.dev/go-slim.dev/sdq)
[![Go Report Card](https://goreportcard.com/badge/go-slim.dev/sdq)](https://goreportcard.com/report/go-slim.dev/sdq)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

简洁高效的延迟队列，基于 Go 实现，受 beanstalkd 启发。

## 特性

- **多 Topic 隔离** - 支持多个命名队列
- **优先级调度** - 数字越小优先级越高
- **延迟执行** - 支持任务延迟执行和指定时间执行
- **TTR 保护** - Time-To-Run 机制防止任务丢失
- **任务管理** - 支持 Bury/Kick/Release/Touch 操作
- **持久化** - 可选的 Memory/SQLite 存储
- **高性能** - 批量写入优化，内存队列调度
- **Pull 模式** - Worker 主动拉取，支持多 worker 并发
- **类型安全** - 高级 Task API 提供编译时类型检查

## 安装

```bash
go get go-slim.dev/sdq
```

## 快速开始

### 基础用法

```go
package main

import (
    "time"
    "go-slim.dev/sdq"
)

func main() {
    // 创建队列
    config := sdq.DefaultConfig()
    config.Storage = sdq.NewMemoryStorage()

    q, _ := sdq.New(config)
    _ = q.Start()
    defer q.Stop()

    // 发布任务
    _, _ = q.Put("email", []byte("send email to user@example.com"),
        10,              // priority
        0,               // delay
        60*time.Second,  // TTR
    )

    // 消费任务
    job, _ := q.Reserve([]string{"email"}, 5*time.Second)
    println(string(job.Body()))
    _ = job.Delete()
}
```

### Task API（推荐）

```go
package main

import (
    "context"
    "time"
    "go-slim.dev/sdq"
    "go-slim.dev/sdq/task"
)

type EmailTask struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

var sendEmail = task.Register("send-email", &task.Config{
    Handler: func(ctx context.Context, data EmailTask) error {
        println("Sending email to:", data.To)
        return nil
    },
    TTR: 60 * time.Second,
})

func main() {
    config := sdq.DefaultConfig()
    q, _ := sdq.New(config)
    _ = q.Start()
    defer q.Stop()

    sendEmail.SetQueue(q)

    // 发布任务
    _ = sendEmail.Publish(context.Background(), EmailTask{
        To:      "user@example.com",
        Subject: "Hello",
    })

    // 启动 Worker
    worker := task.NewWorker(q, "send-email")
    _ = worker.Start(3)
    defer worker.Stop()
}
```

## 核心概念

### Job 状态流转

```
Put → [Delayed] → Ready → Reserve → Reserved → Delete
                    ↑                    ↓
                    ←── Release/Timeout ──
                                         ↓
                                       Bury → Kick →
```

### 操作

| 操作      | 说明                 |
| --------- | -------------------- |
| `Put`     | 发布任务             |
| `Reserve` | 获取任务（阻塞等待） |
| `Delete`  | 删除任务             |
| `Release` | 释放任务（重新入队） |
| `Bury`    | 埋葬任务（暂时搁置） |
| `Kick`    | 唤醒埋葬的任务       |
| `Touch`   | 延长 TTR             |

## 配置

```go
config := sdq.Config{
    DefaultTTR:   60 * time.Second,  // 默认超时时间
    MaxJobSize:   64 * 1024,         // 最大任务大小
    MaxTouches:   10,                // Touch 最大次数
}
```

### Storage

```go
// Memory（开发测试）
config.Storage = sdq.NewMemoryStorage()

// SQLite（生产环境）
// 需要引入驱动：github.com/mattn/go-sqlite3 或 modernc.org/sqlite
config.Storage, _ = sdq.NewSQLiteStorage("./jobs.db")
```

## 错误处理

| 错误                    | 说明           |
| ----------------------- | -------------- |
| `ErrNotFound`           | 任务不存在     |
| `ErrTimeout`            | 操作超时       |
| `ErrNotReserved`        | 任务未被保留   |
| `ErrNotBuried`          | 任务未被埋葬   |
| `ErrTouchLimitExceeded` | Touch 次数超限 |

## 文档

详细使用指南请参阅 [GUIDE.md](./GUIDE.md)，包含：

- 完整配置说明
- Task API 详解
- Worker 工作原理
- 持久化与恢复
- 最佳实践
- 架构设计

## License

MIT License
