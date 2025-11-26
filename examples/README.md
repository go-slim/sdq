# SDQ Examples

本目录包含 SDQ（Simple Delay Queue）的使用示例，展示不同的 Storage 和 Ticker 组合。

## 示例列表

### 1. memorytimewheel - MemoryStorage + TimeWheelTicker

**适用场景：** 进程内高性能任务队列，无需持久化

**特点：**

- 内存存储，性能最优
- 时间轮调度，适合大量定时任务
- 进程重启后数据丢失

**运行：**

```bash
go run memorytimewheel/main.go
# 或者
cd memorytimewheel && go run main.go
```

**学习要点：**

- 基本的 Put/Reserve/Delete 操作
- 延迟任务的处理
- 任务失败时的 Bury 操作

---

### 2. memorydynamic - MemoryStorage + DynamicSleepTicker

**适用场景：** 少量延迟任务，注重资源效率

**特点：**

- 内存存储
- 动态休眠调度，根据最近到期时间自动调整
- 适合延迟任务较少的场景

**运行：**

```bash
go run memorydynamic/main.go
# 或者
cd memorydynamic && go run main.go
```

**学习要点：**

- DynamicSleepTicker 的工作原理
- Touch 操作延长 TTR
- 长时间任务的处理模式

---

### 3. sqlitetimewheel - SQLiteStorage + TimeWheelTicker

**适用场景：** 需要持久化的生产环境，大量定时任务

**特点:**

- SQLite 持久化存储
- 时间轮高性能调度
- 进程重启后数据自动恢复

**运行：**

```bash
go run sqlitetimewheel/main.go
# 或者
cd sqlitetimewheel && go run main.go
```

**学习要点：**

- SQLite 存储的使用
- 进程重启后的数据恢复
- 批量任务处理
- Topic 级别的统计

---

### 4. sqlitedynamic - SQLiteStorage + DynamicSleepTicker

**适用场景：** 需要持久化 + 少量延迟任务 + 资源受限环境

**特点：**

- SQLite 持久化存储
- 动态休眠节省资源
- 优雅关闭处理
- 多 Worker 并发

**运行：**

```bash
go run sqlitedynamic/main.go
# 或者
cd sqlitedynamic && go run main.go
```

**学习要点：**

- 多 Worker 并发处理
- 优雅关闭的实现
- Touch 配置的使用
- 信号处理

---

## Storage 对比

| Storage       | 性能       | 持久化 | 适用场景             |
| ------------- | ---------- | ------ | -------------------- |
| MemoryStorage | ⭐⭐⭐⭐⭐ | ❌     | 临时任务、高性能需求 |
| SQLiteStorage | ⭐⭐⭐     | ✅     | 生产环境、需要持久化 |

## Ticker 对比

| Ticker             | 资源占用         | 延迟任务量 | 精度            |
| ------------------ | ---------------- | ---------- | --------------- |
| TimeWheelTicker    | 中等（固定内存） | 大量       | 高（tick 间隔） |
| DynamicSleepTicker | 低（动态休眠）   | 少量       | 中（最小间隔）  |
| NoOpTicker         | 极低             | 不支持     | -               |

## 配置建议

### 高并发、大量延迟任务

```go
config.Storage = sdq.NewSQLiteStorage("queue.db")
config.Ticker = sdq.NewTimeWheelTicker(100*time.Millisecond, 3600)
```

### 低并发、少量延迟任务

```go
config.Storage = sdq.NewSQLiteStorage("queue.db")
config.Ticker = sdq.NewDynamicSleepTicker(50*time.Millisecond, 5*time.Minute)
```

### 仅立即任务（无延迟）

```go
config.Storage = sdq.NewMemoryStorage()
config.Ticker = sdq.NewNoOpTicker()
```

### 临时测试环境

```go
config.Storage = sdq.NewMemoryStorage()
config.Ticker = sdq.NewDynamicSleepTicker(50*time.Millisecond, 5*time.Minute)
```

## 注意事项

1. **SQLite 驱动**: SQLite 示例需要导入驱动（示例中使用 `github.com/mattn/go-sqlite3`），您也可以使用 `modernc.org/sqlite`

2. **优雅关闭**: 生产环境务必实现优雅关闭，确保正在处理的任务完成

3. **TTR 配置**: 根据任务的实际执行时间合理设置 TTR，避免任务超时

4. **Touch 限制**: 配置 MaxTouches、MinTouchInterval、MaxTouchDuration 防止任务无限延长

5. **并发控制**: 根据系统资源合理设置 Worker 数量

## 更多资源

- [主 README](../README.md)
- [API 文档](https://pkg.go.dev/go-slim.dev/sdq)
- [测试用例](../) - 查看 \*\_test.go 文件了解更多用法
