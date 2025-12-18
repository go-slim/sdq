# WebUI API Example

这个示例演示如何使用 `x/webui` 包提供的 HTTP API 来监控和管理 SDQ 队列。

## 功能特性

- 提供 RESTful JSON API
- 查询队列概览和统计信息
- 查看 Topic 和 Job 详情
- 管理任务（Kick、Delete）
- 支持路由前缀（如 `/sdq`）

## 运行示例

```bash
# 在项目根目录运行
go run ./examples/webui

# 或者先构建再运行
go build -o webui-demo ./examples/webui
./webui-demo
```

服务器将在 `http://localhost:8080` 启动。

## 可用的 API 端点

### 查询 API

```bash
# 获取队列概览
curl http://localhost:8080/sdq/api/overview

# 获取所有 Topic 列表
curl http://localhost:8080/sdq/api/topics

# 获取单个 Topic 详情
curl http://localhost:8080/sdq/api/topics/email

# 获取 Topic 的任务列表（支持分页）
curl "http://localhost:8080/sdq/api/topics/email/jobs?page=1&page_size=10"

# 按状态过滤任务
curl "http://localhost:8080/sdq/api/topics/email/jobs?state=ready"

# 获取单个任务详情
curl http://localhost:8080/sdq/api/jobs/1
```

### 管理 API

```bash
# 踢出 Topic 所有埋葬任务
curl -X POST http://localhost:8080/sdq/api/topics/email/kick

# 删除 Topic 所有埋葬任务
curl -X POST http://localhost:8080/sdq/api/topics/email/delete-buried

# 踢出单个埋葬任务
curl -X POST http://localhost:8080/sdq/api/jobs/1/kick

# 强制删除单个任务（任何状态）
curl -X DELETE http://localhost:8080/sdq/api/jobs/1
```

## 响应示例

### 队列概览

```json
{
  "total_jobs": 11,
  "ready_jobs": 6,
  "delayed_jobs": 4,
  "reserved_jobs": 0,
  "buried_jobs": 1,
  "total_topics": 3,
  "total_waiting_workers": 0,
  "uptime": "5m30s",
  "started_at": "2024-01-01T12:00:00Z"
}
```

### Topic 列表

```json
[
  {
    "name": "email",
    "ready_jobs": 4,
    "delayed_jobs": 3,
    "reserved_jobs": 0,
    "buried_jobs": 1,
    "total_jobs": 8,
    "waiting_workers": 0
  },
  {
    "name": "push",
    "ready_jobs": 0,
    "delayed_jobs": 1,
    "reserved_jobs": 0,
    "buried_jobs": 0,
    "total_jobs": 1,
    "waiting_workers": 0
  }
]
```

### 任务详情

```json
{
  "id": 1,
  "topic": "email",
  "priority": 1,
  "state": "ready",
  "delay": "0s",
  "ttr": "30s",
  "created_at": "2024-01-01T12:00:00Z",
  "ready_at": "2024-01-01T12:00:00Z",
  "age": "5m",
  "reserves": 0,
  "timeouts": 0,
  "releases": 0,
  "buries": 0,
  "kicks": 0,
  "touches": 0,
  "body_size": 22
}
```

## 自定义配置

### 修改端口

```go
server := &http.Server{
    Addr: ":9000",  // 改为 9000 端口
    // ...
}
```

### 使用不同的路由前缀

```go
// 使用 /api 前缀
http.Handle("/api/", http.StripPrefix("/api", handler))

// 或者不使用前缀
http.Handle("/", handler)
```

### 使用持久化存储

```go
import "go-slim.dev/sdq/x/sqlite"

config := sdq.DefaultConfig()
config.Storage = sqlite.New("queue.db")
```

## 停止服务

按 `Ctrl+C` 优雅关闭服务器。

## 与前端集成

这个 API 可以与任何前端框架（React、Vue、Angular 等）集成来构建管理界面。

示例 JavaScript 代码：

```javascript
// 获取队列概览
async function fetchOverview() {
  const response = await fetch('http://localhost:8080/sdq/api/overview');
  const data = await response.json();
  console.log('Overview:', data);
}

// 获取 Topic 列表
async function fetchTopics() {
  const response = await fetch('http://localhost:8080/sdq/api/topics');
  const topics = await response.json();
  console.log('Topics:', topics);
}

// 踢出埋葬任务
async function kickBuriedJobs(topic) {
  const response = await fetch(`http://localhost:8080/sdq/api/topics/${topic}/kick`, {
    method: 'POST'
  });
  const result = await response.json();
  console.log('Kicked:', result.kicked);
}
```

## 注意事项

- 这是一个演示示例，使用内存存储，重启后数据会丢失
- 生产环境建议使用持久化存储（SQLite、PostgreSQL 等）
- 生产环境建议添加认证和授权机制
- 建议在生产环境添加 CORS 支持和速率限制
