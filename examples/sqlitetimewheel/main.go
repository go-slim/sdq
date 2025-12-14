package main

import (
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite 驱动
	"go-slim.dev/sdq"
	"go-slim.dev/sdq/x/sqlite"
	"go-slim.dev/sdq/x/timewheel"
)

// Example 3: SQLiteStorage + TimeWheelTicker
// 适用场景：需要持久化的生产环境，大量定时任务
func main() {
	// 创建临时数据库文件
	dbPath := "example_queue.db"
	defer func() { _ = os.Remove(dbPath) }() // 示例结束后删除

	// 创建配置
	config := sdq.DefaultConfig()

	// 使用 SQLite 存储（持久化）
	storage, err := sqlite.New(dbPath)
	if err != nil {
		log.Fatalf("Failed to create SQLite storage: %v", err)
	}
	config.Storage = storage

	// 使用时间轮 Ticker
	config.Ticker = timewheel.New(
		100*time.Millisecond,
		3600,
	)

	// 创建队列
	q, err := sdq.New(config)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer func() { _ = q.Stop() }()

	// 启动队列
	if err := q.Start(); err != nil {
		log.Fatalf("Failed to start queue: %v", err)
	}

	fmt.Println("=== SQLiteStorage + TimeWheelTicker Example ===")
	fmt.Println("特点：数据持久化 + 高性能时间轮调度")

	// 场景 1: 批量添加任务
	fmt.Println("\nCreating batch jobs...")
	topics := []string{"order", "payment", "notification"}
	for i := range 10 {
		topic := topics[i%len(topics)]
		delay := time.Duration(i) * time.Second
		body := fmt.Sprintf("job-%d for %s", i, topic)

		jobID, err := q.Put(topic, []byte(body), uint32(i%5+1), delay, 60*time.Second)
		if err != nil {
			log.Fatalf("Failed to put job: %v", err)
		}
		fmt.Printf("Created job %d: topic=%s, delay=%v, priority=%d\n", jobID, topic, delay, i%5+1)
	}

	// 显示初始统计
	insp := sdq.NewInspector(q)
	stats := insp.Stats()
	fmt.Printf("\nInitial stats: Total=%d, Ready=%d, Delayed=%d\n",
		stats.TotalJobs, stats.ReadyJobs, stats.DelayedJobs)

	// 场景 2: 模拟进程重启（关闭后重新打开）
	fmt.Println("\n=== Simulating Process Restart ===")
	_ = q.Stop()

	// 重新打开队列（从 SQLite 恢复数据）
	storage2, err := sqlite.New(dbPath)
	if err != nil {
		log.Fatalf("Failed to reopen SQLite storage: %v", err)
	}
	config.Storage = storage2

	q, err = sdq.New(config)
	if err != nil {
		log.Fatalf("Failed to recreate queue: %v", err)
	}
	defer func() { _ = q.Stop() }()

	// 启动队列
	if err := q.Start(); err != nil {
		log.Fatalf("Failed to start queue: %v", err)
	}

	// 验证数据已恢复
	insp = sdq.NewInspector(q)
	stats = insp.Stats()
	fmt.Printf("After restart: Total=%d, Ready=%d, Delayed=%d\n",
		stats.TotalJobs, stats.ReadyJobs, stats.DelayedJobs)

	// 场景 3: 按优先级消费任务
	fmt.Println("\n=== Consuming Jobs by Priority ===")

	consumedCount := 0
	for consumedCount < 5 {
		job, err := q.Reserve([]string{"order", "payment", "notification"}, 2*time.Second)
		if err != nil {
			continue
		}

		fmt.Printf("Reserved job %d: topic=%s, priority=%d, body=%s\n",
			job.Meta.ID, job.Meta.Topic, job.Meta.Priority, string(job.Body()))

		// 处理并删除
		if err := q.Delete(job.Meta.ID); err != nil {
			log.Printf("Failed to delete job: %v", err)
		}
		consumedCount++
	}

	// 场景 4: 查看某个 Topic 的统计
	for _, topic := range topics {
		topicStats, err := insp.StatsTopic(topic)
		if err != nil {
			log.Printf("Failed to get stats for topic %s: %v", topic, err)
			continue
		}
		fmt.Printf("\nTopic '%s': Total=%d, Ready=%d, Reserved=%d, Delayed=%d, Buried=%d\n",
			topic,
			topicStats.TotalJobs,
			topicStats.ReadyJobs,
			topicStats.ReservedJobs,
			topicStats.DelayedJobs,
			topicStats.BuriedJobs,
		)
	}

	fmt.Println("\n=== Example completed ===")
	fmt.Println("SQLite 数据库文件:", dbPath)
	fmt.Println("提示：生产环境中不要删除数据库文件，以保留持久化数据")
}
