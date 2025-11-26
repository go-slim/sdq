package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite 驱动
	"go-slim.dev/sdq"
)

// Example 4: SQLiteStorage + DynamicSleepTicker
// 适用场景：需要持久化 + 少量延迟任务 + 资源受限环境
func main() {
	// 创建数据库文件
	dbPath := "example_persistent_queue.db"
	defer func() { _ = os.Remove(dbPath) }()

	// 创建配置
	config := sdq.DefaultConfig()

	// 使用 SQLite 存储
	storage, err := sdq.NewSQLiteStorage(dbPath)
	if err != nil {
		log.Fatalf("Failed to create SQLite storage: %v", err)
	}
	config.Storage = storage

	// 使用动态休眠 Ticker（节省资源）
	config.Ticker = sdq.NewDynamicSleepTicker(
		50*time.Millisecond,
		5*time.Minute,
	)

	// Touch 配置（允许任务延长执行时间）
	config.MaxTouches = 5
	config.MinTouchInterval = 1 * time.Second
	config.MaxTouchDuration = 5 * time.Minute

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

	ctx := context.Background()

	fmt.Println("=== SQLiteStorage + DynamicSleepTicker Example ===")
	fmt.Println("特点：持久化存储 + 动态休眠 + 优雅关闭")

	// 场景 1: 创建定时任务
	fmt.Println("\nCreating scheduled jobs...")
	schedules := []struct {
		delay time.Duration
		body  string
	}{
		{3 * time.Second, "backup database"},
		{6 * time.Second, "send daily report"},
		{9 * time.Second, "cleanup temp files"},
	}

	for _, s := range schedules {
		jobID, err := q.Put("scheduled", []byte(s.body), 1, s.delay, 2*time.Minute)
		if err != nil {
			log.Fatalf("Failed to put job: %v", err)
		}
		fmt.Printf("Scheduled job %d: delay=%v, task=%s\n", jobID, s.delay, s.body)
	}

	// 场景 2: 启动多个 Worker 并发处理
	fmt.Println("\nStarting workers...")

	var wg sync.WaitGroup
	stopChan := make(chan struct{})
	workerCount := 2

	for i := range workerCount {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, q, workerID, stopChan)
		}(i + 1)
	}

	// 场景 3: 优雅关闭（监听信号）
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("\nPress Ctrl+C to gracefully shutdown...")
	fmt.Println("Waiting for jobs to be processed...")

	// 等待信号或任务完成
	go func() {
		time.Sleep(15 * time.Second) // 自动结束示例
		sigChan <- syscall.SIGTERM
	}()

	<-sigChan
	fmt.Println("\n=== Shutting down gracefully ===")

	// 停止 Workers
	close(stopChan)
	wg.Wait()

	// 显示最终统计
	stats := q.Stats()
	fmt.Printf("\nFinal stats:\n")
	fmt.Printf("  Total Jobs: %d\n", stats.TotalJobs)
	fmt.Printf("  Ready: %d\n", stats.ReadyJobs)
	fmt.Printf("  Reserved: %d\n", stats.ReservedJobs)
	fmt.Printf("  Delayed: %d\n", stats.DelayedJobs)
	fmt.Printf("  Buried: %d\n", stats.BuriedJobs)

	fmt.Println("\n=== Example completed ===")
	fmt.Printf("数据已持久化到: %s\n", dbPath)
}

func runWorker(ctx context.Context, q *sdq.Queue, workerID int, stopChan chan struct{}) {
	fmt.Printf("[Worker %d] Started\n", workerID)

	for {
		select {
		case <-stopChan:
			fmt.Printf("[Worker %d] Stopped\n", workerID)
			return
		default:
			// 尝试获取任务
			job, err := q.Reserve([]string{"scheduled"}, 500*time.Millisecond)
			if err != nil {
				// 超时或无任务，继续循环
				continue
			}

			fmt.Printf("[Worker %d] Reserved job %d: %s\n", workerID, job.Meta.ID, string(job.Body()))

			// 模拟处理任务（较长时间，需要 Touch）
			if err := processLongRunningJob(ctx, q, job, workerID); err != nil {
				fmt.Printf("[Worker %d] Job %d failed: %v\n", workerID, job.Meta.ID, err)
				_ = q.Bury(job.Meta.ID, job.Meta.Priority)
			} else {
				fmt.Printf("[Worker %d] Job %d completed\n", workerID, job.Meta.ID)
				_ = q.Delete(job.Meta.ID)
			}
		}
	}
}

func processLongRunningJob(_ context.Context, q *sdq.Queue, job *sdq.Job, workerID int) error {
	// 模拟长时间任务，分 3 个阶段，每个阶段 Touch 一次
	stages := []string{"initializing", "processing", "finalizing"}

	for i, stage := range stages {
		fmt.Printf("[Worker %d] Job %d - Stage %d/%d: %s\n", workerID, job.Meta.ID, i+1, len(stages), stage)
		time.Sleep(1 * time.Second)

		// Touch 延长 TTR（除了最后一个阶段）
		if i < len(stages)-1 {
			if err := q.Touch(job.Meta.ID); err != nil {
				return fmt.Errorf("touch failed: %w", err)
			}
			fmt.Printf("[Worker %d] Job %d touched (touches=%d)\n", workerID, job.Meta.ID, job.Meta.Touches+1)
		}
	}

	return nil
}
