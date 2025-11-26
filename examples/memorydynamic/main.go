package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go-slim.dev/sdq"
)

// Example 2: MemoryStorage + DynamicSleepTicker
// 适用场景：少量延迟任务，注重资源效率
func main() {
	// 创建配置
	config := sdq.DefaultConfig()

	// 使用内存存储（默认）
	config.Storage = sdq.NewMemoryStorage()

	// 使用动态休眠 Ticker（适合少量延迟任务）
	config.Ticker = sdq.NewDynamicSleepTicker(
		50*time.Millisecond, // 最小检查间隔
		5*time.Minute,       // 最大休眠时间
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

	ctx := context.Background()

	fmt.Println("=== DynamicSleepTicker Example ===")
	fmt.Println("特点：根据最近到期时间动态调整休眠")

	// 场景 1: 创建多个不同延迟的任务
	delays := []struct {
		topic string
		delay time.Duration
		body  string
	}{
		{"task", 2 * time.Second, "task after 2s"},
		{"task", 5 * time.Second, "task after 5s"},
		{"task", 10 * time.Second, "task after 10s"},
	}

	fmt.Println("\nCreating delayed jobs...")
	for _, d := range delays {
		jobID, err := q.Put(d.topic, []byte(d.body), 1, d.delay, 30*time.Second)
		if err != nil {
			log.Fatalf("Failed to put job: %v", err)
		}
		fmt.Printf("Created job %d: delay=%v, body=%s\n", jobID, d.delay, d.body)
	}

	// 场景 2: 模拟 Worker 持续消费
	fmt.Println("\nStarting worker to consume jobs...")

	consumedCount := 0
	startTime := time.Now()

	for consumedCount < len(delays) {
		job, err := q.Reserve([]string{"task"}, 1*time.Second)
		if err != nil {
			// 超时，继续等待
			continue
		}

		elapsed := time.Since(startTime)
		fmt.Printf("[%v] Reserved job %d: %s\n", elapsed.Round(100*time.Millisecond), job.Meta.ID, string(job.Body()))

		// 处理任务
		if err := processJobWithTouch(ctx, q, job); err != nil {
			_ = q.Bury(job.Meta.ID, job.Meta.Priority)
		} else {
			_ = q.Delete(job.Meta.ID)
			consumedCount++
		}
	}

	// 显示统计
	stats := q.Stats()
	fmt.Printf("\nAll jobs processed! Total time: %v\n", time.Since(startTime).Round(100*time.Millisecond))
	fmt.Printf("Stats: Ready=%d, Reserved=%d, Delayed=%d, Buried=%d\n",
		stats.ReadyJobs, stats.ReservedJobs, stats.DelayedJobs, stats.BuriedJobs)
}

func processJobWithTouch(_ context.Context, q *sdq.Queue, job *sdq.Job) error {
	fmt.Printf("Processing job %d...\n", job.Meta.ID)

	// 模拟长时间处理，使用 Touch 延长 TTR
	for i := range 3 {
		time.Sleep(200 * time.Millisecond)

		// 每 200ms Touch 一次，防止 TTR 超时
		if err := q.Touch(job.Meta.ID); err != nil {
			fmt.Printf("Touch failed: %v\n", err)
			return err
		}
		fmt.Printf("  Touched job %d (touch %d)\n", job.Meta.ID, i+1)
	}

	fmt.Printf("Job %d processing completed\n", job.Meta.ID)
	return nil
}
