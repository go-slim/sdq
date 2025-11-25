package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go-slim.dev/sdq"
)

// Example 1: MemoryStorage + TimeWheelTicker
// 适用场景：进程内高性能任务队列，无需持久化
func main() {
	// 创建配置
	config := sdq.DefaultConfig()

	// 使用内存存储（默认）
	config.Storage = sdq.NewMemoryStorage()

	// 使用时间轮 Ticker（适合大量定时任务）
	config.Ticker = sdq.NewTimeWheelTicker(
		100*time.Millisecond, // tick 间隔
		3600,                 // 时间轮槽位数（支持 1 小时范围）
	)

	// 创建队列
	q, err := sdq.NewQueue(config)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	ctx := context.Background()

	// 场景 1: 立即执行的任务
	jobID1, err := q.Put("email", []byte("send welcome email"), 1, 0, 30*time.Second)
	if err != nil {
		log.Fatalf("Failed to put job: %v", err)
	}
	fmt.Printf("Created immediate job: %d\n", jobID1)

	// 场景 2: 延迟执行的任务
	jobID2, err := q.Put("notification", []byte("remind user after 5s"), 2, 5*time.Second, 30*time.Second)
	if err != nil {
		log.Fatalf("Failed to put delayed job: %v", err)
	}
	fmt.Printf("Created delayed job (5s): %d\n", jobID2)

	// 场景 3: 消费任务
	fmt.Println("\nStarting to reserve jobs...")

	// 立即可以获取到第一个任务
	job1, err := q.Reserve([]string{"email"}, 2*time.Second)
	if err != nil {
		log.Fatalf("Failed to reserve job: %v", err)
	}
	fmt.Printf("Reserved job %d: %s\n", job1.Meta.ID, string(job1.Body))

	// 处理任务
	if err := processJob(ctx, job1); err != nil {
		// 处理失败，埋葬任务
		if err := q.Bury(job1.Meta.ID); err != nil {
			log.Printf("Failed to bury job: %v", err)
		}
	} else {
		// 处理成功，删除任务
		if err := q.Delete(ctx, job1.Meta.ID); err != nil {
			log.Printf("Failed to delete job: %v", err)
		}
		fmt.Printf("Job %d completed successfully\n", job1.Meta.ID)
	}

	// 等待延迟任务到期
	fmt.Println("\nWaiting for delayed job to become ready...")
	time.Sleep(6 * time.Second)

	job2, err := q.Reserve([]string{"notification"}, 2*time.Second)
	if err != nil {
		log.Fatalf("Failed to reserve delayed job: %v", err)
	}
	fmt.Printf("Reserved delayed job %d: %s\n", job2.Meta.ID, string(job2.Body))

	// 处理并删除
	if err := processJob(ctx, job2); err == nil {
		q.Delete(ctx, job2.Meta.ID)
		fmt.Printf("Job %d completed successfully\n", job2.Meta.ID)
	}

	// 显示最终统计
	stats := q.Stats()
	fmt.Printf("\nFinal stats: Ready=%d, Reserved=%d, Delayed=%d, Buried=%d\n",
		stats.ReadyJobs, stats.ReservedJobs, stats.DelayedJobs, stats.BuriedJobs)
}

func processJob(ctx context.Context, job *sdq.Job) error {
	// 模拟任务处理
	fmt.Printf("Processing job %d: %s\n", job.Meta.ID, string(job.Body))
	time.Sleep(100 * time.Millisecond)
	return nil
}
