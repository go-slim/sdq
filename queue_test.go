package sdq

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func ExampleQueue_basic() {
	// 创建配置
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

	// 创建队列
	q, err := New(config)
	if err != nil {
		log.Fatal(err)
	}

	// 启动队列
	if err := q.Start(); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	// 添加任务
	jobID, err := q.Put(
		"email",                                  // topic
		[]byte("send email to user@example.com"), // body
		1,                                        // priority
		0,                                        // delay
		60*time.Second,                           // ttr
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Created job: %d\n", jobID)

	// Worker 保留任务
	job, err := q.Reserve([]string{"email"}, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Reserved job: %d, body: %s\n", job.Meta.ID, string(job.Body()))

	// 处理任务...
	// time.Sleep(2 * time.Second)

	// 完成任务
	if err := q.Delete(job.Meta.ID); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Job completed")

	// Output:
	// Created job: 1
	// Reserved job: 1, body: send email to user@example.com
	// Job completed
}

func ExampleQueue_delayed() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加延迟任务（500ms 后执行）
	jobID, _ := q.Put(
		"notification",
		[]byte("reminder"),
		1,
		500*time.Millisecond, // delay
		60*time.Second,
	)

	fmt.Printf("Created delayed job: %d\n", jobID)

	// 立即尝试 Reserve（应该超时）
	_, err := q.Reserve([]string{"notification"}, 50*time.Millisecond)
	if err == ErrTimeout {
		fmt.Println("No job available yet (delayed)")
	}

	// 等待任务到期
	time.Sleep(600 * time.Millisecond)

	// 现在可以 Reserve
	job, err := q.Reserve([]string{"notification"}, 1*time.Second)
	if err == nil {
		fmt.Printf("Reserved job after delay: %d\n", job.Meta.ID)
		_ = q.Delete(job.Meta.ID)
	}

	// Output:
	// Created delayed job: 1
	// No job available yet (delayed)
	// Reserved job after delay: 1
}

func ExampleQueue_priority() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加不同优先级的任务
	_, _ = q.Put("tasks", []byte("low priority"), 10, 0, 60*time.Second)
	_, _ = q.Put("tasks", []byte("high priority"), 1, 0, 60*time.Second)
	_, _ = q.Put("tasks", []byte("medium priority"), 5, 0, 60*time.Second)

	// Reserve 会按优先级顺序返回
	for range 3 {
		job, _ := q.Reserve([]string{"tasks"}, 1*time.Second)
		fmt.Printf("Priority %d: %s\n", job.Meta.Priority, string(job.Body()))
		_ = q.Delete(job.Meta.ID)
	}

	// Output:
	// Priority 1: high priority
	// Priority 5: medium priority
	// Priority 10: low priority
}

func ExampleQueue_release() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewTimeWheelTicker(10*time.Millisecond, 100) // 使用 TimeWheelTicker 确保及时处理

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加任务
	_, _ = q.Put("retry", []byte("task"), 1, 0, 60*time.Second)

	// Worker 1 保留任务
	job, _ := q.Reserve([]string{"retry"}, 1*time.Second)
	fmt.Printf("Worker 1 reserved job: %d\n", job.Meta.ID)

	// 处理失败，释放任务（延迟 100ms 重试）
	_ = q.Release(job.Meta.ID, 1, 100*time.Millisecond)
	fmt.Println("Job released with 100ms delay")

	// 立即尝试 Reserve（应该没有任务）
	_, err := q.Reserve([]string{"retry"}, 10*time.Millisecond)
	if err == ErrTimeout {
		fmt.Println("No job available (released with delay)")
	}

	// 等待延迟到期（TimeWheelTicker 会自动处理）
	time.Sleep(150 * time.Millisecond)

	job, err = q.Reserve([]string{"retry"}, 1*time.Second)
	if err == nil && job != nil {
		fmt.Printf("Worker 2 reserved job: %d (retry)\n", job.Meta.ID)
		_ = q.Delete(job.Meta.ID)
	}

	// Output:
	// Worker 1 reserved job: 1
	// Job released with 100ms delay
	// No job available (released with delay)
	// Worker 2 reserved job: 1 (retry)
}

func ExampleQueue_bury() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加任务
	_, _ = q.Put("process", []byte("task"), 1, 0, 60*time.Second)

	// 保留任务
	job, _ := q.Reserve([]string{"process"}, 1*time.Second)

	// 遇到无法处理的错误，埋葬任务
	_ = q.Bury(job.Meta.ID, 1)
	fmt.Println("Job buried")

	// 无法再 Reserve
	_, err := q.Reserve([]string{"process"}, 100*time.Millisecond)
	if err == ErrTimeout {
		fmt.Println("No job available (buried)")
	}

	// 管理员 Kick 恢复任务
	count, _ := q.Kick("process", 10)
	fmt.Printf("Kicked %d jobs\n", count)

	// 现在可以 Reserve
	job, _ = q.Reserve([]string{"process"}, 1*time.Second)
	fmt.Printf("Reserved job after kick: %d\n", job.Meta.ID)
	_ = q.Delete(job.Meta.ID)

	// Output:
	// Job buried
	// No job available (buried)
	// Kicked 1 jobs
	// Reserved job after kick: 1
}

func ExampleQueue_touch() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加任务（TTR 1秒）
	_, _ = q.Put("long-task", []byte("task"), 1, 0, 1*time.Second)

	// 保留任务
	job, _ := q.Reserve([]string{"long-task"}, 1*time.Second)
	fmt.Printf("Reserved job: %d, TTR: 1s\n", job.Meta.ID)

	// 处理 800ms 后，发现还需要更多时间
	time.Sleep(800 * time.Millisecond)

	// Touch 延长 TTR
	_ = q.Touch(job.Meta.ID) // 重置为原始 TTR
	fmt.Println("Touched job (reset TTR)")

	// 或者延长指定时间
	// q.Touch(job.Meta.ID, 30*time.Second)

	// 继续处理...
	time.Sleep(500 * time.Millisecond)

	// 完成
	_ = q.Delete(job.Meta.ID)
	fmt.Println("Job completed")

	// Output:
	// Reserved job: 1, TTR: 1s
	// Touched job (reset TTR)
	// Job completed
}

func ExampleQueue_stats() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加各种状态的任务
	_, _ = q.Put("email", []byte("task1"), 1, 0, 60*time.Second)             // ready
	_, _ = q.Put("email", []byte("task2"), 1, 5*time.Second, 60*time.Second) // delayed
	job, _ := q.Reserve([]string{"email"}, 1*time.Second)                    // reserved

	// 查看整体统计
	stats := q.Stats()
	fmt.Printf("Total jobs: %d, Ready: %d, Delayed: %d, Reserved: %d\n",
		stats.TotalJobs, stats.ReadyJobs, stats.DelayedJobs, stats.ReservedJobs)

	// 查看 Topic 统计
	topicStats, _ := q.StatsTopic("email")
	fmt.Printf("Topic 'email': Total: %d, Ready: %d, Delayed: %d, Reserved: %d\n",
		topicStats.TotalJobs, topicStats.ReadyJobs, topicStats.DelayedJobs, topicStats.ReservedJobs)

	_ = q.Delete(job.Meta.ID)

	// Output:
	// Total jobs: 2, Ready: 0, Delayed: 1, Reserved: 1
	// Topic 'email': Total: 2, Ready: 0, Delayed: 1, Reserved: 1
}

func ExampleQueue_multipleTopics() {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 添加不同 topic 的任务
	_, _ = q.Put("email", []byte("send email"), 1, 0, 60*time.Second)
	_, _ = q.Put("sms", []byte("send sms"), 1, 0, 60*time.Second)
	_, _ = q.Put("push", []byte("send push"), 1, 0, 60*time.Second)

	// Worker 可以监听多个 topic
	job, _ := q.Reserve([]string{"email", "sms", "push"}, 1*time.Second)
	fmt.Printf("Reserved from topic: %s\n", job.Meta.Topic)
	_ = q.Delete(job.Meta.ID)

	// 列出所有 topic
	topics := q.ListTopics()
	fmt.Printf("Topics: %v\n", topics)

	// Output:
	// Reserved from topic: email
	// Topics: [email push sms]
}

// TestReserve_NoThunderingHerd 测试防止惊群效应
func TestReserve_NoThunderingHerd(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 启动多个 worker 等待任务
		workerCount := 10
		var wg sync.WaitGroup
		var reserveCount atomic.Int32

		for range workerCount {
			wg.Go(func() {
				job, err := q.Reserve([]string{"test"}, 2*time.Second)
				if err == nil && job != nil {
					reserveCount.Add(1)
					_ = q.Delete(job.Meta.ID)
				}
			})
		}

		// 等待 workers 进入等待状态
		time.Sleep(100 * time.Millisecond)

		// 只添加一个任务
		_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		wg.Wait()

		// 只应该有一个 worker 获得任务
		if reserveCount.Load() != 1 {
			t.Errorf("expected 1 worker to reserve job, got %d", reserveCount.Load())
		}
	})
}

// TestTouch_MaxTouches 测试 MaxTouches 限制
func TestTouch_MaxTouches(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()
		config.MaxTouches = 3       // 设置最大 Touch 次数为 3
		config.MinTouchInterval = 0 // 禁用间隔限制
		config.MaxTouchDuration = 0 // 禁用时长限制

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布任务
		_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Reserve 任务
		job, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		// Touch 3 次应该成功
		for i := 0; i < 3; i++ {
			err = q.Touch(job.Meta.ID)
			if err != nil {
				t.Errorf("Touch %d failed: %v", i+1, err)
			}

			// 验证 Touches 计数
			meta, err := q.StatsJob(job.Meta.ID)
			if err != nil {
				t.Fatalf("StatsJob failed: %v", err)
			}
			if meta.Touches != i+1 {
				t.Errorf("Touches = %d, want %d", meta.Touches, i+1)
			}
		}

		// 第 4 次 Touch 应该失败
		err = q.Touch(job.Meta.ID)
		if err == nil {
			t.Error("Touch should fail when exceeding MaxTouches")
		}

		// 清理
		_ = job.Delete()
	})
}

// TestTouch_MinTouchInterval 测试 MinTouchInterval 限制
func TestTouch_MinTouchInterval(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()
		config.MinTouchInterval = 100 * time.Millisecond // 最小间隔 100ms

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布任务
		_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Reserve 任务
		job, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		// 第一次 Touch 应该成功
		err = q.Touch(job.Meta.ID)
		if err != nil {
			t.Errorf("First Touch failed: %v", err)
		}

		// 立即再次 Touch 应该失败（间隔太短）
		err = q.Touch(job.Meta.ID)
		if err == nil {
			t.Error("Touch should fail when interval is too short")
		}

		// 等待足够的间隔
		time.Sleep(150 * time.Millisecond)

		// 现在 Touch 应该成功
		err = q.Touch(job.Meta.ID)
		if err != nil {
			t.Errorf("Touch after interval failed: %v", err)
		}

		// 清理
		_ = job.Delete()
	})
}

// TestTouch_MaxTouchDuration 测试 MaxTouchDuration 限制
func TestTouch_MaxTouchDuration(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()
		config.MaxTouchDuration = 500 * time.Millisecond // 最大延长 500ms
		config.MinTouchInterval = 0                      // 禁用间隔限制

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布任务，TTR 200ms
		_, err = q.Put("test", []byte("task"), 1, 0, 200*time.Millisecond)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Reserve 任务
		job, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		// Touch 延长 200ms，应该成功（总延长 200ms）
		err = job.Touch()
		if err != nil {
			t.Errorf("First Touch failed: %v", err)
		}

		// 再 Touch 延长 200ms，应该成功（总延长 400ms）
		err = job.Touch()
		if err != nil {
			t.Errorf("Second Touch failed: %v", err)
		}

		// 再 Touch 延长 200ms，应该失败（总延长会超过 500ms）
		err = job.Touch()
		if err == nil {
			t.Error("Touch should fail when exceeding MaxTouchDuration")
		}

		// 清理
		_ = job.Delete()
	})
}

// TestTouch_ResetTTR 测试 Touch 重置 TTR
func TestTouch_ResetTTR(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewTimeWheelTicker(10*time.Millisecond, 100)
		config.MaxTouches = 0 // 禁用次数限制

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布任务，TTR 100ms
		jobID, err := q.Put("test", []byte("task"), 1, 0, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Reserve 任务
		job, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		// 等待 80ms（接近超时但未超时）
		time.Sleep(80 * time.Millisecond)

		// Touch 重置 TTR
		err = job.Touch()
		if err != nil {
			t.Fatalf("Touch failed: %v", err)
		}

		// 再等待 80ms（如果没有 Touch，任务应该已经超时）
		time.Sleep(80 * time.Millisecond)

		// 任务应该仍然是 Reserved 状态（因为 Touch 重置了 TTR）
		stats := q.Stats()
		if stats.ReservedJobs != 1 {
			t.Errorf("ReservedJobs = %d, want 1 (Touch should reset TTR)", stats.ReservedJobs)
		}

		// 验证不能再次 Reserve 这个任务
		_, err = q.Reserve([]string{"test"}, 100*time.Millisecond)
		if err != ErrTimeout {
			t.Errorf("Reserve should timeout, got: %v", err)
		}

		// 清理
		_ = q.Delete(jobID)
	})
}

// TestReserve_FIFO 测试 FIFO 公平性
func TestReserve_FIFO(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 记录 reserve 顺序
		var mu sync.Mutex
		order := make([]int, 0)

		// 启动 5 个 worker，按顺序等待
		var wg sync.WaitGroup
		for i := range 5 {
			workerID := i
			wg.Go(func() {
				time.Sleep(time.Duration(workerID) * 10 * time.Millisecond) // 确保按顺序启动
				job, err := q.Reserve([]string{"test"}, 3*time.Second)
				if err == nil && job != nil {
					mu.Lock()
					order = append(order, workerID)
					mu.Unlock()
					_ = q.Delete(job.Meta.ID)
				}
			})
		}

		// 等待所有 workers 进入等待状态
		time.Sleep(200 * time.Millisecond)

		// 添加 5 个任务
		for range 5 {
			_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond) // 确保任务逐个到达
		}

		wg.Wait()

		// 检查顺序应该是 0, 1, 2, 3, 4 (FIFO)
		if len(order) != 5 {
			t.Fatalf("expected 5 workers to reserve jobs, got %d", len(order))
		}

		for i := range 5 {
			if order[i] != i {
				t.Errorf("expected worker %d at position %d, got worker %d", i, i, order[i])
			}
		}
	})
}

// TestReserve_MultipleTopics 测试多 topic 等待
func TestReserve_MultipleTopics(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// Worker 监听多个 topics
		var wg sync.WaitGroup
		wg.Add(1)
		var receivedTopic string

		go func() {
			defer wg.Done()
			job, err := q.Reserve([]string{"email", "sms", "push"}, 2*time.Second)
			if err == nil && job != nil {
				receivedTopic = job.Meta.Topic
				_ = q.Delete(job.Meta.ID)
			}
		}()

		// 等待 worker 进入等待状态
		time.Sleep(100 * time.Millisecond)

		// 在 sms topic 添加任务
		_, err = q.Put("sms", []byte("message"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		wg.Wait()

		if receivedTopic != "sms" {
			t.Errorf("expected to receive from 'sms', got '%s'", receivedTopic)
		}
	})
}

// TestReserve_Timeout 测试超时机制
func TestReserve_Timeout(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		start := time.Now()
		_, err = q.Reserve([]string{"test"}, 500*time.Millisecond)
		elapsed := time.Since(start)

		if err != ErrTimeout {
			t.Errorf("expected ErrTimeout, got %v", err)
		}

		if elapsed < 400*time.Millisecond || elapsed > 600*time.Millisecond {
			t.Errorf("expected ~500ms timeout, got %v", elapsed)
		}
	})
}

// TestReserve_ImmediateAvailable 测试立即可用的任务
func TestReserve_ImmediateAvailable(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 先添加任务
		_, err = q.Put("test", []byte("immediate"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		// Reserve 应该立即返回
		start := time.Now()
		job, err := q.Reserve([]string{"test"}, 5*time.Second)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("expected success, got error: %v", err)
		}

		if job == nil {
			t.Fatal("expected job, got nil")
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("expected immediate return, took %v", elapsed)
		}

		_ = q.Delete(job.Meta.ID)
	})
}

// TestReserve_WaiterCleanup 测试 waiter 超时后的清理
func TestReserve_WaiterCleanup(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 启动多个会超时的 reserve
		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				_, _ = q.Reserve([]string{"test"}, 100*time.Millisecond)
			})
		}

		wg.Wait()

		// 检查等待队列是否已清理
		stats := q.StatsWaiting()
		if len(stats) > 0 {
			for _, s := range stats {
				if s.WaitingWorkers > 0 {
					t.Errorf("expected no waiting workers, got %d for topic %s", s.WaitingWorkers, s.Topic)
				}
			}
		}
	})
}

// BenchmarkReserve_Concurrent 并发 Reserve 性能测试
func BenchmarkReserve_Concurrent(b *testing.B) {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	defer func() { _ = q.Stop() }()
	_ = q.Start()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 添加任务
			id, _ := q.Put("bench", []byte("data"), 1, 0, 60*time.Second)

			// Reserve 任务
			job, err := q.Reserve([]string{"bench"}, 100*time.Millisecond)
			if err == nil && job != nil {
				_ = q.Delete(job.Meta.ID)
			} else {
				// 如果没拿到，删除自己添加的
				_ = q.Delete(id)
			}
		}
	})
}

// TestStress_HighConcurrency 高并发压力测试
// 测试场景：100 个生产者 + 100 个消费者同时工作
func TestStress_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const (
			producers       = 100
			consumers       = 100
			jobsPerProducer = 100
			totalJobs       = producers * jobsPerProducer
		)

		var (
			produced atomic.Int64
			consumed atomic.Int64
			failed   atomic.Int64
		)

		startTime := time.Now()

		// 启动生产者
		var producerWg sync.WaitGroup
		for i := range producers {
			producerWg.Add(1)
			go func(id int) {
				defer producerWg.Done()
				for j := range jobsPerProducer {
					_, err := q.Put(
						fmt.Sprintf("topic-%d", id%10), // 10 个不同的 topic
						fmt.Appendf(nil, "job-%d-%d", id, j),
						uint32(j%10), // 0-9 优先级
						0,
						5*time.Second,
					)
					if err != nil {
						failed.Add(1)
					} else {
						produced.Add(1)
					}
				}
			}(i)
		}

		// 启动消费者
		var consumerWg sync.WaitGroup
		topics := make([]string, 10)
		for i := range 10 {
			topics[i] = fmt.Sprintf("topic-%d", i)
		}

		for i := range consumers {
			consumerWg.Add(1)
			go func(id int) {
				defer consumerWg.Done()
				for {
					// 先检查是否已完成（使用 > 而不是 >=，因为可能正好等于）
					if consumed.Load() >= totalJobs {
						return
					}

					job, err := q.Reserve(topics, 100*time.Millisecond)
					if err == ErrTimeout {
						// 超时，再次检查是否所有任务都已完成
						if consumed.Load() >= totalJobs {
							return
						}
						continue
					}
					if err != nil {
						failed.Add(1)
						continue
					}

					// 模拟处理
					// time.Sleep(time.Microsecond)

					// 先增加计数，如果超过限制则不 Delete（释放回队列）
					newCount := consumed.Add(1)
					if newCount > totalJobs {
						// 超过限制，释放任务回队列
						_ = job.Release(job.Meta.Priority, 0)
						consumed.Add(-1) // 回退计数
						return
					}

					if err := job.Delete(); err != nil {
						failed.Add(1)
						consumed.Add(-1) // Delete 失败，回退计数
					}
				}
			}(i)
		}

		// 等待生产者完成
		producerWg.Wait()
		t.Logf("所有生产者完成，已生产 %d 个任务", produced.Load())

		// 等待消费者完成
		consumerWg.Wait()
		elapsed := time.Since(startTime)

		t.Logf("高并发测试完成:")
		t.Logf("  - 总任务数: %d", totalJobs)
		t.Logf("  - 已生产: %d", produced.Load())
		t.Logf("  - 已消费: %d", consumed.Load())
		t.Logf("  - 失败: %d", failed.Load())
		t.Logf("  - 耗时: %v", elapsed)
		t.Logf("  - 吞吐量: %.0f jobs/sec", float64(totalJobs)/elapsed.Seconds())

		if failed.Load() > 0 {
			t.Errorf("有 %d 个任务失败", failed.Load())
		}

		if consumed.Load() != totalJobs {
			t.Errorf("期望消费 %d 个任务，实际消费 %d", totalJobs, consumed.Load())
		}
	})
}

// TestStress_BurstLoad 突发负载测试
func TestStress_BurstLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const burstSize = 10000

		startTime := time.Now()

		// 突发写入
		var wg sync.WaitGroup
		for i := range burstSize {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				_, _ = q.Put("burst", fmt.Appendf(nil, "job-%d", id), 1, 0, 5*time.Second)
			}(i)
		}
		wg.Wait()
		writeTime := time.Since(startTime)

		t.Logf("突发写入 %d 个任务耗时: %v (%.0f jobs/sec)",
			burstSize, writeTime, float64(burstSize)/writeTime.Seconds())

		// 突发读取
		startTime = time.Now()
		var consumed atomic.Int32
		for range 100 {
			wg.Go(func() {
				for {
					job, err := q.Reserve([]string{"burst"}, 10*time.Millisecond)
					if err == ErrTimeout {
						return
					}
					if err == nil {
						_ = job.Delete()
						consumed.Add(1)
					}
				}
			})
		}
		wg.Wait()
		readTime := time.Since(startTime)

		t.Logf("突发读取 %d 个任务耗时: %v (%.0f jobs/sec)",
			consumed.Load(), readTime, float64(consumed.Load())/readTime.Seconds())
	})
}

// TestStress_DelayedJobs 大量延迟任务测试
func TestStress_DelayedJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const jobCount = 5000

		// 添加大量延迟任务（延迟 100-500ms）
		var wg sync.WaitGroup
		for i := range jobCount {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				delay := time.Duration(100+id%400) * time.Millisecond
				_, _ = q.Put("delayed", fmt.Appendf(nil, "job-%d", id), 1, delay, 5*time.Second)
			}(i)
		}
		wg.Wait()

		t.Logf("已添加 %d 个延迟任务", jobCount)

		// 等待所有任务就绪
		time.Sleep(600 * time.Millisecond)

		// 统计
		stats := q.Stats()
		t.Logf("统计: Ready=%d, Delayed=%d", stats.ReadyJobs, stats.DelayedJobs)

		if stats.ReadyJobs+stats.DelayedJobs != jobCount {
			t.Errorf("任务数量不匹配: Ready=%d, Delayed=%d, Total=%d",
				stats.ReadyJobs, stats.DelayedJobs, jobCount)
		}
	})
}

// BenchmarkPut 基准测试：Put 操作
func BenchmarkPut(b *testing.B) {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = q.Put("bench", []byte("data"), 1, 0, 60*time.Second)
			i++
		}
	})
}

// BenchmarkReserveDelete 基准测试：Reserve + Delete 操作
func BenchmarkReserveDelete(b *testing.B) {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, _ := New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 预填充任务
	for b.Loop() {
		_, _ = q.Put("bench", []byte("data"), 1, 0, 60*time.Second)
	}

	for b.Loop() {
		job, err := q.Reserve([]string{"bench"}, 1*time.Second)
		if err == nil {
			_ = q.Delete(job.Meta.ID)
		}
	}
}

// TestAsyncPut 测试异步 Put 功能
func TestAsyncPut(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}

		// 启动队列
		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		// 快速发布多个任务
		const numJobs = 100
		jobIDs := make([]uint64, numJobs)

		start := time.Now()
		for i := range numJobs {
			id, err := q.Put("test", []byte("body"), 10, 0, 30*time.Second)
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}
			jobIDs[i] = id
		}
		putDuration := time.Since(start)

		// 异步 Put 应该很快（<100ms for 100 jobs）
		// 同步模式每个 Put 需要 1-10ms，100 个任务需要 100-1000ms
		// 异步模式应该 <50ms
		t.Logf("Put %d jobs took: %v (avg: %v per job)", numJobs, putDuration, putDuration/numJobs)

		if putDuration > 200*time.Millisecond {
			t.Errorf("Async Put too slow: %v (expected <200ms)", putDuration)
		}

		// 等待异步存储完成
		time.Sleep(500 * time.Millisecond)

		// 验证所有任务都在队列中（使用 >= 因为可能有恢复的任务）
		stats := q.Stats()
		if stats.TotalJobs < numJobs {
			t.Errorf("TotalJobs = %d, want at least %d", stats.TotalJobs, numJobs)
		}

		// 验证可以 Reserve 任务
		job, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Errorf("Reserve failed: %v", err)
		}
		if job == nil {
			t.Error("Reserve returned nil job")
		} else {
			_ = job.Delete()
		}
	})
}

// TestAsyncPutStop 测试停止时等待异步 Put 完成
func TestAsyncPutStop(t *testing.T) {
	// 创建临时 SQLite 数据库
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	config := DefaultConfig()
	config.Storage = storage

	q, err := New(config)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := q.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// 快速发布任务
	const numJobs = 50
	for range numJobs {
		_, err := q.Put("test", []byte("body"), 10, 0, 30*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// 立即停止（应该等待异步写入完成）
	if err := q.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// 重新打开存储，检查任务是否都被保存
	storage2, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage failed: %v", err)
	}
	defer func() { _ = storage2.Close() }()

	// 扫描所有任务
	result, err := storage2.ScanJobMeta(context.Background(), nil)
	if err != nil {
		t.Fatalf("ScanJobMeta failed: %v", err)
	}

	// 验证所有任务都被保存
	if len(result.Metas) != numJobs {
		t.Errorf("Saved jobs = %d, want %d", len(result.Metas), numJobs)
	}
}

// TestAsyncPutChannelFull 测试通道满时 Put 会阻塞等待
func TestAsyncPutChannelFull(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	config := DefaultConfig()
	config.Storage = storage

	q, err := New(config)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// 启动队列（让 asyncPutWorker 运行）
	if err := q.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = q.Stop() }()

	// 发布大量任务测试批量处理
	const numJobs = 1100
	for i := range numJobs {
		_, err := q.Put("test", []byte("body"), 10, 0, 30*time.Second)
		if err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	// 验证所有任务都在内存队列中
	stats := q.Stats()
	if stats.TotalJobs != numJobs {
		t.Errorf("TotalJobs = %d, want %d", stats.TotalJobs, numJobs)
	}
}

// TestReserve_TTRTimeout 测试 TTR 超时自动恢复
func TestReserve_TTRTimeout(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		// 使用 TimeWheelTicker 确保 TTR 超时能被及时处理
		config.Ticker = NewTimeWheelTicker(10*time.Millisecond, 100)

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布一个 TTR 很短的任务
		jobID, err := q.Put("test", []byte("timeout-task"), 1, 0, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Worker 1 获取任务但不处理
		job1, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		if job1.Meta.ID != jobID {
			t.Errorf("job.ID = %d, want %d", job1.Meta.ID, jobID)
		}

		// 验证任务状态为 Reserved
		stats := q.Stats()
		if stats.ReservedJobs != 1 {
			t.Errorf("ReservedJobs = %d, want 1", stats.ReservedJobs)
		}

		// 等待 TTR 超时（100ms + 一些余量）
		time.Sleep(200 * time.Millisecond)

		// 验证任务已经自动转为 Ready
		stats = q.Stats()
		if stats.ReservedJobs != 0 {
			t.Errorf("ReservedJobs = %d, want 0 (should timeout)", stats.ReservedJobs)
		}
		if stats.ReadyJobs != 1 {
			t.Errorf("ReadyJobs = %d, want 1 (should be released)", stats.ReadyJobs)
		}

		// Worker 2 应该能够重新获取这个任务
		job2, err := q.Reserve([]string{"test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Second Reserve failed: %v", err)
		}

		if job2.Meta.ID != jobID {
			t.Errorf("second job.ID = %d, want %d", job2.Meta.ID, jobID)
		}

		// 验证 Reserves 计数增加
		if job2.Meta.Reserves != 2 {
			t.Errorf("Reserves = %d, want 2", job2.Meta.Reserves)
		}

		// 清理
		_ = job2.Delete()
	})
}

// TestReserve_TTRTimeout_Multiple 测试多个任务 TTR 超时
func TestReserve_TTRTimeout_Multiple(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewTimeWheelTicker(10*time.Millisecond, 100)

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 发布多个任务
		const numJobs = 5
		for i := 0; i < numJobs; i++ {
			_, err := q.Put("test", []byte("timeout"), 1, 0, 100*time.Millisecond)
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Reserve 所有任务但不处理
		jobs := make([]*Job, numJobs)
		for i := 0; i < numJobs; i++ {
			job, err := q.Reserve([]string{"test"}, 1*time.Second)
			if err != nil {
				t.Fatalf("Reserve %d failed: %v", i, err)
			}
			jobs[i] = job
		}

		// 验证所有任务都是 Reserved 状态
		stats := q.Stats()
		if stats.ReservedJobs != numJobs {
			t.Errorf("ReservedJobs = %d, want %d", stats.ReservedJobs, numJobs)
		}

		// 等待所有任务 TTR 超时
		time.Sleep(200 * time.Millisecond)

		// 验证所有任务都转为 Ready
		stats = q.Stats()
		if stats.ReservedJobs != 0 {
			t.Errorf("ReservedJobs = %d, want 0", stats.ReservedJobs)
		}
		if stats.ReadyJobs != numJobs {
			t.Errorf("ReadyJobs = %d, want %d", stats.ReadyJobs, numJobs)
		}

		// 验证可以重新获取所有任务
		for i := 0; i < numJobs; i++ {
			job, err := q.Reserve([]string{"test"}, 1*time.Second)
			if err != nil {
				t.Fatalf("Second Reserve %d failed: %v", i, err)
			}
			if job.Meta.Reserves != 2 {
				t.Errorf("job %d Reserves = %d, want 2", i, job.Meta.Reserves)
			}
			_ = job.Delete()
		}
	})
}
