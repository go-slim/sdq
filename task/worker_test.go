package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-slim.dev/sdq"
)

// TestNewWorker 测试创建 Worker
func TestNewWorker(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, err := sdq.New(config)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer func() { _ = q.Stop() }()

	// 注册任务
	task1 := Register("test-task-1", &Config{
		Handler: func(data string) error {
			return nil
		},
	})

	task2 := Register("test-task-2", &Config{
		Handler: func(data int) error {
			return nil
		},
	})

	defer func() {
		mu.Lock()
		delete(registry, "test-task-1")
		delete(registry, "test-task-2")
		mu.Unlock()
	}()

	worker := NewWorker(q, "test-task-1", "test-task-2")

	if worker == nil {
		t.Fatal("NewWorker() returned nil")
	}

	if worker.queue != q {
		t.Error("worker.queue is not set correctly")
	}

	if len(worker.topics) != 2 {
		t.Errorf("len(worker.topics) = %d, want 2", len(worker.topics))
	}

	if len(worker.handlers) != 2 {
		t.Errorf("len(worker.handlers) = %d, want 2", len(worker.handlers))
	}

	if worker.handlers["test-task-1"] != task1 {
		t.Error("handler for test-task-1 not set correctly")
	}

	if worker.handlers["test-task-2"] != task2 {
		t.Error("handler for test-task-2 not set correctly")
	}
}

// TestWorkerStartStop 测试 Worker 启动和停止
func TestWorkerStartStop(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, err := sdq.New(config)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	Register("test-start-stop", &Config{
		Handler: func(data string) error {
			return nil
		},
	})
	defer func() {
		mu.Lock()
		delete(registry, "test-start-stop")
		mu.Unlock()
	}()

	worker := NewWorker(q, "test-start-stop")

	// 测试启动
	err = worker.Start(3)
	if err != nil {
		t.Errorf("Start() error: %v", err)
	}

	// 等待 workers 启动
	time.Sleep(100 * time.Millisecond)

	// 测试停止
	worker.Stop()

	// 停止应该等待所有 workers 完成
	// 如果没有正确等待，会导致 panic 或其他问题
}

// TestWorkerHandleJob 测试 Worker 处理任务
func TestWorkerHandleJob(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, err := sdq.New(config)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	type TestData struct {
		Value string `json:"value"`
	}

	var processed atomic.Int32
	var receivedMu sync.Mutex
	var receivedValue string

	taskHandler := Register("test-handle", &Config{
		Handler: func(ctx context.Context, data TestData) error {
			processed.Add(1)
			receivedMu.Lock()
			receivedValue = data.Value
			receivedMu.Unlock()
			return nil
		},
		Priority: 10,
		TTR:      30 * time.Second,
	})
	defer func() {
		mu.Lock()
		delete(registry, "test-handle")
		mu.Unlock()
	}()

	taskHandler.SetQueue(q)

	worker := NewWorker(q, "test-handle")
	_ = worker.Start(1)
	defer worker.Stop()

	// 发布任务
	err = taskHandler.Publish(context.Background(), TestData{Value: "hello"})
	if err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	// 等待任务处理
	time.Sleep(500 * time.Millisecond)

	if processed.Load() != 1 {
		t.Errorf("processed = %d, want 1", processed.Load())
	}

	receivedMu.Lock()
	gotValue := receivedValue
	receivedMu.Unlock()
	if gotValue != "hello" {
		t.Errorf("receivedValue = %q, want %q", gotValue, "hello")
	}
}

// TestWorkerMultipleJobs 测试 Worker 处理多个任务
func TestWorkerMultipleJobs(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, err := sdq.New(config)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	var processed atomic.Int32

	taskHandler := Register("test-multiple", &Config{
		Handler: func(data int) error {
			processed.Add(1)
			time.Sleep(10 * time.Millisecond) // 模拟处理时间
			return nil
		},
		Priority: 10,
		TTR:      30 * time.Second,
	})
	defer func() {
		mu.Lock()
		delete(registry, "test-multiple")
		mu.Unlock()
	}()

	taskHandler.SetQueue(q)

	worker := NewWorker(q, "test-multiple")
	_ = worker.Start(3) // 3 个并发 worker
	defer worker.Stop()

	// 发布 10 个任务
	for i := range 10 {
		err = taskHandler.Publish(context.Background(), i)
		if err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// 等待所有任务处理完成
	time.Sleep(1 * time.Second)

	if processed.Load() != 10 {
		t.Errorf("processed = %d, want 10", processed.Load())
	}
}

// TestWorkerHandlerError 测试处理器返回错误时的行为
func TestWorkerHandlerError(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, err := sdq.New(config)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	var processed atomic.Int32

	taskHandler := Register("test-error", &Config{
		Handler: func(data string) error {
			processed.Add(1)
			return errors.New("processing failed")
		},
		Priority: 10,
		TTR:      30 * time.Second,
	})
	defer func() {
		mu.Lock()
		delete(registry, "test-error")
		mu.Unlock()
	}()

	taskHandler.SetQueue(q)

	worker := NewWorker(q, "test-error")
	_ = worker.Start(1)
	defer worker.Stop()

	// 发布任务
	err = taskHandler.Publish(context.Background(), "test")
	if err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	// 等待任务处理
	time.Sleep(500 * time.Millisecond)

	if processed.Load() != 1 {
		t.Errorf("processed = %d, want 1", processed.Load())
	}

	// 检查任务被埋葬
	stats, err := q.StatsTopic("test-error")
	if err != nil {
		t.Fatalf("StatsTopic() error: %v", err)
	}

	if stats.BuriedJobs != 1 {
		t.Errorf("BuriedJobs = %d, want 1", stats.BuriedJobs)
	}
}

// TestWorkerNoHandler 测试没有注册处理器的情况
func TestWorkerNoHandler(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, err := sdq.New(config)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 创建 worker 但不注册任务
	worker := NewWorker(q, "non-existent-task")

	if len(worker.handlers) != 0 {
		t.Errorf("len(worker.handlers) = %d, want 0", len(worker.handlers))
	}

	_ = worker.Start(1)
	defer worker.Stop()

	// 手动发布一个任务到这个 topic
	_, err = q.Put("non-existent-task", []byte(`{"test":"data"}`), 10, 0, 30*time.Second)
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	// 等待一下，worker 应该会获取任务但无法处理（没有 handler）
	time.Sleep(500 * time.Millisecond)

	// 任务应该被埋葬（因为没有 handler）
	stats, err := q.StatsTopic("non-existent-task")
	if err != nil {
		t.Fatalf("StatsTopic() error: %v", err)
	}

	if stats.BuriedJobs != 1 {
		t.Errorf("BuriedJobs = %d, want 1 (task should be buried when no handler)", stats.BuriedJobs)
	}
}

// TestWorkerConcurrency 测试多个 Worker 并发处理
func TestWorkerConcurrency(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, err := sdq.New(config)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	var processed atomic.Int32

	taskHandler := Register("test-concurrency", &Config{
		Handler: func(data int) error {
			processed.Add(1)
			time.Sleep(50 * time.Millisecond) // 模拟处理时间
			return nil
		},
		Priority: 10,
		TTR:      30 * time.Second,
	})
	defer func() {
		mu.Lock()
		delete(registry, "test-concurrency")
		mu.Unlock()
	}()

	taskHandler.SetQueue(q)

	// 启动 5 个并发 worker
	worker := NewWorker(q, "test-concurrency")
	_ = worker.Start(5)
	defer worker.Stop()

	// 发布 20 个任务
	const numJobs = 20
	for i := range numJobs {
		err = taskHandler.Publish(context.Background(), i)
		if err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// 等待所有任务处理完成
	// 5 个 worker，每个任务 50ms，20 个任务应该在 ~250ms 内完成
	time.Sleep(1 * time.Second)

	if processed.Load() != numJobs {
		t.Errorf("processed = %d, want %d", processed.Load(), numJobs)
	}

	// 验证所有任务都已完成
	stats, err := q.StatsTopic("test-concurrency")
	if err != nil {
		t.Fatalf("StatsTopic() error: %v", err)
	}

	if stats.TotalJobs != 0 {
		t.Errorf("TotalJobs = %d, want 0 (all jobs should be deleted)", stats.TotalJobs)
	}
}

// TestWorker_PanicRecovery 测试 Worker 在 handler panic 后能否继续运行
func TestWorker_PanicRecovery(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, err := sdq.New(config)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	var processed atomic.Int32
	var panicked atomic.Bool

	taskHandler := Register("test-panic-recovery", &Config{
		Handler: func(data int) error {
			processed.Add(1)
			if data == 5 {
				// 第 5 个任务会 panic
				panicked.Store(true)
				panic("simulated panic in handler")
			}
			return nil
		},
		Priority:   10,
		TTR:        30 * time.Second,
		MaxRetries: 0, // 不重试
	})
	defer func() {
		mu.Lock()
		delete(registry, "test-panic-recovery")
		mu.Unlock()
	}()

	taskHandler.SetQueue(q)

	worker := NewWorker(q, "test-panic-recovery")
	_ = worker.Start(1)
	defer worker.Stop()

	// 发布 10 个任务，其中第 5 个会 panic
	for i := 1; i <= 10; i++ {
		err = taskHandler.Publish(context.Background(), i)
		if err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// 等待所有任务处理
	time.Sleep(2 * time.Second)

	// 验证至少处理了一部分任务（可能不是全部，取决于 panic 处理）
	processedCount := processed.Load()
	t.Logf("Processed %d tasks (panic occurred: %v)", processedCount, panicked.Load())

	if !panicked.Load() {
		t.Error("Expected panic to occur but it didn't")
	}

	// 检查是否有任务被埋葬（panic 的任务）
	stats, err := q.StatsTopic("test-panic-recovery")
	if err != nil {
		t.Fatalf("StatsTopic() error: %v", err)
	}

	// 注意：当前实现可能没有 panic 恢复，这个测试会暴露这个问题
	t.Logf("Topic stats: Ready=%d, Buried=%d, Total=%d", stats.ReadyJobs, stats.BuriedJobs, stats.TotalJobs)
}

// TestWorker_MultiplePanics 测试多个 panic 不会导致 worker 完全崩溃
func TestWorker_MultiplePanics(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, err := sdq.New(config)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	var processed atomic.Int32
	var panicCount atomic.Int32

	taskHandler := Register("test-multiple-panics", &Config{
		Handler: func(data int) error {
			processed.Add(1)
			if data%2 == 0 {
				// 偶数任务会 panic
				panicCount.Add(1)
				panic("panic on even number")
			}
			return nil
		},
		Priority:   10,
		TTR:        30 * time.Second,
		MaxRetries: 0,
	})
	defer func() {
		mu.Lock()
		delete(registry, "test-multiple-panics")
		mu.Unlock()
	}()

	taskHandler.SetQueue(q)

	worker := NewWorker(q, "test-multiple-panics")
	_ = worker.Start(2) // 2 个并发 worker
	defer worker.Stop()

	// 发布 20 个任务，其中 10 个会 panic
	for i := 1; i <= 20; i++ {
		err = taskHandler.Publish(context.Background(), i)
		if err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// 等待处理
	time.Sleep(3 * time.Second)

	processedCount := processed.Load()
	panicCountValue := panicCount.Load()

	t.Logf("Processed %d tasks, %d panics occurred", processedCount, panicCountValue)

	// 检查统计
	stats, err := q.StatsTopic("test-multiple-panics")
	if err != nil {
		t.Fatalf("StatsTopic() error: %v", err)
	}

	t.Logf("Topic stats: Ready=%d, Buried=%d, Total=%d", stats.ReadyJobs, stats.BuriedJobs, stats.TotalJobs)
}
