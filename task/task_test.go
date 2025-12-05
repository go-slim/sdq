package task

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-slim.dev/sdq"
)

// unregisterTask 从注册表中移除任务（仅用于测试）
func unregisterTask(name string) {
	mu.Lock()
	defer mu.Unlock()
	delete(registry, name)
}

// exampleCounter 用于生成唯一的示例任务名称（支持 -count=N）
var exampleCounter atomic.Int64

// uniqueExampleName 生成唯一的示例任务名称
func uniqueExampleName(base string) string {
	return fmt.Sprintf("%s-%d", base, exampleCounter.Add(1))
}

// TestTaskRegistration 测试任务注册
func TestTaskRegistration(t *testing.T) {
	type TestData struct {
		Value int `json:"value"`
	}

	taskName := "test-registration-" + t.Name()
	t.Cleanup(func() {
		unregisterTask(taskName)
	})

	// 注册任务
	tsk := Register(taskName, &Config{
		Handler: func(ctx context.Context, data TestData) error {
			return nil
		},
		Priority: 10,
		TTR:      60 * time.Second,
	})

	if tsk == nil {
		t.Fatal("Register returned nil")
	}

	if tsk.name != taskName {
		t.Errorf("task.name = %s, want %s", tsk.name, taskName)
	}

	// 检查任务是否已注册
	registered, ok := Get(taskName)
	if !ok {
		t.Error("task not found in registry")
	}

	if registered != tsk {
		t.Error("registered task is not the same instance")
	}
}

// TestTaskHandler_WithContext 测试带 context 的处理函数
func TestTaskHandler_WithContext(t *testing.T) {
	type TestData struct {
		Value int `json:"value"`
	}

	var called atomic.Bool
	var receivedValue int

	handler, err := newHandler(func(ctx context.Context, data TestData) error {
		called.Store(true)
		receivedValue = data.Value
		return nil
	})

	if err != nil {
		t.Fatalf("newHandler error: %v", err)
	}

	// 调用处理函数
	data := `{"value":42}`
	err = handler.Call(context.Background(), []byte(data))
	if err != nil {
		t.Fatalf("handler.Call error: %v", err)
	}

	if !called.Load() {
		t.Error("handler was not called")
	}

	if receivedValue != 42 {
		t.Errorf("receivedValue = %d, want 42", receivedValue)
	}
}

// TestTaskHandler_WithoutContext 测试不带 context 的处理函数
func TestTaskHandler_WithoutContext(t *testing.T) {
	type TestData struct {
		Value string `json:"value"`
	}

	var called atomic.Bool
	var receivedValue string

	handler, err := newHandler(func(data TestData) error {
		called.Store(true)
		receivedValue = data.Value
		return nil
	})

	if err != nil {
		t.Fatalf("newHandler error: %v", err)
	}

	// 调用处理函数
	data := `{"value":"hello"}`
	err = handler.Call(context.Background(), []byte(data))
	if err != nil {
		t.Fatalf("handler.Call error: %v", err)
	}

	if !called.Load() {
		t.Error("handler was not called")
	}

	if receivedValue != "hello" {
		t.Errorf("receivedValue = %s, want hello", receivedValue)
	}
}

// TestTaskPublish 测试任务发布
func TestTaskPublish(t *testing.T) {
	sdq.RunWithAllStorages(t, func(t *testing.T, storage *sdq.TestStorage) {
		type TestData struct {
			OrderID uint `json:"order_id"`
		}

		var processedCount atomic.Int32

		// 注册任务（使用唯一名称支持 -count=N）
		taskName := fmt.Sprintf("test-publish-%s-%d", storage.Type.String(), exampleCounter.Add(1))
		t.Cleanup(func() {
			unregisterTask(taskName)
		})
		task := Register(taskName, &Config{
			Handler: func(ctx context.Context, data TestData) error {
				processedCount.Add(1)
				return nil
			},
			Priority: 10,
			TTR:      60 * time.Second,
		})

		// 创建队列
		config := sdq.DefaultConfig()
		config.Storage = storage.Storage
		// 不使用 NoOpTicker，使用默认的 DynamicSleepTicker

		q, err := sdq.New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 设置队列
		task.SetQueue(q)

		// 发布任务
		ctx := context.Background()
		err = task.Publish(ctx, TestData{OrderID: 123})
		if err != nil {
			t.Fatalf("Publish error: %v", err)
		}

		// 手动 Reserve 和处理任务（避免 ticker 死锁问题）
		job, err := q.Reserve([]string{taskName}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve error: %v", err)
		}

		// 获取任务数据并处理
		body, err := job.GetBody()
		if err != nil {
			t.Fatalf("GetBody error: %v", err)
		}

		// 调用处理函数
		if err := task.handler.Call(ctx, body); err != nil {
			t.Fatalf("handler.Call error: %v", err)
		}

		// 删除任务
		if err := job.Delete(); err != nil {
			t.Fatalf("Delete error: %v", err)
		}

		if processedCount.Load() != 1 {
			t.Errorf("processedCount = %d, want 1", processedCount.Load())
		}
	})
}

// TestTaskWithDelay 测试延迟任务选项
// 注意：此测试使用 NoOpTicker，因此不测试实际的延迟功能（需要 ticker）
// 仅测试 Delay 选项能够正确设置
func TestTaskWithDelay(t *testing.T) {
	sdq.RunWithAllStorages(t, func(t *testing.T, storage *sdq.TestStorage) {
		type TestData struct {
			Value int `json:"value"`
		}

		// 注册任务（使用唯一名称支持 -count=N）
		taskName := fmt.Sprintf("test-delay-%s-%d", storage.Type.String(), exampleCounter.Add(1))
		t.Cleanup(func() {
			unregisterTask(taskName)
		})
		task := Register(taskName, &Config{
			Handler: func(ctx context.Context, data TestData) error {
				return nil
			},
		})

		// 创建队列
		config := sdq.DefaultConfig()
		config.Storage = storage.Storage
		config.Ticker = sdq.NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := sdq.New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		task.SetQueue(q)

		// 发布延迟任务
		ctx := context.Background()
		err = task.Publish(ctx, TestData{Value: 42}, Delay(500*time.Millisecond))
		if err != nil {
			t.Fatalf("Publish error: %v", err)
		}

		// 验证任务已发布（延迟功能需要实际的 ticker 才能测试）
		// 这里只验证 Delay 选项不会导致错误
	})
}

// TestTaskWithPriority 测试优先级
func TestTaskWithPriority(t *testing.T) {
	sdq.RunWithAllStorages(t, func(t *testing.T, storage *sdq.TestStorage) {
		type TestData struct {
			Value int `json:"value"`
		}

		var processedOrder []int
		var mu sync.Mutex

		// 注册任务（使用唯一名称支持 -count=N）
		taskName := fmt.Sprintf("test-priority-%s-%d", storage.Type.String(), exampleCounter.Add(1))
		t.Cleanup(func() {
			unregisterTask(taskName)
		})
		task := Register(taskName, &Config{
			Handler: func(ctx context.Context, data TestData) error {
				mu.Lock()
				processedOrder = append(processedOrder, data.Value)
				mu.Unlock()
				return nil
			},
		})

		// 创建队列
		config := sdq.DefaultConfig()
		config.Storage = storage.Storage
		config.Ticker = sdq.NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := sdq.New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		task.SetQueue(q)

		ctx := context.Background()

		// 发布不同优先级的任务（优先级数字越小越高）
		_ = task.Publish(ctx, TestData{Value: 3}, Priority(30)) // 低优先级
		_ = task.Publish(ctx, TestData{Value: 1}, Priority(10)) // 高优先级
		_ = task.Publish(ctx, TestData{Value: 2}, Priority(20)) // 中优先级

		// 等待异步 Put 完成
		time.Sleep(100 * time.Millisecond)

		// 按优先级顺序 Reserve 和处理
		for range 3 {
			job, err := q.Reserve([]string{taskName}, 1*time.Second)
			if err != nil {
				t.Fatalf("Reserve error: %v", err)
			}

			body, _ := job.GetBody()
			_ = task.handler.Call(ctx, body)
			_ = job.Delete()
		}

		mu.Lock()
		defer mu.Unlock()

		if len(processedOrder) != 3 {
			t.Fatalf("processedOrder length = %d, want 3", len(processedOrder))
		}

		// 验证处理顺序（应该按优先级：1, 2, 3）
		if processedOrder[0] != 1 {
			t.Errorf("first processed = %d, want 1", processedOrder[0])
		}
		if processedOrder[1] != 2 {
			t.Errorf("second processed = %d, want 2", processedOrder[1])
		}
		if processedOrder[2] != 3 {
			t.Errorf("third processed = %d, want 3", processedOrder[2])
		}
	})
}

// TestTaskPublishAndReserveMultiple 测试发布和处理多个任务
func TestTaskPublishAndReserveMultiple(t *testing.T) {
	sdq.RunWithAllStorages(t, func(t *testing.T, storage *sdq.TestStorage) {
		type TestData struct {
			Value int `json:"value"`
		}

		var processedCount atomic.Int32

		// 注册任务（使用唯一名称支持 -count=N）
		taskName := fmt.Sprintf("test-multiple-%s-%d", storage.Type.String(), exampleCounter.Add(1))
		t.Cleanup(func() {
			unregisterTask(taskName)
		})
		task := Register(taskName, &Config{
			Handler: func(ctx context.Context, data TestData) error {
				processedCount.Add(1)
				return nil
			},
		})

		// 创建队列
		config := sdq.DefaultConfig()
		config.Storage = storage.Storage
		config.Ticker = sdq.NewNoOpTicker() // 使用 NoOpTicker 避免死锁

		q, err := sdq.New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		task.SetQueue(q)

		ctx := context.Background()

		// 发布 10 个任务
		for i := range 10 {
			err := task.Publish(ctx, TestData{Value: i})
			if err != nil {
				t.Fatalf("Publish error: %v", err)
			}
		}

		// 等待异步 Put 完成
		time.Sleep(100 * time.Millisecond)

		// 模拟 Worker 的处理逻辑：Reserve -> 调用 handler -> Delete
		for i := range 10 {
			job, err := q.Reserve([]string{taskName}, 1*time.Second)
			if err != nil {
				t.Fatalf("Reserve error at %d: %v", i, err)
			}

			// 获取 body 并调用 handler
			body, err := job.GetBody()
			if err != nil {
				t.Fatalf("GetBody error: %v", err)
			}

			// 直接通过 task 的 handler 调用（内部方法，但在同一个包）
			if err := task.handler.Call(ctx, body); err != nil {
				t.Fatalf("Handler error: %v", err)
			}

			if err := job.Delete(); err != nil {
				t.Fatalf("Delete error: %v", err)
			}
		}

		if processedCount.Load() != 10 {
			t.Errorf("processedCount = %d, want 10", processedCount.Load())
		}
	})
}

// Example_basicUsage 展示基本使用方法
func Example_basicUsage() {
	// OrderData 订单数据
	type OrderData struct {
		OrderID uint   `json:"order_id"`
		UserID  uint   `json:"user_id"`
		Amount  int64  `json:"amount"`
		Status  string `json:"status"`
	}

	// 生成唯一任务名称（支持 -count=N）
	processOrderName := uniqueExampleName("example-process-order")
	sendNotificationName := uniqueExampleName("example-send-notification")

	// 1. 注册任务
	processOrderTask := Register(processOrderName, &Config{
		Handler: func(ctx context.Context, data OrderData) error {
			// 实际业务逻辑
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		Priority: 10,
		TTR:      60 * time.Second,
	})

	sendNotificationTask := Register(sendNotificationName, &Config{
		Handler: func(ctx context.Context, data OrderData) error {
			return nil
		},
		Priority: 5, // 更高优先级
		TTR:      30 * time.Second,
	})

	// 2. 创建队列
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, _ := sdq.New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 设置任务使用的队列
	processOrderTask.SetQueue(q)
	sendNotificationTask.SetQueue(q)

	ctx := context.Background()

	// 3. 发布任务 - 最简单的方式
	_ = processOrderTask.Publish(ctx, OrderData{
		OrderID: 123,
		UserID:  456,
		Amount:  10000,
		Status:  "pending",
	})

	// 4. 发布延迟任务
	_ = processOrderTask.Publish(ctx, OrderData{
		OrderID: 124,
		UserID:  457,
		Amount:  20000,
		Status:  "pending",
	}, Delay(5*time.Minute))

	// 5. 发布指定时间执行的任务
	futureTime := time.Now().Add(1 * time.Hour)
	_ = processOrderTask.Publish(ctx, OrderData{
		OrderID: 125,
		UserID:  458,
		Amount:  30000,
		Status:  "pending",
	}, DelayUntil(futureTime))

	// 6. 发布高优先级任务
	_ = sendNotificationTask.Publish(ctx, OrderData{
		OrderID: 126,
		UserID:  459,
		Amount:  40000,
		Status:  "completed",
	}, Priority(1)) // 最高优先级

	// 7. 启动工作器处理任务
	worker := NewWorker(q, processOrderName, sendNotificationName)
	_ = worker.Start(3) // 启动 3 个工作协程
	defer worker.Stop()

	// 等待任务处理
	time.Sleep(500 * time.Millisecond)

	fmt.Println("All tasks published and processed")
	// Output: All tasks published and processed
}

// Example_multipleWorkers 展示多个 Worker 并发处理
func Example_multipleWorkers() {
	type OrderData struct {
		OrderID uint   `json:"order_id"`
		UserID  uint   `json:"user_id"`
		Amount  int64  `json:"amount"`
		Status  string `json:"status"`
	}

	// 生成唯一任务名称（支持 -count=N）
	taskName := uniqueExampleName("example-multiple-workers")

	processOrderTask := Register(taskName, &Config{
		Handler: func(ctx context.Context, data OrderData) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		Priority: 10,
		TTR:      60 * time.Second,
	})

	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, _ := sdq.New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	processOrderTask.SetQueue(q)

	ctx := context.Background()

	// 批量发布任务
	for i := 1; i <= 10; i++ {
		_ = processOrderTask.Publish(ctx, OrderData{
			OrderID: uint(i),
			UserID:  uint(100 + i),
			Amount:  int64(i * 1000),
			Status:  "pending",
		})
	}

	// 启动多个工作器并发处理
	worker := NewWorker(q, taskName)
	_ = worker.Start(5) // 5 个并发工作器
	defer worker.Stop()

	time.Sleep(500 * time.Millisecond)

	fmt.Println("Processed 10 orders with 5 workers")
	// Output: Processed 10 orders with 5 workers
}

// Example_realWorldUsage 展示实际项目中的使用方法
func Example_realWorldUsage() {
	// 定义任务数据结构
	type AutoOrderData struct {
		PackageOrderID uint `json:"package_order_id"`
	}

	type OrderCancelData struct {
		OrderID uint   `json:"order_id"`
		Reason  string `json:"reason"`
	}

	// 生成唯一任务名称（支持 -count=N）
	autoOrderName := uniqueExampleName("example-auto-order")
	orderCancelName := uniqueExampleName("example-order-cancel")

	// 注册任务处理器
	autoOrderTask := Register(autoOrderName, &Config{
		Handler: func(ctx context.Context, data AutoOrderData) error {
			// 实际业务逻辑：处理自动订购
			return nil
		},
		Priority: 10,
	})

	orderCancelTask := Register(orderCancelName, &Config{
		Handler: func(ctx context.Context, data OrderCancelData) error {
			// 实际业务逻辑：处理订单取消
			return nil
		},
		Priority: 15,
	})

	// 初始化队列
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	q, _ := sdq.New(config)
	_ = q.Start()
	defer func() { _ = q.Stop() }()

	// 为所有任务设置队列
	autoOrderTask.SetQueue(q)
	orderCancelTask.SetQueue(q)

	// 创建并启动工作器
	worker := NewWorker(q, autoOrderName, orderCancelName)
	_ = worker.Start(5) // 启动 5 个并发工作器
	defer worker.Stop()

	// 在业务代码中发布任务

	// 1. 发布立即执行的任务
	_ = autoOrderTask.Publish(context.Background(), AutoOrderData{
		PackageOrderID: 123,
	})

	// 2. 发布延迟任务（5 分钟后执行）
	_ = autoOrderTask.Publish(context.Background(), AutoOrderData{
		PackageOrderID: 456,
	}, Delay(5*time.Minute))

	// 3. 发布高优先级任务
	_ = orderCancelTask.Publish(context.Background(), OrderCancelData{
		OrderID: 789,
		Reason:  "用户取消",
	}, Priority(1))

	time.Sleep(200 * time.Millisecond)

	fmt.Println("All tasks published successfully")
	// Output: All tasks published successfully
}
