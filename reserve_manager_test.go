package sdq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewReserveManager 测试创建 reserveManager
func TestNewReserveManager(t *testing.T) {
	ctx := context.Background()
	rm := newReserveManager(ctx)

	if rm == nil {
		t.Fatal("newReserveManager returned nil")
		return
	}

	if rm.waitingConns == nil {
		t.Error("waitingConns not initialized")
	}

	if rm.ctx == nil {
		t.Error("ctx not set")
	}
}

// TestReserveManagerStartStop 测试启动和停止
func TestReserveManagerStartStop(t *testing.T) {
	ctx := context.Background()
	rm := newReserveManager(ctx)

	rm.start()

	// 等待一下确保启动完成
	time.Sleep(100 * time.Millisecond)

	rm.stop()

	// 停止后 context 应该被取消
	select {
	case <-rm.ctx.Done():
		// 正确
	default:
		t.Error("context not cancelled after stop")
	}
}

// TestReserveImmediate 测试立即可用的任务
func TestReserveImmediate(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 先添加任务
		_, err = q.Put("test", []byte("immediate"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Reserve 应该立即返回
		start := time.Now()
		job, err := q.Reserve([]string{"test"}, 5*time.Second)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		if job == nil {
			t.Fatal("Reserve returned nil job")
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("Reserve took %v, expected immediate return", elapsed)
		}

		_ = job.Delete()
	})
}

// TestReserveTimeout 测试超时机制
func TestReserveTimeout(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 没有任务，应该超时
		start := time.Now()
		_, err = q.Reserve([]string{"test"}, 500*time.Millisecond)
		elapsed := time.Since(start)

		if err != ErrTimeout {
			t.Errorf("expected ErrTimeout, got %v", err)
		}

		if elapsed < 400*time.Millisecond || elapsed > 600*time.Millisecond {
			t.Errorf("timeout elapsed = %v, expected ~500ms", elapsed)
		}
	})
}

// TestReserveWaitAndNotify 测试等待和通知机制
func TestReserveWaitAndNotify(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 启动 goroutine 等待任务
		var wg sync.WaitGroup
		var receivedJob *Job

		wg.Go(func() {
			job, err := q.Reserve([]string{"test"}, 3*time.Second)
			if err == nil {
				receivedJob = job
			}
		})

		// 等待 worker 进入等待状态
		time.Sleep(200 * time.Millisecond)

		// 添加任务（应该唤醒等待的 worker）
		_, err = q.Put("test", []byte("notify"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		wg.Wait()

		if receivedJob == nil {
			t.Fatal("worker did not receive job")
		}

		_ = receivedJob.Delete()
	})
}

// TestReserveFIFO 测试 FIFO 公平性
func TestReserveFIFO(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
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
					_ = job.Delete()
				}
			})
		}

		// 等待所有 workers 进入等待状态
		time.Sleep(200 * time.Millisecond)

		// 添加 5 个任务
		for range 5 {
			_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
			if err != nil {
				t.Fatalf("Put failed: %v", err)
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

// TestReserveNoThunderingHerd 测试防止惊群效应
func TestReserveNoThunderingHerd(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
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
					_ = job.Delete()
				}
			})
		}

		// 等待 workers 进入等待状态
		time.Sleep(100 * time.Millisecond)

		// 只添加一个任务
		_, err = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		wg.Wait()

		// 只应该有一个 worker 获得任务
		if reserveCount.Load() != 1 {
			t.Errorf("expected 1 worker to reserve job, got %d", reserveCount.Load())
		}
	})
}

// TestReserveMultipleTopics 测试监听多个 topics
func TestReserveMultipleTopics(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// Worker 监听多个 topics
		var wg sync.WaitGroup
		var receivedTopic string

		wg.Go(func() {
			job, err := q.Reserve([]string{"email", "sms", "push"}, 2*time.Second)
			if err == nil && job != nil {
				receivedTopic = job.Meta.Topic
				_ = job.Delete()
			}
		})

		// 等待 worker 进入等待状态
		time.Sleep(100 * time.Millisecond)

		// 在 sms topic 添加任务
		_, err = q.Put("sms", []byte("message"), 1, 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		wg.Wait()

		if receivedTopic != "sms" {
			t.Errorf("expected to receive from 'sms', got '%s'", receivedTopic)
		}
	})
}

// TestReserveCleanupExpiredWaiters 测试清理过期的 waiters
func TestReserveCleanupExpiredWaiters(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 启动多个会超时的 reserve
		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				_, _ = q.Reserve([]string{"test"}, 100*time.Millisecond)
			})
		}

		wg.Wait()

		// 等待清理任务执行
		time.Sleep(2 * time.Second)

		// 检查等待队列是否已清理
		stats := q.reserveMgr.stats()
		if len(stats) > 0 {
			for topic, count := range stats {
				if count > 0 {
					t.Errorf("expected no waiting workers for topic %s, got %d", topic, count)
				}
			}
		}
	})
}

// TestReserveMaxWaiters 测试最大等待数量限制
func TestReserveMaxWaiters(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 启动大量 workers 等待
		var wg sync.WaitGroup
		var errCount atomic.Int32

		// MaxWaitersPerTopic = 1000
		for range 1100 {
			wg.Go(func() {
				_, err := q.Reserve([]string{"test"}, 100*time.Millisecond)
				if err == ErrTooManyWaiters {
					errCount.Add(1)
				}
			})
		}

		wg.Wait()

		// 应该有一些 workers 因为超过限制而失败
		if errCount.Load() < 50 {
			t.Errorf("expected some ErrTooManyWaiters errors, got %d", errCount.Load())
		}
	})
}

// TestReserveStats 测试等待统计
func TestReserveStats(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 启动一些 workers 等待
		var wg sync.WaitGroup
		for range 5 {
			wg.Go(func() {
				_, _ = q.Reserve([]string{"test"}, 2*time.Second)
			})
		}

		// 等待 workers 进入等待状态
		time.Sleep(200 * time.Millisecond)

		// 检查统计
		stats := q.StatsWaiting()
		found := false
		for _, s := range stats {
			if s.Topic == "test" && s.WaitingWorkers == 5 {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("expected 5 waiting workers for topic 'test', stats: %+v", stats)
		}

		// 添加任务，让 workers 退出
		for range 5 {
			_, _ = q.Put("test", []byte("task"), 1, 0, 60*time.Second)
		}

		wg.Wait()
	})
}

// TestReserveContextCancellation 测试 context 取消
func TestReserveContextCancellation(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 启动 reserve 等待
		var wg sync.WaitGroup
		var reserveErr error

		wg.Go(func() {
			_, reserveErr = q.Reserve([]string{"test"}, 5*time.Second)
		})

		// 等待进入等待状态
		time.Sleep(100 * time.Millisecond)

		// 停止队列（会取消 context）
		_ = q.Stop()

		wg.Wait()

		// 应该返回 context 取消错误
		if reserveErr == nil {
			t.Error("expected error when context cancelled, got nil")
		}
	})
}

// TestReserve_ZeroTimeout 测试零超时的 Reserve
func TestReserve_ZeroTimeout(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 零超时应该返回 ErrInvalidTimeout
		start := time.Now()
		_, err = q.Reserve([]string{"test"}, 0)
		elapsed := time.Since(start)

		if err != ErrInvalidTimeout {
			t.Errorf("Reserve with zero timeout = %v, want ErrInvalidTimeout", err)
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("Reserve with zero timeout took %v, should be immediate", elapsed)
		}

		// 即使有任务，零超时也应该返回 ErrInvalidTimeout
		_, err = q.Put("test", []byte("body"), 1, 0, 30*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		_, err = q.Reserve([]string{"test"}, 0)
		if err != ErrInvalidTimeout {
			t.Errorf("Reserve with zero timeout (job available) = %v, want ErrInvalidTimeout", err)
		}
	})
}

// TestReserve_NegativeTimeout 测试负超时
func TestReserve_NegativeTimeout(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 负超时应该返回 ErrInvalidTimeout
		start := time.Now()
		_, err = q.Reserve([]string{"test"}, -1*time.Second)
		elapsed := time.Since(start)

		if err != ErrInvalidTimeout {
			t.Errorf("Reserve with negative timeout = %v, want ErrInvalidTimeout", err)
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("Reserve with negative timeout took %v, should be immediate", elapsed)
		}
	})
}

// TestReserve_VeryLongTimeout 测试非常长的超时
func TestReserve_VeryLongTimeout(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 启动一个非常长超时的 Reserve
		var wg sync.WaitGroup
		var reserveErr error
		var job *Job

		wg.Go(func() {
			job, reserveErr = q.Reserve([]string{"test"}, 24*time.Hour)
		})

		// 等待进入等待状态
		time.Sleep(100 * time.Millisecond)

		// 添加任务应该立即唤醒等待
		start := time.Now()
		jobID, err := q.Put("test", []byte("body"), 1, 0, 30*time.Second)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		wg.Wait()
		elapsed := time.Since(start)

		// 应该快速返回（不需要等待 24 小时）
		if reserveErr != nil {
			t.Errorf("Reserve failed: %v", reserveErr)
		}
		if job == nil || job.Meta.ID != jobID {
			t.Error("Reserve returned wrong job")
		}
		if elapsed > 1*time.Second {
			t.Errorf("Reserve took %v to wake up, should be immediate", elapsed)
		}
	})
}

// TestReserve_ConcurrentTimeouts 测试并发的超时情况
func TestReserve_ConcurrentTimeouts(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 启动多个不同超时时间的 Reserve
		var wg sync.WaitGroup
		timeouts := []time.Duration{
			100 * time.Millisecond,
			200 * time.Millisecond,
			300 * time.Millisecond,
			400 * time.Millisecond,
			500 * time.Millisecond,
		}

		errors := make([]error, len(timeouts))
		start := time.Now()

		for i, timeout := range timeouts {
			wg.Add(1)
			go func(idx int, to time.Duration) {
				defer wg.Done()
				_, errors[idx] = q.Reserve([]string{"nonexistent"}, to)
			}(i, timeout)
		}

		wg.Wait()
		totalElapsed := time.Since(start)

		// 所有 Reserve 都应该超时
		for i, err := range errors {
			if err != ErrTimeout {
				t.Errorf("Reserve %d with timeout %v = %v, want ErrTimeout", i, timeouts[i], err)
			}
		}

		// 总时间应该接近最长的超时（不是所有超时之和）
		if totalElapsed > 700*time.Millisecond {
			t.Errorf("Concurrent reserves took %v, expected ~500ms (longest timeout)", totalElapsed)
		}

		t.Logf("Concurrent timeouts completed in %v", totalElapsed)
	})
}

// TestReserve_EmptyTopicList 测试空 topic 列表
func TestReserve_EmptyTopicList(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// 空 topic 列表应该返回错误
		_, err = q.Reserve([]string{}, 1*time.Second)
		if err == nil {
			t.Error("Reserve with empty topic list should fail")
		}

		// nil topic 列表应该返回错误
		_, err = q.Reserve(nil, 1*time.Second)
		if err == nil {
			t.Error("Reserve with nil topic list should fail")
		}
	})
}
