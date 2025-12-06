package sdq

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateEnqueued, "enqueued"},
		{StateReady, "ready"},
		{StateDelayed, "delayed"},
		{StateReserved, "reserved"},
		{StateBuried, "buried"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %s, want %s", tt.state, got, tt.want)
		}
	}
}

func TestNewJobMeta(t *testing.T) {
	id := uint64(1)
	topic := "test-topic"
	priority := uint32(10)
	delay := 5 * time.Second
	ttr := 30 * time.Second

	meta := NewJobMeta(id, topic, priority, delay, ttr)

	if meta.ID != id {
		t.Errorf("ID = %d, want %d", meta.ID, id)
	}

	if meta.Topic != topic {
		t.Errorf("Topic = %s, want %s", meta.Topic, topic)
	}

	if meta.Priority != priority {
		t.Errorf("Priority = %d, want %d", meta.Priority, priority)
	}

	if meta.State != StateEnqueued {
		t.Errorf("State = %v, want %v", meta.State, StateEnqueued)
	}

	if meta.Delay != delay {
		t.Errorf("Delay = %v, want %v", meta.Delay, delay)
	}

	if meta.TTR != ttr {
		t.Errorf("TTR = %v, want %v", meta.TTR, ttr)
	}

	if meta.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}

	// With delay, ReadyAt should be in the future
	if !meta.ReadyAt.After(meta.CreatedAt) {
		t.Error("ReadyAt should be after CreatedAt when delay > 0")
	}

	// Counters should be zero
	if meta.Reserves != 0 || meta.Timeouts != 0 || meta.Releases != 0 ||
		meta.Buries != 0 || meta.Kicks != 0 || meta.Touches != 0 {
		t.Error("Counters should be zero")
	}

}

func TestNewJobMetaNoDelay(t *testing.T) {
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)

	// Without delay, ReadyAt should be close to CreatedAt
	diff := meta.ReadyAt.Sub(meta.CreatedAt)
	if diff < 0 || diff > time.Millisecond {
		t.Errorf("ReadyAt should be close to CreatedAt when delay = 0, diff = %v", diff)
	}

}

func TestJobMetaClone(t *testing.T) {
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateReady
	meta.Reserves = 5

	clone := meta.Clone()

	if clone == meta {
		t.Error("Clone should return a different pointer")
	}

	if clone.ID != meta.ID {
		t.Errorf("Clone.ID = %d, want %d", clone.ID, meta.ID)
	}

	if clone.State != meta.State {
		t.Errorf("Clone.State = %v, want %v", clone.State, meta.State)
	}

	if clone.Reserves != meta.Reserves {
		t.Errorf("Clone.Reserves = %d, want %d", clone.Reserves, meta.Reserves)
	}

	// Modifying clone should not affect original
	clone.Reserves = 10
	if meta.Reserves == 10 {
		t.Error("Modifying clone should not affect original")
	}

}

func TestJobMetaShouldBeReady(t *testing.T) {
	now := time.Now()

	// Test delayed job that should be ready
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateDelayed
	meta.ReadyAt = now.Add(-1 * time.Second) // Past

	if !meta.ShouldBeReady(now) {
		t.Error("Should be ready when ReadyAt is in the past")
	}

	// Test delayed job that should not be ready
	meta.ReadyAt = now.Add(1 * time.Second) // Future
	if meta.ShouldBeReady(now) {
		t.Error("Should not be ready when ReadyAt is in the future")
	}

	// Test non-delayed job
	meta.State = StateReady
	if meta.ShouldBeReady(now) {
		t.Error("ShouldBeReady should return false for non-delayed state")
	}

}

func TestJobMetaShouldTimeout(t *testing.T) {
	now := time.Now()

	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateReserved
	meta.ReservedAt = now.Add(-31 * time.Second) // Reserved 31 seconds ago, TTR is 30s

	if !meta.ShouldTimeout(now) {
		t.Error("Should timeout when past TTR deadline")
	}

	// Not yet timeout
	meta.ReservedAt = now.Add(-10 * time.Second) // Reserved 10 seconds ago
	if meta.ShouldTimeout(now) {
		t.Error("Should not timeout when within TTR")
	}

	// Not reserved state
	meta.State = StateReady
	if meta.ShouldTimeout(now) {
		t.Error("ShouldTimeout should return false for non-reserved state")
	}

}

func TestJobMetaReserveDeadline(t *testing.T) {
	now := time.Now()

	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateReserved
	meta.ReservedAt = now

	deadline := meta.ReserveDeadline()
	expected := now.Add(30 * time.Second)

	if !deadline.Equal(expected) {
		t.Errorf("ReserveDeadline = %v, want %v", deadline, expected)
	}

	// Non-reserved state
	meta.State = StateReady
	deadline = meta.ReserveDeadline()
	if !deadline.IsZero() {
		t.Error("ReserveDeadline should return zero time for non-reserved state")
	}

}

func TestJobMetaTimeUntilReady(t *testing.T) {
	now := time.Now()

	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateDelayed
	meta.ReadyAt = now.Add(10 * time.Second)

	duration := meta.TimeUntilReady(now)
	if duration < 9*time.Second || duration > 11*time.Second {
		t.Errorf("TimeUntilReady = %v, want ~10s", duration)
	}

	// Already ready
	meta.ReadyAt = now.Add(-1 * time.Second)
	duration = meta.TimeUntilReady(now)
	if duration != 0 {
		t.Errorf("TimeUntilReady = %v, want 0 when already past", duration)
	}

	// Non-delayed state
	meta.State = StateReady
	duration = meta.TimeUntilReady(now)
	if duration != 0 {
		t.Errorf("TimeUntilReady = %v, want 0 for non-delayed state", duration)
	}

}

func TestJobMetaTimeUntilTimeout(t *testing.T) {
	now := time.Now()

	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateReserved
	meta.ReservedAt = now

	duration := meta.TimeUntilTimeout(now)
	if duration < 29*time.Second || duration > 31*time.Second {
		t.Errorf("TimeUntilTimeout = %v, want ~30s", duration)
	}

	// Already timeout
	meta.ReservedAt = now.Add(-31 * time.Second)
	duration = meta.TimeUntilTimeout(now)
	if duration != 0 {
		t.Errorf("TimeUntilTimeout = %v, want 0 when already past", duration)
	}

	// Non-reserved state
	meta.State = StateReady
	duration = meta.TimeUntilTimeout(now)
	if duration != 0 {
		t.Errorf("TimeUntilTimeout = %v, want 0 for non-reserved state", duration)
	}

}

func TestNewJob(t *testing.T) {
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	job := NewJob(meta, body, nil)

	if job.Meta != meta {
		t.Error("Job.Meta should be the same as provided")
	}

	gotBody, err := job.GetBody()
	if err != nil {
		t.Errorf("GetBody error: %v", err)
	}

	if string(gotBody) != string(body) {
		t.Errorf("Body = %s, want %s", gotBody, body)
	}

	// Test Body() convenience method
	if string(job.Body()) != string(body) {
		t.Errorf("Body() = %s, want %s", job.Body(), body)
	}

}

func TestNewJobWithStorage(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, storage *TestStorage) {
		meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)

		job := NewJobWithStorage(meta, storage.Storage, nil)

		// NewJobWithStorage 会克隆 meta，所以比较内容而不是指针
		if job.Meta.ID != meta.ID || job.Meta.Topic != meta.Topic {
			t.Error("Job.Meta should have the same content as provided")
		}

		// Body should be nil initially (not loaded)
		// GetBody will try to load from storage but fail since nothing was saved
		_, err := job.GetBody()
		if err == nil {
			t.Error("GetBody should fail when job not in storage")
		}

	})
}

// TestJob_OperationMethods 测试 Job 的操作方法
func TestJob_OperationMethods(t *testing.T) {
	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	// 使用 NoOpTicker 避免 ticker 的死锁问题
	config.Ticker = NewNoOpTicker()

	q, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	// 测试 Delete
	t.Run("Delete", func(t *testing.T) {
		id, _ := q.Put("test-delete", []byte("test delete"), 1, 0, 60*time.Second)
		job, err := q.Reserve([]string{"test-delete"}, TestTimeout(1*time.Second))
		if err != nil {
			t.Fatal(err)
		}

		if err := job.Delete(); err != nil {
			t.Errorf("job.Delete() failed: %v", err)
		}

		// 验证任务已删除
		_, err = q.Peek(id)
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}
	})

	// 测试 Release
	t.Run("Release", func(t *testing.T) {
		id, _ := q.Put("test-release", []byte("test release"), 1, 0, 60*time.Second)
		job, err := q.Reserve([]string{"test-release"}, TestTimeout(1*time.Second))
		if err != nil {
			t.Fatal(err)
		}

		if err := job.Release(2, 0); err != nil {
			t.Errorf("job.Release() failed: %v", err)
		}

		// 验证任务已重新入队
		meta, err := q.StatsJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.State != StateReady {
			t.Errorf("expected StateReady after release, got %v", meta.State)
		}
		if meta.Priority != 2 {
			t.Errorf("expected priority 2, got %d", meta.Priority)
		}

		// 清理
		_ = q.Delete(id)
	})

	// 测试 Bury
	t.Run("Bury", func(t *testing.T) {
		id, _ := q.Put("test-bury", []byte("test bury"), 1, 0, 60*time.Second)
		job, err := q.Reserve([]string{"test-bury"}, TestTimeout(1*time.Second))
		if err != nil {
			t.Fatal(err)
		}

		if err := job.Bury(5); err != nil {
			t.Errorf("job.Bury() failed: %v", err)
		}

		// 验证任务已埋葬
		meta, err := q.StatsJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.State != StateBuried {
			t.Errorf("expected StateBuried after bury, got %v", meta.State)
		}
		if meta.Priority != 5 {
			t.Errorf("expected priority 5, got %d", meta.Priority)
		}

		// 清理
		_ = q.Delete(id)
	})

	// 测试 Kick
	t.Run("Kick", func(t *testing.T) {
		id, _ := q.Put("test-kick", []byte("test kick"), 1, 0, 60*time.Second)
		job, err := q.Reserve([]string{"test-kick"}, TestTimeout(1*time.Second))
		if err != nil {
			t.Fatal(err)
		}

		// 先 bury
		if err := job.Bury(5); err != nil {
			t.Fatal(err)
		}

		// 验证已埋葬
		meta, err := q.StatsJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.State != StateBuried {
			t.Fatalf("expected StateBuried after bury, got %v", meta.State)
		}

		// 测试 kick（使用原来的 job 对象，它持有正确的 ID）
		if err := job.Kick(); err != nil {
			t.Errorf("job.Kick() failed: %v", err)
		}

		// 验证任务已恢复
		meta, err = q.StatsJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.State != StateReady {
			t.Errorf("expected StateReady after kick, got %v", meta.State)
		}

		// 清理
		_ = q.Delete(id)
	})

	// 测试 Touch
	t.Run("Touch", func(t *testing.T) {
		_, _ = q.Put("test-touch", []byte("test touch"), 1, 0, 5*time.Second)
		job, err := q.Reserve([]string{"test-touch"}, TestTimeout(1*time.Second))
		if err != nil {
			t.Fatal(err)
		}

		oldDeadline := job.Meta.ReserveDeadline()

		// 延长 TTR
		TestSleep(100 * time.Millisecond)
		if err := job.Touch(10 * time.Second); err != nil {
			t.Errorf("job.Touch() failed: %v", err)
		}

		// 获取新的 deadline
		meta, _ := q.StatsJob(job.Meta.ID)
		newDeadline := meta.ReserveDeadline()

		if !newDeadline.After(oldDeadline) {
			t.Errorf("expected deadline to be extended, old: %v, new: %v", oldDeadline, newDeadline)
		}

		// 清理
		_ = job.Delete()
	})
}

// TestJobMeta_BoundaryValues 测试 JobMeta 的边界值情况
func TestJobMeta_BoundaryValues(t *testing.T) {
	t.Run("MaxJobID", func(t *testing.T) {
		// 测试最大 JobID
		maxID := uint64(math.MaxUint64)
		meta := NewJobMeta(maxID, "test", 1, 0, 30*time.Second)

		if meta.ID != maxID {
			t.Errorf("ID = %d, want %d", meta.ID, maxID)
		}

	})

	t.Run("MaxPriority", func(t *testing.T) {
		// 测试最大优先级
		maxPriority := uint32(math.MaxUint32)
		meta := NewJobMeta(1, "test", maxPriority, 0, 30*time.Second)

		if meta.Priority != maxPriority {
			t.Errorf("Priority = %d, want %d", meta.Priority, maxPriority)
		}

	})

	t.Run("ZeroPriority", func(t *testing.T) {
		// 测试零优先级（应该是最高优先级）
		meta := NewJobMeta(1, "test", 0, 0, 30*time.Second)

		if meta.Priority != 0 {
			t.Errorf("Priority = %d, want 0", meta.Priority)
		}

	})

	t.Run("ExtremelyLongDelay", func(t *testing.T) {
		// 测试极长延迟（100年）
		longDelay := 100 * 365 * 24 * time.Hour
		meta := NewJobMeta(1, "test", 1, longDelay, 30*time.Second)

		if meta.Delay != longDelay {
			t.Errorf("Delay = %v, want %v", meta.Delay, longDelay)
		}

		// ReadyAt 应该是未来很久
		if !meta.ReadyAt.After(time.Now().Add(99 * 365 * 24 * time.Hour)) {
			t.Error("ReadyAt should be far in the future")
		}

	})

	t.Run("ZeroDelay", func(t *testing.T) {
		// 测试零延迟（立即可用）
		meta := NewJobMeta(1, "test", 1, 0, 30*time.Second)

		if meta.Delay != 0 {
			t.Errorf("Delay = %v, want 0", meta.Delay)
		}

		// ReadyAt 应该接近 CreatedAt
		diff := meta.ReadyAt.Sub(meta.CreatedAt)
		if diff < 0 || diff > time.Millisecond {
			t.Errorf("ReadyAt should be close to CreatedAt, diff = %v", diff)
		}

	})

	t.Run("ZeroTTR", func(t *testing.T) {
		// 测试零 TTR（可能导致立即超时）
		meta := NewJobMeta(1, "test", 1, 0, 0)

		if meta.TTR != 0 {
			t.Errorf("TTR = %v, want 0", meta.TTR)
		}

		// 将任务标记为 Reserved
		meta.State = StateReserved
		meta.ReservedAt = time.Now()

		// 应该立即超时
		if !meta.ShouldTimeout(time.Now()) {
			t.Error("Job with zero TTR should timeout immediately")
		}

	})

	t.Run("VeryLongTTR", func(t *testing.T) {
		// 测试超长 TTR（100年）
		longTTR := 100 * 365 * 24 * time.Hour
		meta := NewJobMeta(1, "test", 1, 0, longTTR)

		if meta.TTR != longTTR {
			t.Errorf("TTR = %v, want %v", meta.TTR, longTTR)
		}

	})

	t.Run("MinimalJobMeta", func(t *testing.T) {
		// 测试所有参数都是最小值
		meta := NewJobMeta(0, "", 0, 0, 0)

		if meta.ID != 0 {
			t.Errorf("ID = %d, want 0", meta.ID)
		}
		if meta.Topic != "" {
			t.Errorf("Topic = %s, want empty", meta.Topic)
		}
		if meta.Priority != 0 {
			t.Errorf("Priority = %d, want 0", meta.Priority)
		}

	})
}

// TestJobMeta_CounterOverflow 测试计数器溢出情况
func TestJobMeta_CounterOverflow(t *testing.T) {
	meta := NewJobMeta(1, "test", 1, 0, 30*time.Second)

	// 将计数器设置为接近最大值
	meta.Reserves = math.MaxInt - 1
	meta.Timeouts = math.MaxInt - 1
	meta.Releases = math.MaxInt - 1
	meta.Buries = math.MaxInt - 1
	meta.Kicks = math.MaxInt - 1
	meta.Touches = math.MaxInt - 1

	// 递增应该不会导致 panic（即使溢出）
	meta.Reserves++
	meta.Timeouts++
	meta.Releases++
	meta.Buries++
	meta.Kicks++
	meta.Touches++

	// 验证值
	if meta.Reserves != math.MaxInt {
		t.Errorf("Reserves = %d, want %d", meta.Reserves, math.MaxInt)
	}

}

// TestJob_ConcurrentOperations 测试同一 Job 被多个操作同时修改
func TestJob_ConcurrentOperations(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 创建多个任务
		const numJobs = 10
		jobIDs := make([]uint64, numJobs)
		for i := range numJobs {
			id, err := q.Put("concurrent-ops", fmt.Appendf(nil, "job-%d", i), 1, 0, 5*time.Second)
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}
			jobIDs[i] = id
		}

		var wg sync.WaitGroup
		const numWorkers = 20

		// 多个 worker 并发操作这些任务
		for i := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for range 5 {
					// Reserve
					job, err := q.Reserve([]string{"concurrent-ops"}, 100*time.Millisecond)
					if err != nil {
						continue
					}

					// 随机操作
					switch workerID % 4 {
					case 0:
						// Delete
						_ = job.Delete()
					case 1:
						// Release
						_ = q.Release(job.Meta.ID, 1, 0)
					case 2:
						// Touch
						_ = job.Touch()
						TestSleep(10 * time.Millisecond)
						_ = job.Delete()
					case 3:
						// Bury
						_ = job.Bury(5)
					}
				}
			}(i)
		}

		wg.Wait()

		// 验证没有 panic 或 deadlock
		stats := q.Stats()
		t.Logf("Final stats: Total=%d, Ready=%d, Reserved=%d, Buried=%d",
			stats.TotalJobs, stats.ReadyJobs, stats.ReservedJobs, stats.BuriedJobs)
	})
}

// TestJob_ConcurrentTouch 测试并发 Touch 操作
func TestJob_ConcurrentTouch(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()
		config.MaxTouches = 0       // 禁用次数限制
		config.MaxTouchDuration = 0 // 禁用时长限制
		config.MinTouchInterval = 0 // 禁用间隔限制

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		// 创建并 Reserve 一个任务
		id, _ := q.Put("touch-test", []byte("data"), 1, 0, 60*time.Second)
		job, err := q.Reserve([]string{"touch-test"}, TestTimeout(1*time.Second))
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}

		var wg sync.WaitGroup
		const numGoroutines = 50
		successCount := make([]int, numGoroutines)

		// 多个 goroutine 同时 Touch 同一个任务
		for i := range numGoroutines {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				err := job.Touch()
				if err == nil {
					successCount[idx] = 1
				}
				TestSleep(time.Millisecond)
			}(i)
		}

		wg.Wait()

		// 统计成功次数
		total := 0
		for _, count := range successCount {
			total += count
		}

		t.Logf("Touch operations: %d/%d succeeded", total, numGoroutines)

		// 验证 Touches 计数
		meta, err := q.StatsJob(id)
		if err != nil {
			t.Fatalf("StatsJob failed: %v", err)
		}

		t.Logf("Final Touches count: %d", meta.Touches)

		// 清理
		_ = job.Delete()
	})
}
