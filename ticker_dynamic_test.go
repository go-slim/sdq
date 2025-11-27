package sdq

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// mockTickable is a mock implementation of Tickable for testing
type mockTickable struct {
	mu           sync.Mutex
	nextTickTime time.Time
	tickCount    int
	needsTick    bool
}

func newMockTickable(nextTime time.Time) *mockTickable {
	return &mockTickable{
		nextTickTime: nextTime,
		needsTick:    !nextTime.IsZero(),
	}
}

func (m *mockTickable) ProcessTick(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tickCount++
}

func (m *mockTickable) NextTickTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextTickTime
}

func (m *mockTickable) NeedsTick() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.needsTick
}

func (m *mockTickable) GetTickCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tickCount
}

func (m *mockTickable) SetNextTickTime(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextTickTime = t
	m.needsTick = !t.IsZero()
}

func TestNewDynamicSleepTicker(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
	if ticker == nil {
		t.Fatal("NewDynamicSleepTicker returned nil")
	}

	dt, ok := ticker.(*DynamicSleepTicker)
	if !ok {
		t.Fatal("NewDynamicSleepTicker did not return *DynamicSleepTicker")
	}

	if dt.minInterval != 10*time.Millisecond {
		t.Errorf("minInterval = %v, want %v", dt.minInterval, 10*time.Millisecond)
	}

	if dt.idleInterval != 1*time.Second {
		t.Errorf("idleInterval = %v, want %v", dt.idleInterval, 1*time.Second)
	}
}

func TestDynamicSleepTickerDefaults(t *testing.T) {
	ticker := NewDynamicSleepTicker(0, 0)

	dt, ok := ticker.(*DynamicSleepTicker)
	if !ok {
		t.Fatal("NewDynamicSleepTicker did not return *DynamicSleepTicker")
	}

	if dt.minInterval != 10*time.Millisecond {
		t.Errorf("default minInterval = %v, want %v", dt.minInterval, 10*time.Millisecond)
	}

	if dt.idleInterval != 1*time.Second {
		t.Errorf("default idleInterval = %v, want %v", dt.idleInterval, 1*time.Second)
	}
}

func TestDynamicSleepTickerStartStop(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)

	ticker.Start()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	ticker.Stop()
}

func TestDynamicSleepTickerStatsEmpty(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)

	stats := ticker.Stats()

	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount = %d, want 0", stats.RegisteredCount)
	}

	if stats.Mode != "dynamic" {
		t.Errorf("Mode = %s, want dynamic", stats.Mode)
	}
}

func TestDynamicSleepTickerNoObjects(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 50*time.Millisecond)

	ticker.Start()
	defer ticker.Stop()

	// Should not panic with no registered objects
	time.Sleep(100 * time.Millisecond)

	stats := ticker.Stats()
	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount = %d, want 0", stats.RegisteredCount)
	}
}

// TestDynamicSleepTicker_RegisterAndTick 测试注册和 tick 处理
func TestDynamicSleepTicker_RegisterAndTick(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// 注册一个需要立即 tick 的对象
	mock := newMockTickable(time.Now().Add(-1 * time.Second)) // 过去的时间，应该立即 tick
	ticker.Register("test", mock)

	// 等待 tick 处理
	time.Sleep(100 * time.Millisecond)

	// 验证 tick 被调用
	tickCount := mock.GetTickCount()
	if tickCount == 0 {
		t.Error("ProcessTick should have been called")
	}

	// 验证注册统计
	stats := ticker.Stats()
	if stats.RegisteredCount != 1 {
		t.Errorf("RegisteredCount = %d, want 1", stats.RegisteredCount)
	}

	// 注销
	ticker.Unregister("test")

	stats = ticker.Stats()
	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount after unregister = %d, want 0", stats.RegisteredCount)
	}
}

// TestDynamicSleepTicker_MultipleObjects 测试多对象调度
func TestDynamicSleepTicker_MultipleObjects(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// 注册多个需要 tick 的对象
	mocks := make([]*mockTickable, 5)
	for i := range mocks {
		mocks[i] = newMockTickable(time.Now().Add(-1 * time.Second))
		ticker.Register(string(rune('a'+i)), mocks[i])
	}

	// 等待 tick 处理
	time.Sleep(100 * time.Millisecond)

	// 验证所有对象都被 tick
	for i, mock := range mocks {
		tickCount := mock.GetTickCount()
		if tickCount == 0 {
			t.Errorf("Mock %d was not ticked", i)
		}
	}

	// 验证统计
	stats := ticker.Stats()
	if stats.RegisteredCount != 5 {
		t.Errorf("RegisteredCount = %d, want 5", stats.RegisteredCount)
	}
}

// TestDynamicSleepTicker_Wakeup 测试 Wakeup 功能
func TestDynamicSleepTicker_Wakeup(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 5*time.Second) // 长的空闲间隔
	ticker.Start()
	defer ticker.Stop()

	// 注册一个需要 tick 的对象
	mock := newMockTickable(time.Now().Add(-1 * time.Second))
	ticker.Register("test", mock)

	// 立即调用 Wakeup
	ticker.Wakeup()

	// 等待一小段时间
	time.Sleep(50 * time.Millisecond)

	// 验证 tick 被调用（即使空闲间隔很长）
	tickCount := mock.GetTickCount()
	if tickCount == 0 {
		t.Error("ProcessTick should have been called after Wakeup")
	}
}

// TestDynamicSleepTicker_DynamicScheduling 测试动态调度
func TestDynamicSleepTicker_DynamicScheduling(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// 注册一个未来需要 tick 的对象
	nextTick := time.Now().Add(50 * time.Millisecond)
	mock := newMockTickable(nextTick)
	ticker.Register("test", mock)

	// 在 nextTick 之前不应该 tick
	time.Sleep(20 * time.Millisecond)
	if mock.GetTickCount() > 0 {
		t.Error("Should not tick before NextTickTime")
	}

	// 等待到 nextTick 之后
	time.Sleep(50 * time.Millisecond)

	// 现在应该已经 tick
	tickCount := mock.GetTickCount()
	if tickCount == 0 {
		t.Error("Should tick after NextTickTime")
	}
}

// TestDynamicSleepTicker_NoTickNeeded 测试不需要 tick 的对象
func TestDynamicSleepTicker_NoTickNeeded(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 100*time.Millisecond)
	ticker.Start()
	defer ticker.Stop()

	// 注册一个不需要 tick 的对象
	mock := newMockTickable(time.Time{}) // 零值表示不需要 tick
	ticker.Register("test", mock)

	// 等待一段时间
	time.Sleep(150 * time.Millisecond)

	// 不应该被 tick
	tickCount := mock.GetTickCount()
	if tickCount > 0 {
		t.Errorf("Should not tick when NeedsTick is false, got %d ticks", tickCount)
	}
}

// TestDynamicSleepTicker_UpdateNextTickTime 测试动态更新下次 tick 时间
func TestDynamicSleepTicker_UpdateNextTickTime(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// 注册一个远期 tick 的对象
	mock := newMockTickable(time.Now().Add(10 * time.Second))
	ticker.Register("test", mock)

	// 等待一小段时间
	time.Sleep(50 * time.Millisecond)

	// 不应该被 tick
	if mock.GetTickCount() > 0 {
		t.Error("Should not tick yet")
	}

	// 更新为立即需要 tick
	mock.SetNextTickTime(time.Now().Add(-1 * time.Second))
	ticker.Wakeup() // 唤醒 ticker

	// 等待 tick 处理
	time.Sleep(50 * time.Millisecond)

	// 现在应该被 tick
	tickCount := mock.GetTickCount()
	if tickCount == 0 {
		t.Error("Should tick after updating NextTickTime")
	}
}

// TestDynamicSleepTicker_ConcurrentRegisterUnregister tests high-frequency Register/Unregister operations
func TestDynamicSleepTicker_ConcurrentRegisterUnregister(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	const numGoroutines = 50
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				name := fmt.Sprintf("obj-%d-%d", id, j)
				mock := newMockTickable(time.Now().Add(50 * time.Millisecond))
				ticker.Register(name, mock)
				ticker.Unregister(name)
			}
		}(i)
	}

	wg.Wait()

	// All objects should be unregistered
	stats := ticker.Stats()
	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount = %d, want 0", stats.RegisteredCount)
	}
}

// TestDynamicSleepTicker_ConcurrentProcessTickAndWakeup tests ProcessTick and Wakeup called simultaneously
func TestDynamicSleepTicker_ConcurrentProcessTickAndWakeup(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)

	// Register multiple objects
	const numObjects = 20
	for i := 0; i < numObjects; i++ {
		name := fmt.Sprintf("obj-%d", i)
		mock := newMockTickable(time.Now().Add(30 * time.Millisecond))
		ticker.Register(name, mock)
	}

	ticker.Start()
	defer ticker.Stop()

	// Call Wakeup concurrently while ProcessTick is running
	const numWakeups = 100
	var wg sync.WaitGroup
	wg.Add(numWakeups)

	for i := 0; i < numWakeups; i++ {
		go func() {
			defer wg.Done()
			ticker.Wakeup()
		}()
	}

	// Wait a bit for processing
	time.Sleep(100 * time.Millisecond)

	wg.Wait()

	// Should not panic and stats should be consistent
	stats := ticker.Stats()
	if stats.RegisteredCount != numObjects {
		t.Errorf("RegisteredCount = %d, want %d", stats.RegisteredCount, numObjects)
	}
}

// TestDynamicSleepTicker_ConcurrentOperations tests various concurrent operations
func TestDynamicSleepTicker_ConcurrentOperations(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	const duration = 500 * time.Millisecond
	stopTime := time.Now().Add(duration)

	var wg sync.WaitGroup

	// Concurrent registrations
	wg.Add(1)
	go func() {
		defer wg.Done()
		counter := 0
		for time.Now().Before(stopTime) {
			name := fmt.Sprintf("reg-%d", counter)
			mock := newMockTickable(time.Now().Add(20 * time.Millisecond))
			ticker.Register(name, mock)
			counter++
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Concurrent unregistrations
	wg.Add(1)
	go func() {
		defer wg.Done()
		counter := 0
		for time.Now().Before(stopTime) {
			name := fmt.Sprintf("reg-%d", counter)
			ticker.Unregister(name)
			counter++
			time.Sleep(7 * time.Millisecond)
		}
	}()

	// Concurrent stats queries
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(stopTime) {
			_ = ticker.Stats()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Concurrent wakeups
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(stopTime) {
			ticker.Wakeup()
			time.Sleep(15 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Should complete without panics or deadlocks
}

// TestDynamicSleepTicker_PanicRecovery 测试 ProcessTick panic 时的恢复
func TestDynamicSleepTicker_PanicRecovery(t *testing.T) {
	ticker := NewDynamicSleepTicker(50*time.Millisecond, 100*time.Millisecond)
	ticker.Start()
	defer ticker.Stop()

	// 创建一个会 panic 的 tickable 和一个正常的 tickable
	panicObj := newPanicTickable(time.Now().Add(60*time.Millisecond), true, "simulated panic")
	normalObj := newPanicTickable(time.Now().Add(60*time.Millisecond), false, "")

	ticker.Register("panic-task", panicObj)
	ticker.Register("normal-task", normalObj)

	// 等待 tick 处理
	time.Sleep(200 * time.Millisecond)

	// 验证正常任务仍然被处理（即使另一个任务 panic）
	normalTickCount := normalObj.GetTickCount()
	panicTickCount := panicObj.GetTickCount()

	if normalTickCount == 0 {
		t.Error("Normal task should have been ticked despite panic in another task")
	}

	if panicTickCount == 0 {
		t.Error("Panic task should have been called at least once")
	}

	t.Logf("Normal task ticked %d times, panic task ticked %d times", normalTickCount, panicTickCount)
}

// TestDynamicSleepTicker_MultiplePanics 测试多个任务同时 panic
func TestDynamicSleepTicker_MultiplePanics(t *testing.T) {
	ticker := NewDynamicSleepTicker(50*time.Millisecond, 100*time.Millisecond)
	ticker.Start()
	defer ticker.Stop()

	// 创建多个会 panic 的任务
	numPanicTasks := 5
	panicTasks := make([]*panicTickable, numPanicTasks)
	for i := 0; i < numPanicTasks; i++ {
		panicTasks[i] = newPanicTickable(time.Now().Add(60*time.Millisecond), true, fmt.Sprintf("panic %d", i))
		ticker.Register(fmt.Sprintf("panic-task-%d", i), panicTasks[i])
	}

	// 等待处理
	time.Sleep(200 * time.Millisecond)

	// 验证所有任务都被调用了
	for i, task := range panicTasks {
		count := task.GetTickCount()
		if count == 0 {
			t.Errorf("Panic task %d was not ticked", i)
		}
	}

	// Ticker 应该仍然正常运行
	stats := ticker.Stats()
	if stats.RegisteredCount != numPanicTasks {
		t.Errorf("RegisteredCount = %d, want %d", stats.RegisteredCount, numPanicTasks)
	}
}

// TestDynamicSleepTicker_RecoverAfterPanic 测试 panic 后任务能否继续运行
func TestDynamicSleepTicker_RecoverAfterPanic(t *testing.T) {
	ticker := NewDynamicSleepTicker(50*time.Millisecond, 100*time.Millisecond)
	ticker.Start()
	defer ticker.Stop()

	// 创建一个任务，第一次 panic，然后正常运行
	task := newPanicTickable(time.Now().Add(60*time.Millisecond), true, "first panic")
	ticker.Register("recover-task", task)

	// 等待第一次 tick（会 panic）
	time.Sleep(150 * time.Millisecond)

	firstCount := task.GetTickCount()
	if firstCount == 0 {
		t.Error("Task should have been ticked at least once")
	}
	t.Logf("After first wait: task ticked %d times", firstCount)

	// 禁用 panic
	task.SetShouldPanic(false)

	// 等待更多 tick
	time.Sleep(200 * time.Millisecond)

	secondCount := task.GetTickCount()
	if secondCount <= firstCount {
		t.Errorf("Task should continue ticking after panic recovery, first=%d, second=%d", firstCount, secondCount)
	}

	t.Logf("Task ticked %d times total (including panic)", secondCount)
}
