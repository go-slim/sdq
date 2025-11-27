package sdq

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewTimeWheelTicker(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
	if ticker == nil {
		t.Fatal("NewTimeWheelTicker returned nil")
	}

	tw, ok := ticker.(*TimeWheelTicker)
	if !ok {
		t.Fatal("NewTimeWheelTicker did not return *TimeWheelTicker")
	}

	if tw.tickInterval != 10*time.Millisecond {
		t.Errorf("tickInterval = %v, want %v", tw.tickInterval, 10*time.Millisecond)
	}

	if tw.slots != 100 {
		t.Errorf("slots = %d, want %d", tw.slots, 100)
	}

	if len(tw.wheel) != 100 {
		t.Errorf("wheel length = %d, want %d", len(tw.wheel), 100)
	}
}

func TestTimeWheelTickerDefaults(t *testing.T) {
	ticker := NewTimeWheelTicker(0, 0)

	tw, ok := ticker.(*TimeWheelTicker)
	if !ok {
		t.Fatal("NewTimeWheelTicker did not return *TimeWheelTicker")
	}

	if tw.tickInterval != 10*time.Millisecond {
		t.Errorf("default tickInterval = %v, want %v", tw.tickInterval, 10*time.Millisecond)
	}

	if tw.slots != 3600 {
		t.Errorf("default slots = %d, want %d", tw.slots, 3600)
	}
}

func TestTimeWheelTickerRegister(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
	tw := ticker.(*TimeWheelTicker)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)

	ticker.Register("test", mockObj)

	tw.mu.RLock()
	defer tw.mu.RUnlock()

	if len(tw.registry) != 1 {
		t.Errorf("registry size = %d, want 1", len(tw.registry))
	}

	if _, exists := tw.registry["test"]; !exists {
		t.Error("tickable not registered")
	}

	// Should not be paused after registration
	if tw.paused {
		t.Error("ticker should not be paused after registration")
	}
}

func TestTimeWheelTickerUnregister(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
	tw := ticker.(*TimeWheelTicker)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)

	ticker.Register("test", mockObj)
	ticker.Unregister("test")

	tw.mu.RLock()
	defer tw.mu.RUnlock()

	if len(tw.registry) != 0 {
		t.Errorf("registry size = %d, want 0", len(tw.registry))
	}
}

func TestTimeWheelTickerStartStop(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)

	ticker.Start()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	ticker.Stop()
}

func TestTimeWheelTickerStats(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)
	ticker.Register("test", mockObj)

	stats := ticker.Stats()

	if stats.RegisteredCount != 1 {
		t.Errorf("RegisteredCount = %d, want 1", stats.RegisteredCount)
	}

	if stats.Mode != "timewheel" {
		t.Errorf("Mode = %s, want timewheel", stats.Mode)
	}

	if stats.TimeUntilTick <= 0 {
		t.Error("TimeUntilTick should be > 0")
	}
}

func TestTimeWheelTickerProcessing(t *testing.T) {
	ticker := NewTimeWheelTicker(20*time.Millisecond, 100)

	nextTime := time.Now().Add(40 * time.Millisecond)
	mockObj := newMockTickable(nextTime)

	ticker.Register("test", mockObj)
	ticker.Start()
	defer ticker.Stop()

	// Wait for tick to be processed
	time.Sleep(100 * time.Millisecond)

	tickCount := mockObj.GetTickCount()
	if tickCount == 0 {
		t.Error("ProcessTick should have been called")
	}
}

func TestTimeWheelTickerWakeup(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)
	ticker.Register("test", mockObj)

	// Wakeup should reschedule tasks
	ticker.Wakeup()

	// Should not panic
}

func TestTimeWheelTickerMultipleObjects(t *testing.T) {
	ticker := NewTimeWheelTicker(20*time.Millisecond, 100)

	now := time.Now()
	mock1 := newMockTickable(now.Add(30 * time.Millisecond))
	mock2 := newMockTickable(now.Add(60 * time.Millisecond))

	ticker.Register("obj1", mock1)
	ticker.Register("obj2", mock2)

	ticker.Start()
	defer ticker.Stop()

	// Wait for both to be processed
	time.Sleep(100 * time.Millisecond)

	if mock1.GetTickCount() == 0 {
		t.Error("obj1 should have been ticked")
	}

	if mock2.GetTickCount() == 0 {
		t.Error("obj2 should have been ticked")
	}
}

func TestTimeWheelTickerPauseResume(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
	tw := ticker.(*TimeWheelTicker)

	// Should start paused
	tw.mu.RLock()
	if !tw.paused {
		t.Error("ticker should start paused")
	}
	tw.mu.RUnlock()

	// Register should resume
	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)
	ticker.Register("test", mockObj)

	tw.mu.RLock()
	if tw.paused {
		t.Error("ticker should resume after registration")
	}
	tw.mu.RUnlock()
}

func TestTimeWheelTickerNoObjects(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)

	ticker.Start()
	defer ticker.Stop()

	// Should not panic with no registered objects
	time.Sleep(100 * time.Millisecond)

	stats := ticker.Stats()
	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount = %d, want 0", stats.RegisteredCount)
	}
}

func TestTimeWheelTickerCalculateSlot(t *testing.T) {
	ticker := NewTimeWheelTicker(100*time.Millisecond, 10)
	tw := ticker.(*TimeWheelTicker)

	now := time.Now()
	tw.currentSlot = 0

	// Test various tick times
	tests := []struct {
		tickTime time.Time
		wantSlot int
	}{
		{now, 0},                              // Already expired
		{now.Add(100 * time.Millisecond), 1},  // 1 tick away
		{now.Add(200 * time.Millisecond), 2},  // 2 ticks away
		{now.Add(1000 * time.Millisecond), 0}, // 10 ticks away (wraps to 0)
		{now.Add(-100 * time.Millisecond), 0}, // Past time
	}

	for _, tt := range tests {
		got := tw.calculateSlot(tt.tickTime)
		if got != tt.wantSlot {
			t.Errorf("calculateSlot(%v) = %d, want %d", tt.tickTime.Sub(now), got, tt.wantSlot)
		}
	}
}

func TestTimeWheelTickerHasAnyTask(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 10)
	tw := ticker.(*TimeWheelTicker)

	tw.mu.Lock()
	if tw.hasAnyTask() {
		t.Error("hasAnyTask should return false for empty wheel")
	}

	// Add a task to the wheel
	tw.wheel[0] = append(tw.wheel[0], &tickTask{
		name:     "test",
		tickable: newMockTickable(time.Now()),
	})

	if !tw.hasAnyTask() {
		t.Error("hasAnyTask should return true when tasks exist")
	}
	tw.mu.Unlock()
}

func TestTimeWheelTickerRemoveTask(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 10)
	tw := ticker.(*TimeWheelTicker)

	mockObj := newMockTickable(time.Now().Add(50 * time.Millisecond))
	ticker.Register("test", mockObj)

	tw.mu.Lock()
	tw.removeTask("test")
	tw.mu.Unlock()

	// Verify task is removed from all slots
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	for i := range tw.wheel {
		for _, task := range tw.wheel[i] {
			if task.name == "test" {
				t.Error("task should be removed from all slots")
			}
		}
	}
}

// TestTimeWheelTicker_ConcurrentRegisterUnregister tests high-frequency Register/Unregister operations
func TestTimeWheelTicker_ConcurrentRegisterUnregister(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
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

// TestTimeWheelTicker_ConcurrentProcessTickAndWakeup tests ProcessTick and Wakeup called simultaneously
func TestTimeWheelTicker_ConcurrentProcessTickAndWakeup(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)

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

// TestTimeWheelTicker_ConcurrentOperations tests various concurrent operations
func TestTimeWheelTicker_ConcurrentOperations(t *testing.T) {
	ticker := NewTimeWheelTicker(10*time.Millisecond, 100)
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

// panicTickable 用于测试 panic 恢复的 Tickable 实现
type panicTickable struct {
	mu           sync.Mutex
	nextTickTime time.Time
	tickCount    int
	shouldPanic  bool
	panicMsg     string
}

func newPanicTickable(nextTime time.Time, shouldPanic bool, msg string) *panicTickable {
	return &panicTickable{
		nextTickTime: nextTime,
		shouldPanic:  shouldPanic,
		panicMsg:     msg,
	}
}

func (p *panicTickable) ProcessTick(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tickCount++
	// 重新调度下一次 tick
	p.nextTickTime = now.Add(50 * time.Millisecond)
	if p.shouldPanic {
		panic(p.panicMsg)
	}
}

func (p *panicTickable) NextTickTime() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextTickTime
}

func (p *panicTickable) NeedsTick() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.nextTickTime.IsZero()
}

func (p *panicTickable) GetTickCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tickCount
}

func (p *panicTickable) SetShouldPanic(shouldPanic bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.shouldPanic = shouldPanic
}

// TestTimeWheelTicker_PanicRecovery 测试 ProcessTick panic 时的恢复
func TestTimeWheelTicker_PanicRecovery(t *testing.T) {
	ticker := NewTimeWheelTicker(50*time.Millisecond, 10)
	ticker.Start()
	defer ticker.Stop()

	// 创建一个会 panic 的 tickable 和一个正常的 tickable
	panicObj := newPanicTickable(time.Now().Add(10*time.Millisecond), true, "simulated panic")
	normalObj := newMockTickable(time.Now().Add(10 * time.Millisecond))

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

	// Ticker 应该仍然在运行
	stats := ticker.Stats()
	if stats.RegisteredCount != 2 {
		t.Errorf("RegisteredCount = %d, want 2", stats.RegisteredCount)
	}
}

// TestTimeWheelTicker_MultiplePanics 测试多个任务同时 panic
func TestTimeWheelTicker_MultiplePanics(t *testing.T) {
	ticker := NewTimeWheelTicker(50*time.Millisecond, 10)
	ticker.Start()
	defer ticker.Stop()

	// 创建多个会 panic 的任务
	const numPanicTasks = 5
	panicTasks := make([]*panicTickable, numPanicTasks)
	for i := 0; i < numPanicTasks; i++ {
		panicTasks[i] = newPanicTickable(time.Now().Add(10*time.Millisecond), true, fmt.Sprintf("panic-%d", i))
		ticker.Register(fmt.Sprintf("panic-task-%d", i), panicTasks[i])
	}

	// 等待处理
	time.Sleep(200 * time.Millisecond)

	// 验证所有任务都被调用了（尽管它们 panic）
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

// TestTimeWheelTicker_RecoverAfterPanic 测试 panic 后任务能否继续运行
func TestTimeWheelTicker_RecoverAfterPanic(t *testing.T) {
	ticker := NewTimeWheelTicker(50*time.Millisecond, 10)
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
