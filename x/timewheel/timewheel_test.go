package timewheel

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

func TestNew(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)
	if ticker == nil {
		t.Fatal("New returned nil")
		return
	}

	if ticker.tickInterval != 10*time.Millisecond {
		t.Errorf("tickInterval = %v, want %v", ticker.tickInterval, 10*time.Millisecond)
	}

	if ticker.slots != 100 {
		t.Errorf("slots = %d, want %d", ticker.slots, 100)
	}

	if len(ticker.wheel) != 100 {
		t.Errorf("wheel length = %d, want %d", len(ticker.wheel), 100)
	}
}

func TestDefaults(t *testing.T) {
	ticker := New(0, 0)

	if ticker.tickInterval != 10*time.Millisecond {
		t.Errorf("default tickInterval = %v, want %v", ticker.tickInterval, 10*time.Millisecond)
	}

	if ticker.slots != 3600 {
		t.Errorf("default slots = %d, want %d", ticker.slots, 3600)
	}
}

func TestRegister(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)

	ticker.Register("test", mockObj)

	ticker.mu.RLock()
	defer ticker.mu.RUnlock()

	if len(ticker.registry) != 1 {
		t.Errorf("registry size = %d, want 1", len(ticker.registry))
	}

	if _, exists := ticker.registry["test"]; !exists {
		t.Error("tickable not registered")
	}

	// Should not be paused after registration
	if ticker.paused {
		t.Error("ticker should not be paused after registration")
	}
}

func TestUnregister(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)

	ticker.Register("test", mockObj)
	ticker.Unregister("test")

	ticker.mu.RLock()
	defer ticker.mu.RUnlock()

	if len(ticker.registry) != 0 {
		t.Errorf("registry size = %d, want 0", len(ticker.registry))
	}
}

func TestStartStop(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)

	ticker.Start()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	ticker.Stop()
}

func TestStats(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)
	ticker.Register("test", mockObj)

	stats := ticker.Stats()

	if stats.RegisteredCount != 1 {
		t.Errorf("RegisteredCount = %d, want 1", stats.RegisteredCount)
	}

	if stats.Name != "timewheel" {
		t.Errorf("Name = %s, want timewheel", stats.Name)
	}

	if stats.TimeUntilTick <= 0 {
		t.Error("TimeUntilTick should be > 0")
	}
}

func TestProcessing(t *testing.T) {
	ticker := New(20*time.Millisecond, 100)

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

func TestWakeup(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)

	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)
	ticker.Register("test", mockObj)

	// Wakeup should reschedule tasks
	ticker.Wakeup()

	// Should not panic
}

func TestMultipleObjects(t *testing.T) {
	ticker := New(20*time.Millisecond, 100)

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

func TestPauseResume(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)

	// Should start paused
	ticker.mu.RLock()
	if !ticker.paused {
		t.Error("ticker should start paused")
	}
	ticker.mu.RUnlock()

	// Register should resume
	nextTime := time.Now().Add(50 * time.Millisecond)
	mockObj := newMockTickable(nextTime)
	ticker.Register("test", mockObj)

	ticker.mu.RLock()
	if ticker.paused {
		t.Error("ticker should resume after registration")
	}
	ticker.mu.RUnlock()
}

func TestNoObjects(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)

	ticker.Start()
	defer ticker.Stop()

	// Should not panic with no registered objects
	time.Sleep(100 * time.Millisecond)

	stats := ticker.Stats()
	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount = %d, want 0", stats.RegisteredCount)
	}
}

func TestCalculateSlot(t *testing.T) {
	ticker := New(100*time.Millisecond, 10)

	now := time.Now()
	ticker.currentSlot = 0

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
		got := ticker.calculateSlot(tt.tickTime)
		if got != tt.wantSlot {
			t.Errorf("calculateSlot(%v) = %d, want %d", tt.tickTime.Sub(now), got, tt.wantSlot)
		}
	}
}

func TestHasAnyTask(t *testing.T) {
	ticker := New(10*time.Millisecond, 10)

	ticker.mu.Lock()
	if ticker.hasAnyTask() {
		t.Error("hasAnyTask should return false for empty wheel")
	}

	// Add a task to the wheel
	ticker.wheel[0] = append(ticker.wheel[0], &tickTask{
		name:     "test",
		tickable: newMockTickable(time.Now()),
	})

	if !ticker.hasAnyTask() {
		t.Error("hasAnyTask should return true when tasks exist")
	}
	ticker.mu.Unlock()
}

func TestRemoveTask(t *testing.T) {
	ticker := New(10*time.Millisecond, 10)

	mockObj := newMockTickable(time.Now().Add(50 * time.Millisecond))
	ticker.Register("test", mockObj)

	ticker.mu.Lock()
	ticker.removeTask("test")
	ticker.mu.Unlock()

	// Verify task is removed from all slots
	ticker.mu.RLock()
	defer ticker.mu.RUnlock()

	for i := range ticker.wheel {
		for _, task := range ticker.wheel[i] {
			if task.name == "test" {
				t.Error("task should be removed from all slots")
			}
		}
	}
}

// TestConcurrentRegisterUnregister tests high-frequency Register/Unregister operations
func TestConcurrentRegisterUnregister(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)
	ticker.Start()
	defer ticker.Stop()

	const numGoroutines = 50
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range operationsPerGoroutine {
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

// TestConcurrentProcessTickAndWakeup tests ProcessTick and Wakeup called simultaneously
func TestConcurrentProcessTickAndWakeup(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)

	// Register multiple objects
	const numObjects = 20
	for i := range numObjects {
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

	for range numWakeups {
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

// TestConcurrentOperations tests various concurrent operations
func TestConcurrentOperations(t *testing.T) {
	ticker := New(10*time.Millisecond, 100)
	ticker.Start()
	defer ticker.Stop()

	const duration = 500 * time.Millisecond
	stopTime := time.Now().Add(duration)

	var wg sync.WaitGroup

	// Concurrent registrations
	wg.Go(func() {
		counter := 0
		for time.Now().Before(stopTime) {
			name := fmt.Sprintf("reg-%d", counter)
			mock := newMockTickable(time.Now().Add(20 * time.Millisecond))
			ticker.Register(name, mock)
			counter++
			time.Sleep(5 * time.Millisecond)
		}
	})

	// Concurrent unregistrations
	wg.Go(func() {
		counter := 0
		for time.Now().Before(stopTime) {
			name := fmt.Sprintf("reg-%d", counter)
			ticker.Unregister(name)
			counter++
			time.Sleep(7 * time.Millisecond)
		}
	})

	// Concurrent stats queries
	wg.Go(func() {
		for time.Now().Before(stopTime) {
			_ = ticker.Stats()
			time.Sleep(10 * time.Millisecond)
		}
	})

	// Concurrent wakeups
	wg.Go(func() {
		for time.Now().Before(stopTime) {
			ticker.Wakeup()
			time.Sleep(15 * time.Millisecond)
		}
	})

	wg.Wait()

	// Should complete without panics or deadlocks
}

// panicTickable is a Tickable implementation for testing panic recovery
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
	// Reschedule next tick
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

// TestPanicRecovery tests recovery when ProcessTick panics
func TestPanicRecovery(t *testing.T) {
	ticker := New(50*time.Millisecond, 10)
	ticker.Start()
	defer ticker.Stop()

	// Create a tickable that panics and a normal one
	panicObj := newPanicTickable(time.Now().Add(10*time.Millisecond), true, "simulated panic")
	normalObj := newMockTickable(time.Now().Add(10 * time.Millisecond))

	ticker.Register("panic-task", panicObj)
	ticker.Register("normal-task", normalObj)

	// Wait for tick processing
	time.Sleep(200 * time.Millisecond)

	// Verify normal task was still processed (even though another task panicked)
	normalTickCount := normalObj.GetTickCount()
	panicTickCount := panicObj.GetTickCount()

	if normalTickCount == 0 {
		t.Error("Normal task should have been ticked despite panic in another task")
	}

	if panicTickCount == 0 {
		t.Error("Panic task should have been called at least once")
	}

	t.Logf("Normal task ticked %d times, panic task ticked %d times", normalTickCount, panicTickCount)

	// Ticker should still be running
	stats := ticker.Stats()
	if stats.RegisteredCount != 2 {
		t.Errorf("RegisteredCount = %d, want 2", stats.RegisteredCount)
	}
}

// TestMultiplePanics tests multiple tasks panicking simultaneously
func TestMultiplePanics(t *testing.T) {
	ticker := New(50*time.Millisecond, 10)
	ticker.Start()
	defer ticker.Stop()

	// Create multiple panicking tasks
	const numPanicTasks = 5
	panicTasks := make([]*panicTickable, numPanicTasks)
	for i := range numPanicTasks {
		panicTasks[i] = newPanicTickable(time.Now().Add(10*time.Millisecond), true, fmt.Sprintf("panic-%d", i))
		ticker.Register(fmt.Sprintf("panic-task-%d", i), panicTasks[i])
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Verify all tasks were called (even though they panicked)
	for i, task := range panicTasks {
		count := task.GetTickCount()
		if count == 0 {
			t.Errorf("Panic task %d was not ticked", i)
		}
	}

	// Ticker should still be running normally
	stats := ticker.Stats()
	if stats.RegisteredCount != numPanicTasks {
		t.Errorf("RegisteredCount = %d, want %d", stats.RegisteredCount, numPanicTasks)
	}
}

// TestRecoverAfterPanic tests whether task can continue running after panic
func TestRecoverAfterPanic(t *testing.T) {
	ticker := New(50*time.Millisecond, 10)
	ticker.Start()
	defer ticker.Stop()

	// Create a task that panics first, then runs normally
	task := newPanicTickable(time.Now().Add(60*time.Millisecond), true, "first panic")
	ticker.Register("recover-task", task)

	// Wait for first tick (will panic) - use longer wait time for CI environment
	time.Sleep(300 * time.Millisecond)

	firstCount := task.GetTickCount()
	if firstCount == 0 {
		t.Error("Task should have been ticked at least once")
	}
	t.Logf("After first wait: task ticked %d times", firstCount)

	// Disable panic
	task.SetShouldPanic(false)

	// Wait for more ticks - use longer wait time
	time.Sleep(400 * time.Millisecond)

	secondCount := task.GetTickCount()
	if secondCount <= firstCount {
		t.Errorf("Task should continue ticking after panic recovery, first=%d, second=%d", firstCount, secondCount)
	}

	t.Logf("Task ticked %d times total (including panic)", secondCount)
}
