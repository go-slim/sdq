package dynsleep

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
	ticker := New(10*time.Millisecond, 1*time.Second)
	if ticker == nil {
		t.Fatal("New returned nil")
	}

	if ticker.minInterval != 10*time.Millisecond {
		t.Errorf("minInterval = %v, want %v", ticker.minInterval, 10*time.Millisecond)
	}

	if ticker.idleInterval != 1*time.Second {
		t.Errorf("idleInterval = %v, want %v", ticker.idleInterval, 1*time.Second)
	}
}

func TestDefaults(t *testing.T) {
	ticker := New(0, 0)

	if ticker.minInterval != 10*time.Millisecond {
		t.Errorf("default minInterval = %v, want %v", ticker.minInterval, 10*time.Millisecond)
	}

	if ticker.idleInterval != 1*time.Second {
		t.Errorf("default idleInterval = %v, want %v", ticker.idleInterval, 1*time.Second)
	}
}

func TestStartStop(t *testing.T) {
	ticker := New(10*time.Millisecond, 1*time.Second)

	ticker.Start()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	ticker.Stop()
}

func TestStatsEmpty(t *testing.T) {
	ticker := New(10*time.Millisecond, 1*time.Second)

	stats := ticker.Stats()

	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount = %d, want 0", stats.RegisteredCount)
	}

	if stats.Name != "dynsleep" {
		t.Errorf("Name = %s, want dynsleep", stats.Name)
	}
}

func TestNoObjects(t *testing.T) {
	ticker := New(10*time.Millisecond, 50*time.Millisecond)

	ticker.Start()
	defer ticker.Stop()

	// Should not panic with no registered objects
	time.Sleep(100 * time.Millisecond)

	stats := ticker.Stats()
	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount = %d, want 0", stats.RegisteredCount)
	}
}

// TestRegisterAndTick tests registration and tick processing
func TestRegisterAndTick(t *testing.T) {
	ticker := New(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// Register an object that needs immediate tick
	mock := newMockTickable(time.Now().Add(-1 * time.Second)) // Past time, should tick immediately
	ticker.Register("test", mock)

	// Wait for tick processing
	time.Sleep(100 * time.Millisecond)

	// Verify tick was called
	tickCount := mock.GetTickCount()
	if tickCount == 0 {
		t.Error("ProcessTick should have been called")
	}

	// Verify registration stats
	stats := ticker.Stats()
	if stats.RegisteredCount != 1 {
		t.Errorf("RegisteredCount = %d, want 1", stats.RegisteredCount)
	}

	// Unregister
	ticker.Unregister("test")

	stats = ticker.Stats()
	if stats.RegisteredCount != 0 {
		t.Errorf("RegisteredCount after unregister = %d, want 0", stats.RegisteredCount)
	}
}

// TestMultipleObjects tests multiple object scheduling
func TestMultipleObjects(t *testing.T) {
	ticker := New(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// Register multiple objects that need tick
	mocks := make([]*mockTickable, 5)
	for i := range mocks {
		mocks[i] = newMockTickable(time.Now().Add(-1 * time.Second))
		ticker.Register(string(rune('a'+i)), mocks[i])
	}

	// Wait for tick processing
	time.Sleep(100 * time.Millisecond)

	// Verify all objects were ticked
	for i, mock := range mocks {
		tickCount := mock.GetTickCount()
		if tickCount == 0 {
			t.Errorf("Mock %d was not ticked", i)
		}
	}

	// Verify stats
	stats := ticker.Stats()
	if stats.RegisteredCount != 5 {
		t.Errorf("RegisteredCount = %d, want 5", stats.RegisteredCount)
	}
}

// TestWakeup tests Wakeup functionality
func TestWakeup(t *testing.T) {
	ticker := New(10*time.Millisecond, 5*time.Second) // Long idle interval
	ticker.Start()
	defer ticker.Stop()

	// Register an object that needs tick
	mock := newMockTickable(time.Now().Add(-1 * time.Second))
	ticker.Register("test", mock)

	// Call Wakeup immediately
	ticker.Wakeup()

	// Wait a short time
	time.Sleep(50 * time.Millisecond)

	// Verify tick was called (even with long idle interval)
	tickCount := mock.GetTickCount()
	if tickCount == 0 {
		t.Error("ProcessTick should have been called after Wakeup")
	}
}

// TestDynamicScheduling tests dynamic scheduling
func TestDynamicScheduling(t *testing.T) {
	ticker := New(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// Register an object that needs future tick
	nextTick := time.Now().Add(50 * time.Millisecond)
	mock := newMockTickable(nextTick)
	ticker.Register("test", mock)

	// Should not tick before nextTick
	time.Sleep(20 * time.Millisecond)
	if mock.GetTickCount() > 0 {
		t.Error("Should not tick before NextTickTime")
	}

	// Wait until after nextTick
	time.Sleep(50 * time.Millisecond)

	// Should have ticked by now
	tickCount := mock.GetTickCount()
	if tickCount == 0 {
		t.Error("Should tick after NextTickTime")
	}
}

// TestNoTickNeeded tests objects that don't need tick
func TestNoTickNeeded(t *testing.T) {
	ticker := New(10*time.Millisecond, 100*time.Millisecond)
	ticker.Start()
	defer ticker.Stop()

	// Register an object that doesn't need tick
	mock := newMockTickable(time.Time{}) // Zero value means no tick needed
	ticker.Register("test", mock)

	// Wait a while
	time.Sleep(150 * time.Millisecond)

	// Should not have been ticked
	tickCount := mock.GetTickCount()
	if tickCount > 0 {
		t.Errorf("Should not tick when NeedsTick is false, got %d ticks", tickCount)
	}
}

// TestUpdateNextTickTime tests dynamic update of next tick time
func TestUpdateNextTickTime(t *testing.T) {
	ticker := New(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// Register an object with far future tick
	mock := newMockTickable(time.Now().Add(10 * time.Second))
	ticker.Register("test", mock)

	// Wait a short time
	time.Sleep(50 * time.Millisecond)

	// Should not have been ticked
	if mock.GetTickCount() > 0 {
		t.Error("Should not tick yet")
	}

	// Update to need immediate tick
	mock.SetNextTickTime(time.Now().Add(-1 * time.Second))
	ticker.Wakeup() // Wakeup ticker

	// Wait for tick processing
	time.Sleep(50 * time.Millisecond)

	// Should have been ticked now
	tickCount := mock.GetTickCount()
	if tickCount == 0 {
		t.Error("Should tick after updating NextTickTime")
	}
}

// TestConcurrentRegisterUnregister tests high-frequency Register/Unregister operations
func TestConcurrentRegisterUnregister(t *testing.T) {
	ticker := New(10*time.Millisecond, 1*time.Second)
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
	ticker := New(10*time.Millisecond, 1*time.Second)

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
	ticker := New(10*time.Millisecond, 1*time.Second)
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
	ticker := New(50*time.Millisecond, 100*time.Millisecond)
	ticker.Start()
	defer ticker.Stop()

	// Create a tickable that panics and a normal one
	panicObj := newPanicTickable(time.Now().Add(60*time.Millisecond), true, "simulated panic")
	normalObj := newPanicTickable(time.Now().Add(60*time.Millisecond), false, "")

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
}

// TestMultiplePanics tests multiple tasks panicking simultaneously
func TestMultiplePanics(t *testing.T) {
	ticker := New(50*time.Millisecond, 100*time.Millisecond)
	ticker.Start()
	defer ticker.Stop()

	// Create multiple panicking tasks
	numPanicTasks := 5
	panicTasks := make([]*panicTickable, numPanicTasks)
	for i := range numPanicTasks {
		panicTasks[i] = newPanicTickable(time.Now().Add(60*time.Millisecond), true, fmt.Sprintf("panic %d", i))
		ticker.Register(fmt.Sprintf("panic-task-%d", i), panicTasks[i])
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Verify all tasks were called
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
	ticker := New(50*time.Millisecond, 100*time.Millisecond)
	ticker.Start()
	defer ticker.Stop()

	// Create a task that panics first, then runs normally
	task := newPanicTickable(time.Now().Add(60*time.Millisecond), true, "first panic")
	ticker.Register("recover-task", task)

	// Wait for first tick (will panic)
	time.Sleep(150 * time.Millisecond)

	firstCount := task.GetTickCount()
	if firstCount == 0 {
		t.Error("Task should have been ticked at least once")
	}
	t.Logf("After first wait: task ticked %d times", firstCount)

	// Disable panic
	task.SetShouldPanic(false)

	// Wait for more ticks
	time.Sleep(200 * time.Millisecond)

	secondCount := task.GetTickCount()
	if secondCount <= firstCount {
		t.Errorf("Task should continue ticking after panic recovery, first=%d, second=%d", firstCount, secondCount)
	}

	t.Logf("Task ticked %d times total (including panic)", secondCount)
}
