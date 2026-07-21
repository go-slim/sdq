// Package timewheel provides a time wheel ticker implementation.
// It uses fixed interval ticks for higher time precision.
//
// ProcessTick panics are recovered so one Tickable cannot stop the scheduler. The ticker has no logging
// dependency and does not report the recovered value; Tickable implementations that need diagnostics
// should recover and report their own panic.
package timewheel

import (
	"context"
	"sync"
	"time"

	"go-slim.dev/sdq"
)

// Compile-time interface check
var _ sdq.Ticker = (*Ticker)(nil)

// tickTask represents a tick task.
type tickTask struct {
	name     string
	tickable sdq.Tickable
	tickTime time.Time
	slot     int
}

// Ticker is a time wheel mode ticker.
// It uses fixed interval ticks for higher time precision.
type Ticker struct {
	mu           sync.RWMutex
	registry     map[string]sdq.Tickable // Registered objects
	scheduled    map[string]*tickTask    // Authoritative wheel entry for each name
	tickInterval time.Duration           // Fixed tick interval
	slots        int                     // Number of time slots

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	currentSlot int           // Current time slot
	wheel       [][]*tickTask // Time wheel: slot -> tasks

	// Pause control
	pauseChan  chan struct{} // Pause signal
	resumeChan chan struct{} // Resume signal
	paused     bool          // Whether paused
}

// New creates a new time wheel ticker.
// tickInterval: interval for each tick (e.g., 10ms)
// slots: number of time slots (e.g., 3600, can cover 36 seconds)
func New(tickInterval time.Duration, slots int) *Ticker {
	if tickInterval == 0 {
		tickInterval = 10 * time.Millisecond
	}
	if slots == 0 {
		slots = 3600 // Default 3600 slots
	}

	ctx, cancel := context.WithCancel(context.Background())

	wheel := make([][]*tickTask, slots)
	for i := range wheel {
		wheel[i] = make([]*tickTask, 0)
	}

	return &Ticker{
		registry:     make(map[string]sdq.Tickable),
		scheduled:    make(map[string]*tickTask),
		tickInterval: tickInterval,
		slots:        slots,
		ctx:          ctx,
		cancel:       cancel,
		currentSlot:  0,
		wheel:        wheel,
		pauseChan:    make(chan struct{}, 1),
		resumeChan:   make(chan struct{}, 1),
		paused:       true, // Initially paused
	}
}

// Start starts the ticker.
func (w *Ticker) Start() {
	w.wg.Add(1)
	go w.run()
}

// Stop stops the ticker.
func (w *Ticker) Stop() {
	w.cancel()

	// If paused, send resume signal to ensure goroutine can exit
	w.mu.Lock()
	if w.paused {
		select {
		case w.resumeChan <- struct{}{}:
		default:
		}
	}
	w.mu.Unlock()

	w.wg.Wait()
}

// Register registers an object. Registering the same name removes the previous
// wheel entry before scheduling the replacement.
func (w *Ticker) Register(name string, tickable sdq.Tickable) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.removeTask(name)
	w.registry[name] = tickable
	w.scheduleTask(name, tickable)

	// Only resume when registration actually scheduled a task.
	if w.paused && w.hasAnyTask() {
		w.paused = false
		select {
		case w.resumeChan <- struct{}{}:
		default:
		}
	}
}

// Unregister unregisters an object.
func (w *Ticker) Unregister(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.registry, name)
	// Remove task from time wheel
	w.removeTask(name)

	// Check if should pause
	w.checkAndPause()
}

// Wakeup rebuilds the wheel from every registered object's current deadline.
func (w *Ticker) Wakeup() {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Rebuild the wheel so changed NextTickTime values take effect immediately.
	w.wheel = make([][]*tickTask, w.slots)
	for i := range w.wheel {
		w.wheel[i] = make([]*tickTask, 0)
	}
	clear(w.scheduled)
	for name, tickable := range w.registry {
		w.scheduleTask(name, tickable)
	}

	// If previously paused, resume now
	if w.paused && w.hasAnyTask() {
		w.paused = false
		select {
		case w.resumeChan <- struct{}{}:
		default:
		}
	}
}

// Name returns the ticker name.
func (w *Ticker) Name() string {
	return "timewheel"
}

// Stats returns ticker statistics.
func (w *Ticker) Stats() *sdq.TickerStats {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return &sdq.TickerStats{
		Name:            w.Name(),
		RegisteredCount: len(w.registry),
		NextTickTime:    time.Now().Add(w.tickInterval),
		TimeUntilTick:   w.tickInterval,
	}
}

// run is the main loop.
func (w *Ticker) run() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.tickInterval)
	defer ticker.Stop()

	for {
		// Check if paused
		w.mu.RLock()
		isPaused := w.paused
		w.mu.RUnlock()

		if isPaused {
			// Paused state, wait for resume or exit
			select {
			case <-w.ctx.Done():
				return
			case <-w.resumeChan:
				// Resume running
				continue
			}
		}

		// Normal running, wait for tick
		select {
		case <-w.ctx.Done():
			return
		case now := <-ticker.C:
			w.processTick(now)
		}
	}
}

// processTick processes tasks in the current slot.
func (w *Ticker) processTick(now time.Time) {
	w.mu.Lock()

	// Get tasks in current slot
	tasks := w.wheel[w.currentSlot]
	w.wheel[w.currentSlot] = make([]*tickTask, 0)
	for _, task := range tasks {
		if w.scheduled[task.name] == task {
			task.slot = -1
		}
	}

	// Move to next slot
	w.currentSlot = (w.currentSlot + 1) % w.slots

	w.mu.Unlock()

	// Process tasks
	for _, task := range tasks {
		// A concurrent Register, Unregister, or Wakeup can supersede an entry after it
		// was removed from the slot. Only the map's current identity may run or reschedule.
		w.mu.RLock()
		current := w.scheduled[task.name] == task
		w.mu.RUnlock()
		if !current {
			continue
		}

		// Check if really expired
		if !task.tickTime.After(now) {
			// Panic recovery protection
			func() {
				defer func() {
					_ = recover()
				}()
				task.tickable.ProcessTick(now)
			}()

			// Reschedule
			w.mu.Lock()
			if w.scheduled[task.name] == task {
				w.scheduleTask(task.name, w.registry[task.name])
			}
			w.mu.Unlock()
		} else {
			// Not yet expired, re-add to slot
			w.mu.Lock()
			if w.scheduled[task.name] == task {
				slot := w.calculateSlot(task.tickTime)
				task.slot = slot
				w.wheel[slot] = append(w.wheel[slot], task)
			}
			w.mu.Unlock()
		}
	}

	// Check if should pause (all slots are empty)
	w.mu.Lock()
	if !w.hasAnyTask() {
		w.paused = true
		select {
		case w.pauseChan <- struct{}{}:
		default:
		}
	}
	w.mu.Unlock()
}

// scheduleTask schedules a task to the time wheel (requires holding the lock).
func (w *Ticker) scheduleTask(name string, tickable sdq.Tickable) {
	nextTime := tickable.NextTickTime()
	if nextTime.IsZero() {
		delete(w.scheduled, name)
		return
	}

	slot := w.calculateSlot(nextTime)
	task := &tickTask{
		name:     name,
		tickable: tickable,
		tickTime: nextTime,
		slot:     slot,
	}
	w.scheduled[name] = task
	w.wheel[slot] = append(w.wheel[slot], task)
}

// removeTask removes a task from the time wheel (requires holding the lock).
func (w *Ticker) removeTask(name string) {
	task := w.scheduled[name]
	delete(w.scheduled, name)
	if task == nil || task.slot < 0 || task.slot >= len(w.wheel) {
		return
	}

	tasks := w.wheel[task.slot]
	for i, candidate := range tasks {
		if candidate != task {
			continue
		}
		copy(tasks[i:], tasks[i+1:])
		tasks[len(tasks)-1] = nil
		w.wheel[task.slot] = tasks[:len(tasks)-1]
		return
	}
}

// hasAnyTask checks if the time wheel has any tasks (requires holding the lock).
func (w *Ticker) hasAnyTask() bool {
	return len(w.scheduled) > 0
}

// checkAndPause checks if should pause (requires holding the lock).
func (w *Ticker) checkAndPause() {
	if !w.hasAnyTask() && !w.paused {
		w.paused = true
		select {
		case w.pauseChan <- struct{}{}:
		default:
		}
	}
}

// calculateSlot calculates which slot a task should be placed in.
func (w *Ticker) calculateSlot(tickTime time.Time) int {
	now := time.Now()
	duration := tickTime.Sub(now)

	if duration <= 0 {
		// Already expired, place in current slot
		return w.currentSlot
	}

	// Calculate how many ticks are needed
	ticks := int(duration / w.tickInterval)
	if duration%w.tickInterval > 0 {
		ticks++
	}

	// Calculate slot (circular)
	slot := (w.currentSlot + ticks) % w.slots

	return slot
}
