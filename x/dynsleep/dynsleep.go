// Package dynsleep provides a dynamic sleep ticker implementation.
// It calculates sleep duration dynamically based on the nearest expiration time.
package dynsleep

import (
	"context"
	"math"
	"sync"
	"time"

	"go-slim.dev/sdq"
)

// Compile-time interface check
var _ sdq.Ticker = (*Ticker)(nil)

// cachedTickable caches the next tick time for a Tickable.
type cachedTickable struct {
	mu       sync.RWMutex
	tickable sdq.Tickable
	nextTime time.Time
	dirty    bool // Whether recalculation is needed
}

// markDirty marks the cache as needing recalculation.
func (ct *cachedTickable) markDirty() {
	ct.mu.Lock()
	ct.dirty = true
	ct.mu.Unlock()
}

// getNextTime returns the next tick time, recalculating if dirty.
func (ct *cachedTickable) getNextTime() time.Time {
	ct.mu.RLock()
	dirty := ct.dirty
	nextTime := ct.nextTime
	ct.mu.RUnlock()

	if dirty {
		// Need to recalculate, call tickable outside lock to avoid deadlock
		newNextTime := ct.tickable.NextTickTime()

		ct.mu.Lock()
		ct.nextTime = newNextTime
		ct.dirty = false
		ct.mu.Unlock()

		return newNextTime
	}

	return nextTime
}

// setNextTimeAndMarkDirty updates state after processing a tick.
func (ct *cachedTickable) setNextTimeAndMarkDirty() {
	ct.mu.Lock()
	ct.dirty = true
	ct.mu.Unlock()
}

// Ticker is a dynamic sleep mode ticker.
// It calculates sleep duration dynamically based on the nearest expiration time.
type Ticker struct {
	mu           sync.RWMutex
	registry     map[string]*cachedTickable // Registered objects with cache
	minInterval  time.Duration              // Minimum tick interval
	idleInterval time.Duration              // Idle tick interval

	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	wakeupChan chan struct{} // Wakeup channel
}

// Name returns the ticker name.
func (w *Ticker) Name() string {
	return "dynsleep"
}

// New creates a new dynamic sleep ticker.
func New(minInterval, idleInterval time.Duration) *Ticker {
	if minInterval == 0 {
		minInterval = 10 * time.Millisecond
	}
	if idleInterval == 0 {
		idleInterval = 1 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Ticker{
		registry:     make(map[string]*cachedTickable),
		minInterval:  minInterval,
		idleInterval: idleInterval,
		ctx:          ctx,
		cancel:       cancel,
		wakeupChan:   make(chan struct{}, 1),
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
	w.wg.Wait()
}

// Register registers an object.
func (w *Ticker) Register(name string, tickable sdq.Tickable) {
	w.mu.Lock()
	_, exists := w.registry[name]
	w.mu.Unlock()

	if !exists {
		// Call NextTickTime outside lock to avoid deadlock
		nextTime := tickable.NextTickTime()

		w.mu.Lock()
		// Check again to prevent concurrent registration
		if _, exists := w.registry[name]; !exists {
			w.registry[name] = &cachedTickable{
				tickable: tickable,
				nextTime: nextTime,
				dirty:    false,
			}
		}
		w.mu.Unlock()

		// Wakeup outside lock to avoid lock reentry
		w.wakeup()
	}
}

// Unregister unregisters an object.
func (w *Ticker) Unregister(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.registry, name)
}

// Wakeup wakes up the ticker (public method).
func (w *Ticker) Wakeup() {
	// Mark all caches as dirty to force recalculation
	w.mu.RLock()
	cached := make([]*cachedTickable, 0, len(w.registry))
	for _, ct := range w.registry {
		cached = append(cached, ct)
	}
	w.mu.RUnlock()

	for _, ct := range cached {
		ct.markDirty()
	}

	w.wakeup()
}

// wakeup is the internal wakeup method.
func (w *Ticker) wakeup() {
	select {
	case w.wakeupChan <- struct{}{}:
	default:
	}
}

// Stats returns ticker statistics.
func (w *Ticker) Stats() *sdq.TickerStats {
	// Calculate next time first (internally acquires lock)
	nextTime, hasNext := w.calculateNextTime()

	w.mu.RLock()
	defer w.mu.RUnlock()

	stats := &sdq.TickerStats{
		Name:            w.Name(),
		RegisteredCount: len(w.registry),
	}

	if hasNext {
		stats.NextTickTime = nextTime
		now := time.Now()
		if nextTime.After(now) {
			stats.TimeUntilTick = nextTime.Sub(now)
		}
	} else {
		stats.TimeUntilTick = time.Duration(math.MaxInt64)
	}

	return stats
}

// run is the main loop.
func (w *Ticker) run() {
	defer w.wg.Done()

	for {
		// Calculate next expiration time
		nextTime, hasNext := w.calculateNextTime()

		var sleepDuration time.Duration
		if hasNext {
			now := time.Now()
			if nextTime.After(now) {
				// Limit minimum interval
				sleepDuration = max(nextTime.Sub(now), w.minInterval)
			} else {
				// Already expired, process immediately
				sleepDuration = 0
			}
		} else {
			// No tasks, use idle interval
			sleepDuration = w.idleInterval
		}

		// Sleep or wait for wakeup
		if sleepDuration == 0 {
			// Process immediately, but check for exit first
			select {
			case <-w.ctx.Done():
				return
			default:
				w.processTick()
			}
		} else {
			select {
			case <-w.ctx.Done():
				return
			case <-w.wakeupChan:
				// Woken up, recalculate
				continue
			case <-time.After(sleepDuration):
				// Expired, process tick
				w.processTick()
			}
		}
	}
}

// calculateNextTime calculates the next nearest expiration time (using cache).
func (w *Ticker) calculateNextTime() (time.Time, bool) {
	w.mu.RLock()
	cached := make([]*cachedTickable, 0, len(w.registry))
	for _, ct := range w.registry {
		cached = append(cached, ct)
	}
	w.mu.RUnlock()

	var nextTime time.Time
	hasNext := false

	for _, ct := range cached {
		t := ct.getNextTime()
		if t.IsZero() {
			continue
		}

		if !hasNext || t.Before(nextTime) {
			nextTime = t
			hasNext = true
		}
	}

	return nextTime, hasNext
}

// processTick processes a tick, notifying all registered objects.
func (w *Ticker) processTick() {
	now := time.Now()

	w.mu.RLock()
	// Copy the list to avoid holding the lock for too long
	cached := make([]*cachedTickable, 0, len(w.registry))
	for _, ct := range w.registry {
		cached = append(cached, ct)
	}
	w.mu.RUnlock()

	// Notify each object (only process expired ones)
	for _, ct := range cached {
		nextTime := ct.getNextTime()

		// Check if tick is needed
		if nextTime.IsZero() || now.Before(nextTime) {
			continue
		}

		// Panic recovery protection
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Log panic but don't affect other task processing
					// TODO: Add logging
					_ = r
				}
			}()
			ct.tickable.ProcessTick(now)
		}()

		// Mark as dirty, recalculation needed next time
		ct.setNextTimeAndMarkDirty()
	}
}
