package sdq

import (
	"testing"
	"time"
)

func TestNewTopic(t *testing.T) {
	topic := newTopic("test", nil)
	if topic == nil {
		t.Fatal("newTopic returned nil")
	}

	if topic.name != "test" {
		t.Errorf("name = %s, want test", topic.name)
	}

	if topic.ready == nil {
		t.Error("ready heap should be initialized")
	}

	if topic.delayed == nil {
		t.Error("delayed heap should be initialized")
	}

	if topic.reserved == nil {
		t.Error("reserved map should be initialized")
	}

	if topic.buried == nil {
		t.Error("buried heap should be initialized")
	}
}

func TestTopicReadyOperations(t *testing.T) {
	topic := newTopic("test", nil)

	// Push ready jobs with different priorities
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateReady
	meta2 := NewJobMeta(2, "test", 5, 0, 30*time.Second)
	meta2.State = StateReady
	meta3 := NewJobMeta(3, "test", 15, 0, 30*time.Second)
	meta3.State = StateReady

	topic.pushReady(meta1)
	topic.pushReady(meta2)
	topic.pushReady(meta3)

	// Peek should return highest priority (lowest number)
	peeked := topic.peekReady()
	if peeked.ID != 2 {
		t.Errorf("peekReady ID = %d, want 2 (highest priority)", peeked.ID)
	}

	// Pop should return in priority order
	popped := topic.popReady()
	if popped.ID != 2 {
		t.Errorf("popReady ID = %d, want 2", popped.ID)
	}

	popped = topic.popReady()
	if popped.ID != 1 {
		t.Errorf("popReady ID = %d, want 1", popped.ID)
	}

	popped = topic.popReady()
	if popped.ID != 3 {
		t.Errorf("popReady ID = %d, want 3", popped.ID)
	}

	// Pop from empty should return nil
	popped = topic.popReady()
	if popped != nil {
		t.Error("popReady from empty should return nil")
	}

	// Peek from empty should return nil
	peeked = topic.peekReady()
	if peeked != nil {
		t.Error("peekReady from empty should return nil")
	}

	// Test removeReady
	meta4 := NewJobMeta(4, "test", 10, 0, 30*time.Second)
	meta5 := NewJobMeta(5, "test", 20, 0, 30*time.Second)
	topic.pushReady(meta4)
	topic.pushReady(meta5)

	removed := topic.removeReady(4)
	if !removed {
		t.Error("removeReady should return true")
	}

	// Only meta5 should remain
	popped = topic.popReady()
	if popped.ID != 5 {
		t.Errorf("popReady after remove ID = %d, want 5", popped.ID)
	}
}

func TestTopicDelayedOperations(t *testing.T) {
	topic := newTopic("test", nil)
	now := time.Now()

	// Push delayed jobs with different ready times
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateDelayed
	meta1.ReadyAt = now.Add(10 * time.Second)

	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.State = StateDelayed
	meta2.ReadyAt = now.Add(5 * time.Second)

	meta3 := NewJobMeta(3, "test", 10, 0, 30*time.Second)
	meta3.State = StateDelayed
	meta3.ReadyAt = now.Add(15 * time.Second)

	topic.pushDelayed(meta1)
	topic.pushDelayed(meta2)
	topic.pushDelayed(meta3)

	// Peek should return earliest ready time
	peeked := topic.peekDelayed()
	if peeked.ID != 2 {
		t.Errorf("peekDelayed ID = %d, want 2 (earliest)", peeked.ID)
	}

	// Pop should return in time order
	popped := topic.popDelayed()
	if popped.ID != 2 {
		t.Errorf("popDelayed ID = %d, want 2", popped.ID)
	}

	// Remove specific job
	removed := topic.removeDelayed(1)
	if !removed {
		t.Error("removeDelayed should return true")
	}

	popped = topic.popDelayed()
	if popped.ID != 3 {
		t.Errorf("popDelayed after remove ID = %d, want 3", popped.ID)
	}

	// Pop from empty should return nil
	popped = topic.popDelayed()
	if popped != nil {
		t.Error("popDelayed from empty should return nil")
	}
}

func TestTopicReservedOperations(t *testing.T) {
	topic := newTopic("test", nil)

	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateReserved

	// Add reserved
	topic.addReserved(meta1)

	// Get reserved
	got := topic.getReserved(1)
	if got != meta1 {
		t.Error("getReserved should return the meta")
	}

	// Get non-existent
	got = topic.getReserved(999)
	if got != nil {
		t.Error("getReserved non-existent should return nil")
	}

	// Remove reserved
	removed := topic.removeReserved(1)
	if removed != meta1 {
		t.Error("removeReserved should return the meta")
	}

	// Verify removal
	got = topic.getReserved(1)
	if got != nil {
		t.Error("getReserved after remove should return nil")
	}
}

func TestTopicBuriedOperations(t *testing.T) {
	topic := newTopic("test", nil)

	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateBuried
	meta2 := NewJobMeta(2, "test", 5, 0, 30*time.Second)
	meta2.State = StateBuried

	topic.pushBuried(meta1)
	topic.pushBuried(meta2)

	// Peek should return highest priority
	peeked := topic.peekBuried()
	if peeked.ID != 2 {
		t.Errorf("peekBuried ID = %d, want 2", peeked.ID)
	}

	// Pop should return in priority order
	popped := topic.popBuried()
	if popped.ID != 2 {
		t.Errorf("popBuried ID = %d, want 2", popped.ID)
	}

	// Remove specific job
	meta3 := NewJobMeta(3, "test", 15, 0, 30*time.Second)
	topic.pushBuried(meta3)

	removed := topic.removeBuried(1)
	if !removed {
		t.Error("removeBuried should return true")
	}

	popped = topic.popBuried()
	if popped.ID != 3 {
		t.Errorf("popBuried after remove ID = %d, want 3", popped.ID)
	}
}

func TestTopicStats(t *testing.T) {
	topic := newTopic("test", nil)

	// Add jobs to different queues
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateReady
	topic.pushReady(meta1)

	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.State = StateDelayed
	topic.pushDelayed(meta2)

	meta3 := NewJobMeta(3, "test", 10, 0, 30*time.Second)
	meta3.State = StateReserved
	topic.addReserved(meta3)

	meta4 := NewJobMeta(4, "test", 10, 0, 30*time.Second)
	meta4.State = StateBuried
	topic.pushBuried(meta4)

	stats := topic.stats()

	if stats.Name != "test" {
		t.Errorf("Name = %s, want test", stats.Name)
	}

	if stats.ReadyJobs != 1 {
		t.Errorf("ReadyJobs = %d, want 1", stats.ReadyJobs)
	}

	if stats.DelayedJobs != 1 {
		t.Errorf("DelayedJobs = %d, want 1", stats.DelayedJobs)
	}

	if stats.ReservedJobs != 1 {
		t.Errorf("ReservedJobs = %d, want 1", stats.ReservedJobs)
	}

	if stats.BuriedJobs != 1 {
		t.Errorf("BuriedJobs = %d, want 1", stats.BuriedJobs)
	}

	if stats.TotalJobs != 4 {
		t.Errorf("TotalJobs = %d, want 4", stats.TotalJobs)
	}
}

func TestTopicProcessTick(t *testing.T) {
	topic := newTopic("test", nil)
	now := time.Now()

	// Add delayed job that should be ready
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateDelayed
	meta1.ReadyAt = now.Add(-1 * time.Second) // Past
	topic.pushDelayed(meta1)

	// Add reserved job that should timeout
	meta2 := NewJobMeta(2, "test", 10, 0, 1*time.Second)
	meta2.State = StateReserved
	meta2.ReservedAt = now.Add(-2 * time.Second) // 2 seconds ago, TTR is 1s
	topic.addReserved(meta2)

	// Process tick
	topic.ProcessTick(now)

	// meta1 should now be in ready queue
	if topic.delayed.Len() != 0 {
		t.Errorf("delayed queue should be empty, got %d", topic.delayed.Len())
	}

	if topic.ready.Len() != 2 {
		t.Errorf("ready queue should have 2, got %d", topic.ready.Len())
	}

	// meta2 should be moved from reserved to ready
	if len(topic.reserved) != 0 {
		t.Errorf("reserved map should be empty, got %d", len(topic.reserved))
	}
}

func TestTopicNextTickTime(t *testing.T) {
	topic := newTopic("test", nil)
	now := time.Now()

	// Empty topic should return zero time
	nextTime := topic.NextTickTime()
	if !nextTime.IsZero() {
		t.Error("NextTickTime for empty topic should be zero")
	}

	// Add delayed job
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateDelayed
	meta1.ReadyAt = now.Add(10 * time.Second)
	topic.pushDelayed(meta1)

	nextTime = topic.NextTickTime()
	if nextTime.IsZero() {
		t.Error("NextTickTime should not be zero with delayed job")
	}

	// Add reserved job with earlier deadline
	meta2 := NewJobMeta(2, "test", 10, 0, 5*time.Second)
	meta2.State = StateReserved
	meta2.ReservedAt = now
	topic.addReserved(meta2)

	nextTime = topic.NextTickTime()
	// Should return the earlier of delayed.ReadyAt and reserved deadline
	deadline := meta2.ReserveDeadline()
	if nextTime.After(deadline.Add(time.Millisecond)) {
		t.Error("NextTickTime should return the earlier deadline")
	}
}

func TestTopicNeedsTick(t *testing.T) {
	topic := newTopic("test", nil)

	// Empty topic doesn't need tick
	if topic.NeedsTick() {
		t.Error("Empty topic should not need tick")
	}

	// Add ready job - still doesn't need tick
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateReady
	topic.pushReady(meta1)

	if topic.NeedsTick() {
		t.Error("Topic with only ready jobs should not need tick")
	}

	// Add delayed job - needs tick
	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.State = StateDelayed
	topic.pushDelayed(meta2)

	if !topic.NeedsTick() {
		t.Error("Topic with delayed jobs should need tick")
	}

	// Clear delayed, add reserved - still needs tick
	topic.popDelayed()

	meta3 := NewJobMeta(3, "test", 10, 0, 30*time.Second)
	meta3.State = StateReserved
	topic.addReserved(meta3)

	if !topic.NeedsTick() {
		t.Error("Topic with reserved jobs should need tick")
	}
}

func TestTopicProcessDelayed(t *testing.T) {
	topic := newTopic("test", nil)
	now := time.Now()

	// Add multiple delayed jobs, some ready, some not
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateDelayed
	meta1.ReadyAt = now.Add(-1 * time.Second) // Ready

	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.State = StateDelayed
	meta2.ReadyAt = now.Add(10 * time.Second) // Not ready

	meta3 := NewJobMeta(3, "test", 10, 0, 30*time.Second)
	meta3.State = StateDelayed
	meta3.ReadyAt = now.Add(-2 * time.Second) // Ready

	topic.pushDelayed(meta1)
	topic.pushDelayed(meta2)
	topic.pushDelayed(meta3)

	topic.processDelayed(now)

	// 2 should be ready, 1 still delayed
	if topic.ready.Len() != 2 {
		t.Errorf("ready queue should have 2, got %d", topic.ready.Len())
	}

	if topic.delayed.Len() != 1 {
		t.Errorf("delayed queue should have 1, got %d", topic.delayed.Len())
	}
}

func TestTopicProcessReservedTimeout(t *testing.T) {
	topic := newTopic("test", nil)
	now := time.Now()

	// Add reserved jobs, some timeout, some not
	meta1 := NewJobMeta(1, "test", 10, 0, 1*time.Second)
	meta1.State = StateReserved
	meta1.ReservedAt = now.Add(-2 * time.Second) // Timeout

	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.State = StateReserved
	meta2.ReservedAt = now // Not timeout

	meta3 := NewJobMeta(3, "test", 10, 0, 1*time.Second)
	meta3.State = StateReserved
	meta3.ReservedAt = now.Add(-3 * time.Second) // Timeout

	topic.addReserved(meta1)
	topic.addReserved(meta2)
	topic.addReserved(meta3)

	topic.processReservedTimeout(now)

	// 2 should timeout and move to ready, 1 still reserved
	if topic.ready.Len() != 2 {
		t.Errorf("ready queue should have 2, got %d", topic.ready.Len())
	}

	if len(topic.reserved) != 1 {
		t.Errorf("reserved map should have 1, got %d", len(topic.reserved))
	}

	// Timeout jobs should have increased counters
	// Pop and check
	popped := topic.popReady()
	if popped.Timeouts != 1 {
		t.Errorf("Timeouts = %d, want 1", popped.Timeouts)
	}
	if popped.Releases != 1 {
		t.Errorf("Releases = %d, want 1", popped.Releases)
	}
}

func TestJobMetaHeapFIFO(t *testing.T) {
	topic := newTopic("test", nil)

	// Add jobs with same priority
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	meta3 := NewJobMeta(3, "test", 10, 0, 30*time.Second)

	topic.pushReady(meta1)
	topic.pushReady(meta2)
	topic.pushReady(meta3)

	// Should pop in FIFO order (by ID since same priority)
	popped := topic.popReady()
	if popped.ID != 1 {
		t.Errorf("First pop ID = %d, want 1", popped.ID)
	}

	popped = topic.popReady()
	if popped.ID != 2 {
		t.Errorf("Second pop ID = %d, want 2", popped.ID)
	}

	popped = topic.popReady()
	if popped.ID != 3 {
		t.Errorf("Third pop ID = %d, want 3", popped.ID)
	}
}

func TestJobMetaHeapFind(t *testing.T) {
	h := newJobMetaHeap()

	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta2 := NewJobMeta(2, "test", 5, 0, 30*time.Second)

	h.Push(meta1)
	h.Push(meta2)

	// Find existing
	found := h.find(1)
	if found != meta1 {
		t.Error("find should return meta1")
	}

	found = h.find(2)
	if found != meta2 {
		t.Error("find should return meta2")
	}

	// Find non-existent
	found = h.find(999)
	if found != nil {
		t.Error("find non-existent should return nil")
	}
}

func TestDelayedJobHeapFind(t *testing.T) {
	h := newDelayedJobHeap()
	now := time.Now()

	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.ReadyAt = now.Add(10 * time.Second)
	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.ReadyAt = now.Add(5 * time.Second)

	h.Push(meta1)
	h.Push(meta2)

	// Find existing
	found := h.find(1)
	if found != meta1 {
		t.Error("find should return meta1")
	}

	// Find non-existent
	found = h.find(999)
	if found != nil {
		t.Error("find non-existent should return nil")
	}
}
