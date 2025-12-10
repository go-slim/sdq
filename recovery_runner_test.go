package sdq

import (
	"context"
	"testing"
	"time"
)

func TestNewRecoveryManager(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		if rm == nil {
			t.Fatal("newRecoveryRunner returned nil")
			return
		}

		if rm.storage != testStorage.Storage {
			t.Error("storage should be set")
		}
	})
}

func TestRecoveryManagerRecoverEmpty(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)

		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.TotalJobs != 0 {
			t.Errorf("TotalJobs = %d, want 0", result.TotalJobs)
		}

		if result.MaxID != 0 {
			t.Errorf("MaxID = %d, want 0", result.MaxID)
		}

		if len(result.TopicJobs) != 0 {
			t.Errorf("TopicJobs = %v, want empty", result.TopicJobs)
		}
	})
}

// TestRecoveryManagerRecoverNilStorage 已删除
// 现在 Storage 是必需的，不再支持 nil storage

func TestRecoveryManagerRecoverReadyJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save ready jobs
		meta1 := NewJobMeta(1, "topic1", 10, 0, 30*time.Second)
		meta1.State = StateReady
		_ = testStorage.Storage.SaveJob(ctx, meta1, []byte("body1"))

		meta2 := NewJobMeta(2, "topic1", 5, 0, 30*time.Second)
		meta2.State = StateReady
		_ = testStorage.Storage.SaveJob(ctx, meta2, []byte("body2"))

		meta3 := NewJobMeta(3, "topic2", 10, 0, 30*time.Second)
		meta3.State = StateReady
		_ = testStorage.Storage.SaveJob(ctx, meta3, []byte("body3"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.TotalJobs != 3 {
			t.Errorf("TotalJobs = %d, want 3", result.TotalJobs)
		}

		if result.MaxID != 3 {
			t.Errorf("MaxID = %d, want 3", result.MaxID)
		}

		if len(result.TopicJobs) != 2 {
			t.Errorf("TopicJobs count = %d, want 2", len(result.TopicJobs))
		}

		if len(result.TopicJobs["topic1"]) != 2 {
			t.Errorf("topic1 jobs = %d, want 2", len(result.TopicJobs["topic1"]))
		}

		if len(result.TopicJobs["topic2"]) != 1 {
			t.Errorf("topic2 jobs = %d, want 1", len(result.TopicJobs["topic2"]))
		}
	})
}

func TestRecoveryManagerRecoverDelayedJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save delayed job
		meta := NewJobMeta(1, "test", 10, 5*time.Second, 30*time.Second)
		meta.State = StateDelayed
		meta.ReadyAt = time.Now().Add(5 * time.Second)
		_ = testStorage.Storage.SaveJob(ctx, meta, []byte("body"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.TotalJobs != 1 {
			t.Errorf("TotalJobs = %d, want 1", result.TotalJobs)
		}

		jobs := result.TopicJobs["test"]
		if len(jobs) != 1 {
			t.Fatalf("test jobs = %d, want 1", len(jobs))
		}

		if jobs[0].State != StateDelayed {
			t.Errorf("State = %v, want StateDelayed", jobs[0].State)
		}
	})
}

func TestRecoveryManagerRecoverBuriedJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save buried job
		meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.State = StateBuried
		meta.BuriedAt = time.Now()
		_ = testStorage.Storage.SaveJob(ctx, meta, []byte("body"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		jobs := result.TopicJobs["test"]
		if len(jobs) != 1 {
			t.Fatalf("test jobs = %d, want 1", len(jobs))
		}

		if jobs[0].State != StateBuried {
			t.Errorf("State = %v, want StateBuried", jobs[0].State)
		}
	})
}

func TestRecoveryManagerRecoverEnqueuedJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save enqueued job (no delay)
		meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		meta1.State = StateEnqueued
		_ = testStorage.Storage.SaveJob(ctx, meta1, []byte("body1"))

		// Save enqueued job (with delay)
		meta2 := NewJobMeta(2, "test", 10, 5*time.Second, 30*time.Second)
		meta2.State = StateEnqueued
		meta2.Delay = 5 * time.Second
		_ = testStorage.Storage.SaveJob(ctx, meta2, []byte("body2"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		jobs := result.TopicJobs["test"]
		if len(jobs) != 2 {
			t.Fatalf("test jobs = %d, want 2", len(jobs))
		}

		// Enqueued without delay should become Ready
		var readyJob, delayedJob *JobMeta
		for _, job := range jobs {
			if job.ID == 1 {
				readyJob = job
			} else {
				delayedJob = job
			}
		}

		if readyJob == nil || readyJob.State != StateReady {
			t.Errorf("Job 1 State = %v, want StateReady", readyJob.State)
		}

		// Enqueued with delay should become Delayed
		if delayedJob == nil || delayedJob.State != StateDelayed {
			t.Errorf("Job 2 State = %v, want StateDelayed", delayedJob.State)
		}
	})
}

func TestRecoveryManagerRecoverReservedJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save reserved job (simulating crash during processing)
		meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.State = StateReserved
		meta.ReservedAt = time.Now().Add(-1 * time.Minute)
		meta.Reserves = 1
		_ = testStorage.Storage.SaveJob(ctx, meta, []byte("body"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		jobs := result.TopicJobs["test"]
		if len(jobs) != 1 {
			t.Fatalf("test jobs = %d, want 1", len(jobs))
		}

		// Reserved jobs should become Ready (for re-processing)
		if jobs[0].State != StateReady {
			t.Errorf("State = %v, want StateReady", jobs[0].State)
		}

		// Timeouts should be incremented
		if jobs[0].Timeouts != 1 {
			t.Errorf("Timeouts = %d, want 1", jobs[0].Timeouts)
		}

		// ReservedAt should be cleared
		if !jobs[0].ReservedAt.IsZero() {
			t.Error("ReservedAt should be zero")
		}
	})
}

func TestRecoveryManagerMaxID(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save jobs with various IDs
		for _, id := range []uint64{5, 100, 50, 1, 75} {
			meta := NewJobMeta(id, "test", 10, 0, 30*time.Second)
			meta.State = StateReady
			_ = testStorage.Storage.SaveJob(ctx, meta, []byte("body"))
		}

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.MaxID != 100 {
			t.Errorf("MaxID = %d, want 100", result.MaxID)
		}
	})
}

func TestRecoveryManagerFailedJobs(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		// Save job with unknown state (should be skipped)
		meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.State = State(99) // Unknown state
		_ = testStorage.Storage.SaveJob(ctx, meta, []byte("body"))

		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		if result.TotalJobs != 1 {
			t.Errorf("TotalJobs = %d, want 1", result.TotalJobs)
		}

		if result.FailedJobs != 1 {
			t.Errorf("FailedJobs = %d, want 1", result.FailedJobs)
		}

		// Unknown state jobs should not be in TopicJobs
		if len(result.TopicJobs["test"]) != 0 {
			t.Errorf("TopicJobs should not contain failed jobs")
		}
	})
}

func TestRecoveryManagerPreprocessJobMeta(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)

		tests := []struct {
			name        string
			inputState  State
			inputDelay  time.Duration
			wantState   State
			shouldBeNil bool
		}{
			{"Ready stays Ready", StateReady, 0, StateReady, false},
			{"Delayed stays Delayed", StateDelayed, 5 * time.Second, StateDelayed, false},
			{"Buried stays Buried", StateBuried, 0, StateBuried, false},
			{"Enqueued no delay becomes Ready", StateEnqueued, 0, StateReady, false},
			{"Enqueued with delay becomes Delayed", StateEnqueued, 5 * time.Second, StateDelayed, false},
			{"Reserved becomes Ready", StateReserved, 0, StateReady, false},
			{"Unknown state returns nil", State(99), 0, 0, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				meta := NewJobMeta(1, "test", 10, tt.inputDelay, 30*time.Second)
				meta.State = tt.inputState
				if tt.inputState == StateReserved {
					meta.ReservedAt = time.Now()
				}

				result := rm.preprocessJobMeta(meta)

				if tt.shouldBeNil {
					if result != nil {
						t.Errorf("preprocessJobMeta should return nil for unknown state")
					}
				} else {
					if result == nil {
						t.Errorf("preprocessJobMeta should not return nil")
					} else if result.State != tt.wantState {
						t.Errorf("State = %v, want %v", result.State, tt.wantState)
					}
				}
			})
		}
	})
}

func TestRecoveryManagerHandleEnqueuedJob(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)

		// Without delay
		meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		meta1.State = StateEnqueued
		result := rm.handleEnqueuedJob(meta1)

		if result.State != StateReady {
			t.Errorf("State = %v, want StateReady", result.State)
		}

		// With delay
		meta2 := NewJobMeta(2, "test", 10, 5*time.Second, 30*time.Second)
		meta2.State = StateEnqueued
		result = rm.handleEnqueuedJob(meta2)

		if result.State != StateDelayed {
			t.Errorf("State = %v, want StateDelayed", result.State)
		}
	})
}

func TestRecoveryManagerHandleReservedJob(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		ctx := context.Background()

		rm := newRecoveryRunner(ctx, testStorage.Storage)

		meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.State = StateReserved
		meta.ReservedAt = time.Now()
		meta.Timeouts = 0

		result := rm.handleReservedJob(meta)

		if result.State != StateReady {
			t.Errorf("State = %v, want StateReady", result.State)
		}

		if !result.ReservedAt.IsZero() {
			t.Error("ReservedAt should be cleared")
		}

		if result.Timeouts != 1 {
			t.Errorf("Timeouts = %d, want 1", result.Timeouts)
		}

		if result.ReadyAt.IsZero() {
			t.Error("ReadyAt should be set")
		}
	})
}

func TestRecoveryResultApply(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		// Test that RecoveryResult can be applied to TopicHub
		ctx := context.Background()

		// Create jobs in storage
		meta1 := NewJobMeta(1, "topic1", 10, 0, 30*time.Second)
		meta1.State = StateReady
		_ = testStorage.Storage.SaveJob(ctx, meta1, []byte("body1"))

		meta2 := NewJobMeta(2, "topic1", 5, 0, 30*time.Second)
		meta2.State = StateDelayed
		meta2.ReadyAt = time.Now().Add(10 * time.Second)
		_ = testStorage.Storage.SaveJob(ctx, meta2, []byte("body2"))

		meta3 := NewJobMeta(3, "topic2", 10, 0, 30*time.Second)
		meta3.State = StateBuried
		_ = testStorage.Storage.SaveJob(ctx, meta3, []byte("body3"))

		// Recover
		rm := newRecoveryRunner(ctx, testStorage.Storage)
		result, err := rm.Recover()
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}

		// Apply to TopicHub
		config := DefaultConfig()
		ticker := NewNoOpTicker() // 使用 NoOpTicker 避免死锁
		hub := newTopicHub(&config, testStorage.Storage, ticker, nil)

		err = hub.ApplyRecovery(result)
		if err != nil {
			t.Fatalf("ApplyRecovery error: %v", err)
		}

		// Verify
		topics := hub.ListTopics()
		if len(topics) != 2 {
			t.Errorf("topics count = %d, want 2", len(topics))
		}

		stats1 := hub.TopicStats("topic1")
		if stats1 == nil {
			t.Fatal("topic1 stats should not be nil")
			return
		}

		// Note: one job is delayed, so it went to delayed queue
		if stats1.ReadyJobs != 1 {
			t.Errorf("topic1 ReadyJobs = %d, want 1", stats1.ReadyJobs)
		}

		stats2 := hub.TopicStats("topic2")
		if stats2 == nil {
			t.Fatal("topic2 stats should not be nil")
			return
		}

		if stats2.BuriedJobs != 1 {
			t.Errorf("topic2 BuriedJobs = %d, want 1", stats2.BuriedJobs)
		}
	})
}
