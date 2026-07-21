package sdq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type scanErrorStorage struct {
	Storage
	err error
}

type updateErrorStorage struct {
	Storage
	err error
}

func (storage *updateErrorStorage) UpdateJobMeta(context.Context, *JobMeta) error {
	return storage.err
}

func (storage *scanErrorStorage) ScanJobMeta(
	context.Context,
	*JobMetaFilter,
) (*JobMetaList, error) {
	return nil, storage.err
}

type gatedScanStorage struct {
	Storage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type nonAdvancingCursorStorage struct {
	Storage
}

func (storage *nonAdvancingCursorStorage) ScanJobMeta(
	ctx context.Context,
	filter *JobMetaFilter,
) (*JobMetaList, error) {
	result, err := storage.Storage.ScanJobMeta(ctx, filter)
	if err != nil {
		return nil, err
	}
	result.HasMore = true
	result.NextCursor = filter.Cursor
	return result, nil
}

func (storage *gatedScanStorage) ScanJobMeta(
	ctx context.Context,
	filter *JobMetaFilter,
) (*JobMetaList, error) {
	storage.once.Do(func() { close(storage.entered) })
	select {
	case <-storage.release:
		return storage.Storage.ScanJobMeta(ctx, filter)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestRecoveryManagerProcessesPagesIncrementally(t *testing.T) {
	const jobCount = 2505

	storage := newMemoryStorage()
	ctx := context.Background()
	for id := uint64(1); id <= jobCount; id++ {
		meta := NewJobMeta(id, "recover", 10, 0, time.Minute)
		meta.State = StateReady
		if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
			t.Fatalf("save job %d: %v", id, err)
		}
	}

	manager := newRecoveryManager(ctx, storage)
	seen := make(map[uint64]struct{}, jobCount)
	batchCount := 0
	maxBatchSize := 0
	var completed *RecoveryProgress

	manager.Recover(jobCount, func(progress *RecoveryProgress) {
		if progress.Phase == RecoveryPhaseError {
			t.Fatalf("recover: %v", progress.Error)
		}
		if progress.Phase == RecoveryPhaseComplete {
			completed = progress
		}
	}, func(batch *RecoveryResult) error {
		batchCount++
		batchSize := 0
		for _, jobs := range batch.TopicJobs {
			batchSize += len(jobs)
			for _, meta := range jobs {
				if _, exists := seen[meta.ID]; exists {
					t.Fatalf("job %d recovered more than once", meta.ID)
				}
				seen[meta.ID] = struct{}{}
			}
		}
		maxBatchSize = max(maxBatchSize, batchSize)
		return nil
	})

	if completed == nil {
		t.Fatal("expected completion progress")
	}
	if completed.TotalJobs != jobCount || completed.LoadedJobs != jobCount {
		t.Fatalf(
			"expected %d recovered jobs, got total=%d loaded=%d",
			jobCount,
			completed.TotalJobs,
			completed.LoadedJobs,
		)
	}
	if completed.Result == nil || len(completed.Result.TopicJobs) != 0 {
		t.Fatal("completion result must not retain recovered job batches")
	}
	if batchCount != 3 {
		t.Fatalf("expected 3 batches, got %d", batchCount)
	}
	if maxBatchSize != 1000 {
		t.Fatalf("expected maximum batch size 1000, got %d", maxBatchSize)
	}
	if len(seen) != jobCount {
		t.Fatalf("expected %d unique jobs, got %d", jobCount, len(seen))
	}
}

func TestRecoveryManagerHonorsSnapshotMaxID(t *testing.T) {
	storage := newMemoryStorage()
	ctx := context.Background()
	for id := uint64(1); id <= 3; id++ {
		meta := NewJobMeta(id, "recover", 10, 0, time.Minute)
		meta.State = StateReady
		if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
			t.Fatalf("save job %d: %v", id, err)
		}
	}

	manager := newRecoveryManager(ctx, storage)
	var recovered []uint64
	manager.Recover(2, func(progress *RecoveryProgress) {
		if progress.Phase == RecoveryPhaseError {
			t.Fatalf("recover: %v", progress.Error)
		}
	}, func(batch *RecoveryResult) error {
		for _, jobs := range batch.TopicJobs {
			for _, meta := range jobs {
				recovered = append(recovered, meta.ID)
			}
		}
		return nil
	})

	if len(recovered) != 2 || recovered[0] != 1 || recovered[1] != 2 {
		t.Fatalf("expected snapshot jobs [1 2], got %v", recovered)
	}
}

func TestQueueWaitForRecoveryReturnsScanError(t *testing.T) {
	base := newMemoryStorage()
	meta := NewJobMeta(1, "recover", 10, 0, time.Minute)
	if err := base.SaveJob(context.Background(), meta, []byte("body")); err != nil {
		t.Fatalf("save job: %v", err)
	}

	wantErr := errors.New("scan failed")
	config := DefaultConfig()
	config.Storage = &scanErrorStorage{Storage: base, err: wantErr}
	config.Ticker = &noOpTicker{}
	queue, err := New(config)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	t.Cleanup(func() { _ = queue.Stop() })

	if err := queue.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	if err := queue.WaitForRecovery(time.Second); !errors.Is(err, wantErr) {
		t.Fatalf("WaitForRecovery() error = %v, want %v", err, wantErr)
	}
}

func TestQueueWaitForRecoveryReturnsNormalizationError(t *testing.T) {
	base := newMemoryStorage()
	meta := NewJobMeta(1, "recover", 10, 0, time.Minute)
	meta.State = StateReserved
	if err := base.SaveJob(context.Background(), meta, []byte("body")); err != nil {
		t.Fatalf("save job: %v", err)
	}

	wantErr := errors.New("update failed")
	config := DefaultConfig()
	config.Storage = &updateErrorStorage{Storage: base, err: wantErr}
	config.Ticker = &noOpTicker{}
	queue, err := New(config)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	t.Cleanup(func() { _ = queue.Stop() })

	if err := queue.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	if err := queue.WaitForRecovery(time.Second); !errors.Is(err, wantErr) {
		t.Fatalf("WaitForRecovery() error = %v, want %v", err, wantErr)
	}
}

func TestQueueWaitForRecoveryRejectsNonAdvancingCursor(t *testing.T) {
	base := newMemoryStorage()
	meta := NewJobMeta(1, "recover", 10, 0, time.Minute)
	if err := base.SaveJob(context.Background(), meta, []byte("body")); err != nil {
		t.Fatalf("save job: %v", err)
	}

	config := DefaultConfig()
	config.Storage = &nonAdvancingCursorStorage{Storage: base}
	config.Ticker = &noOpTicker{}
	queue, err := New(config)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	t.Cleanup(func() { _ = queue.Stop() })

	if err := queue.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	if err := queue.WaitForRecovery(time.Second); err == nil {
		t.Fatal("WaitForRecovery() accepted a non-advancing Storage cursor")
	}
}

func TestRecoveredReadyJobWakesExistingWaiter(t *testing.T) {
	base := newMemoryStorage()
	meta := NewJobMeta(1, "recover", 10, 0, time.Minute)
	if err := base.SaveJob(context.Background(), meta, []byte("body")); err != nil {
		t.Fatalf("save job: %v", err)
	}

	storage := &gatedScanStorage{
		Storage: base,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	config := DefaultConfig()
	config.Storage = storage
	config.Ticker = &noOpTicker{}
	queue, err := New(config)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	t.Cleanup(func() { _ = queue.Stop() })

	if err := queue.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("recovery scan did not start")
	}

	type reserveResult struct {
		job *Job
		err error
	}
	reserved := make(chan reserveResult, 1)
	go func() {
		job, err := queue.Reserve([]string{"recover"}, 2*time.Second)
		reserved <- reserveResult{job: job, err: err}
	}()

	deadline := time.Now().Add(time.Second)
	for {
		queue.reserveMgr.mu.RLock()
		waiters := queue.reserveMgr.waitingConns["recover"]
		waiting := waiters != nil && waiters.Len() > 0
		queue.reserveMgr.mu.RUnlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reserve waiter was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	close(storage.release)
	select {
	case result := <-reserved:
		if result.err != nil {
			t.Fatalf("reserve recovered job: %v", result.err)
		}
		if result.job == nil || result.job.Meta == nil || result.job.Meta.ID != 1 {
			t.Fatalf("reserved job = %#v, want ID 1", result.job)
		}
	case <-time.After(time.Second):
		t.Fatal("recovered ready job did not wake waiter")
	}

	if err := queue.WaitForRecovery(time.Second); err != nil {
		t.Fatalf("wait for recovery: %v", err)
	}
}
