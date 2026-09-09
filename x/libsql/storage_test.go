package libsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	turso "turso.tech/database/tursogo"

	"go-slim.dev/sdq"
)

func TestStorageLifecycle(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)

	now := time.Now().Truncate(time.Second)
	meta := &sdq.JobMeta{
		ID:             42,
		Topic:          "email",
		Priority:       7,
		State:          sdq.StateReserved,
		Delay:          3 * time.Second,
		TTR:            30 * time.Second,
		CreatedAt:      now.Add(-time.Minute),
		ReadyAt:        now.Add(-30 * time.Second),
		ReservedAt:     now.Add(-5 * time.Second),
		LastTouchAt:    now.Add(-2 * time.Second),
		BuriedAt:       now.Add(-time.Hour),
		DeletedAt:      now.Add(-2 * time.Hour),
		Reserves:       2,
		Timeouts:       3,
		Releases:       4,
		Buries:         5,
		Kicks:          6,
		Touches:        7,
		TotalTouchTime: 8 * time.Second,
	}
	body := []byte("deliver this message")

	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob() error = %v", err)
	}
	if err := storage.SaveJob(ctx, meta, body); !errors.Is(err, sdq.ErrJobExists) {
		t.Fatalf("duplicate SaveJob() error = %v, want ErrJobExists", err)
	}

	gotMeta, err := storage.GetJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobMeta() error = %v", err)
	}
	assertMetaEqual(t, gotMeta, meta)

	gotBody, err := storage.GetJobBody(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobBody() error = %v", err)
	}
	if string(gotBody) != string(body) {
		t.Fatalf("GetJobBody() = %q, want %q", gotBody, body)
	}

	meta.State = sdq.StateBuried
	meta.Buries++
	if err := storage.UpdateJobMeta(ctx, meta); err != nil {
		t.Fatalf("UpdateJobMeta() error = %v", err)
	}
	gotMeta, err = storage.GetJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobMeta() after update error = %v", err)
	}
	assertMetaEqual(t, gotMeta, meta)

	if err := storage.DeleteJob(ctx, meta.ID); err != nil {
		t.Fatalf("DeleteJob() error = %v", err)
	}
	if _, err := storage.GetJobMeta(ctx, meta.ID); !errors.Is(err, sdq.ErrNotFound) {
		t.Fatalf("GetJobMeta() after delete error = %v, want ErrNotFound", err)
	}
	if _, err := storage.GetJobBody(ctx, meta.ID); !errors.Is(err, sdq.ErrNotFound) {
		t.Fatalf("GetJobBody() after delete error = %v, want ErrNotFound", err)
	}
	if err := storage.DeleteJob(ctx, meta.ID); !errors.Is(err, sdq.ErrNotFound) {
		t.Fatalf("second DeleteJob() error = %v, want ErrNotFound", err)
	}
}

func TestStorageScanCountAndMaxID(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)

	states := []sdq.State{
		sdq.StateReady,
		sdq.StateDelayed,
		sdq.StateReady,
		sdq.StateBuried,
		sdq.StateReady,
	}
	for index, state := range states {
		id := uint64(index + 1)
		topic := "alpha"
		if id == 4 {
			topic = "beta"
		}
		meta := sdq.NewJobMeta(id, topic, uint32(id), 0, time.Minute)
		meta.State = state
		if err := storage.SaveJob(ctx, meta, []byte{byte(id)}); err != nil {
			t.Fatalf("SaveJob(%d) error = %v", id, err)
		}
	}

	ready := sdq.StateReady
	count, err := storage.CountJobs(ctx, &sdq.JobMetaFilter{
		Topic:  "alpha",
		State:  &ready,
		Limit:  1,
		Offset: 99,
		Cursor: 99,
	})
	if err != nil {
		t.Fatalf("CountJobs() error = %v", err)
	}
	if count != 3 {
		t.Fatalf("CountJobs() = %d, want 3", count)
	}

	first, err := storage.ScanJobMeta(ctx, &sdq.JobMetaFilter{
		Topic: "alpha",
		State: &ready,
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("first ScanJobMeta() error = %v", err)
	}
	assertIDs(t, first.Metas, 1, 3)
	if !first.HasMore || first.NextCursor != 3 {
		t.Fatalf("first page hasMore = %v, nextCursor = %d; want true, 3", first.HasMore, first.NextCursor)
	}

	second, err := storage.ScanJobMeta(ctx, &sdq.JobMetaFilter{
		Topic:  "alpha",
		State:  &ready,
		Limit:  2,
		Cursor: first.NextCursor,
	})
	if err != nil {
		t.Fatalf("second ScanJobMeta() error = %v", err)
	}
	assertIDs(t, second.Metas, 5)
	if second.HasMore || second.NextCursor != 0 {
		t.Fatalf("second page hasMore = %v, nextCursor = %d; want false, 0", second.HasMore, second.NextCursor)
	}

	maxID, err := storage.GetMaxJobID(ctx)
	if err != nil {
		t.Fatalf("GetMaxJobID() error = %v", err)
	}
	if maxID != 5 {
		t.Fatalf("GetMaxJobID() = %d, want 5", maxID)
	}

	stats, err := storage.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.Name != "libsql" || stats.TotalJobs != 5 || stats.TotalTopics != 2 || stats.BodySize != 5 {
		t.Fatalf("Stats() = %+v", stats)
	}
}

func TestStoragePersistsEmptyBodyAndMetadata(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "queue.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	meta := sdq.NewJobMeta(1, "empty", 1, 0, time.Second)
	if err := storage.SaveJob(ctx, meta, nil); err != nil {
		t.Fatalf("SaveJob() error = %v", err)
	}
	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := New(dbPath)
	if err != nil {
		t.Fatalf("reopen New() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()

	body, err := reopened.GetJobBody(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobBody() error = %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("GetJobBody() length = %d, want 0", len(body))
	}
	if _, err := reopened.GetJobMeta(ctx, meta.ID); err != nil {
		t.Fatalf("GetJobMeta() error = %v", err)
	}
}

func TestStorageInMemory(t *testing.T) {
	ctx := context.Background()
	storage, err := New(":memory:", WithMaxOpenConns(5), WithMaxIdleConns(0))
	if err != nil {
		t.Fatalf("New(:memory:) error = %v", err)
	}
	defer func() { _ = storage.Close() }()

	meta := sdq.NewJobMeta(1, "memory", 1, 0, time.Second)
	if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
		t.Fatalf("SaveJob() error = %v", err)
	}
	if _, err := storage.GetJobMeta(ctx, meta.ID); err != nil {
		t.Fatalf("GetJobMeta() error = %v", err)
	}
}

func TestStorageMVCCThresholdOptions(t *testing.T) {
	ctx := context.Background()
	storage, err := New(
		filepath.Join(t.TempDir(), "thresholds.db"),
		WithMaxOpenConns(2),
		WithMaxIdleConns(2),
		WithMVCCCheckpointThreshold(123),
		WithMVCCGCThreshold(456),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = storage.Close() }()

	connections := make([]*sql.Conn, 2)
	for index := range connections {
		connections[index], err = storage.db.Conn(ctx)
		if err != nil {
			t.Fatalf("Conn(%d) error = %v", index, err)
		}
		defer func(conn *sql.Conn) { _ = conn.Close() }(connections[index])
	}

	for index, conn := range connections {
		assertPragmaValue(t, ctx, conn, "mvcc_checkpoint_threshold", 123, index)
		assertPragmaValue(t, ctx, conn, "mvcc_gc_threshold", 456, index)
	}
}

func TestStorageRejectsCanceledSave(t *testing.T) {
	storage := newTestStorage(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	meta := sdq.NewJobMeta(1, "canceled", 1, 0, time.Second)
	if err := storage.SaveJob(ctx, meta, []byte("body")); !errors.Is(err, context.Canceled) {
		t.Fatalf("SaveJob() error = %v, want context.Canceled", err)
	}
	if _, err := storage.GetJobMeta(context.Background(), meta.ID); !errors.Is(err, sdq.ErrNotFound) {
		t.Fatalf("GetJobMeta() error = %v, want sdq.ErrNotFound", err)
	}
}

func TestStorageMigratesLegacySQLiteSchema(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	base, err := turso.NewConnector(dbPath)
	if err != nil {
		t.Fatalf("NewConnector() error = %v", err)
	}
	db := sql.OpenDB(base)
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE job_meta (
			id INTEGER PRIMARY KEY,
			topic TEXT NOT NULL,
			priority INTEGER NOT NULL,
			state INTEGER NOT NULL,
			delay INTEGER NOT NULL,
			ttr INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			ready_at INTEGER NOT NULL,
			reserved_at INTEGER,
			buried_at INTEGER,
			deleted_at INTEGER,
			reserves INTEGER DEFAULT 0,
			timeouts INTEGER DEFAULT 0,
			releases INTEGER DEFAULT 0,
			buries INTEGER DEFAULT 0,
			kicks INTEGER DEFAULT 0,
			touches INTEGER DEFAULT 0
		)`); err != nil {
		t.Fatalf("create legacy schema error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy database error = %v", err)
	}

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New() with legacy schema error = %v", err)
	}
	defer func() { _ = storage.Close() }()

	meta := sdq.NewJobMeta(1, "legacy", 1, 0, time.Minute)
	meta.LastTouchAt = time.Now().Truncate(time.Second)
	meta.TotalTouchTime = 5 * time.Second
	if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
		t.Fatalf("SaveJob() after migration error = %v", err)
	}
	got, err := storage.GetJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobMeta() after migration error = %v", err)
	}
	if !got.LastTouchAt.Equal(meta.LastTouchAt) || got.TotalTouchTime != meta.TotalTouchTime {
		t.Fatalf("migrated fields = (%v, %v), want (%v, %v)",
			got.LastTouchAt,
			got.TotalTouchTime,
			meta.LastTouchAt,
			meta.TotalTouchTime,
		)
	}
}

func TestStorageConcurrentSave(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)

	const (
		workers       = 8
		jobsPerWorker = 25
	)
	errorsCh := make(chan error, workers*jobsPerWorker)
	var waitGroup sync.WaitGroup
	for worker := range workers {
		waitGroup.Go(func() {
			for job := range jobsPerWorker {
				id := uint64(worker*jobsPerWorker + job + 1)
				meta := sdq.NewJobMeta(id, "concurrent", uint32(job), 0, time.Minute)
				if err := storage.SaveJob(ctx, meta, fmt.Appendf(nil, "body-%d", id)); err != nil {
					errorsCh <- fmt.Errorf("SaveJob(%d): %w", id, err)
				}
			}
		})
	}
	waitGroup.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}

	count, err := storage.CountJobs(ctx, nil)
	if err != nil {
		t.Fatalf("CountJobs() error = %v", err)
	}
	if count != workers*jobsPerWorker {
		t.Fatalf("CountJobs() = %d, want %d", count, workers*jobsPerWorker)
	}
}

func TestStorageConcurrentDuplicateSave(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)

	const workers = 16
	start := make(chan struct{})
	results := make(chan error, workers)
	var waitGroup sync.WaitGroup
	for range workers {
		waitGroup.Go(func() {
			<-start
			meta := sdq.NewJobMeta(1, "duplicate", 1, 0, time.Minute)
			results <- storage.SaveJob(ctx, meta, []byte("body"))
		})
	}
	close(start)
	waitGroup.Wait()
	close(results)

	saved := 0
	duplicates := 0
	for err := range results {
		switch {
		case err == nil:
			saved++
		case errors.Is(err, sdq.ErrJobExists):
			duplicates++
		default:
			t.Fatalf("SaveJob() error = %v", err)
		}
	}
	if saved != 1 || duplicates != workers-1 {
		t.Fatalf("save results = (%d saved, %d duplicates), want (1, %d)",
			saved,
			duplicates,
			workers-1,
		)
	}
}

func TestStorageConcurrentMixedOperations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	storage := newTestStorage(t)

	const jobCount = 100
	for id := 1; id <= jobCount; id++ {
		meta := sdq.NewJobMeta(uint64(id), fmt.Sprintf("topic-%d", id%5), 1, 0, time.Minute)
		if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
			t.Fatalf("SaveJob(%d) error = %v", id, err)
		}
	}

	const operations = 200
	errorsCh := make(chan error, 16)
	var waitGroup sync.WaitGroup
	for worker := range 6 {
		waitGroup.Go(func() {
			id := uint64(worker + 1)
			for sequence := range operations {
				meta, err := storage.GetJobMeta(ctx, id)
				if err != nil {
					errorsCh <- fmt.Errorf("GetJobMeta(%d): %w", id, err)
					return
				}
				meta.Reserves = sequence + 1
				if err := storage.UpdateJobMeta(ctx, meta); err != nil {
					errorsCh <- fmt.Errorf("UpdateJobMeta(%d): %w", id, err)
					return
				}
			}
		})
	}
	for reader := range 4 {
		waitGroup.Go(func() {
			for sequence := range operations {
				id := uint64((reader*operations+sequence)%jobCount + 1)
				if _, err := storage.GetJobBody(ctx, id); err != nil {
					errorsCh <- fmt.Errorf("GetJobBody(%d): %w", id, err)
					return
				}
			}
		})
	}
	for scanner := range 2 {
		waitGroup.Go(func() {
			for range operations / 4 {
				if _, err := storage.ScanJobMeta(ctx, &sdq.JobMetaFilter{
					Topic: fmt.Sprintf("topic-%d", scanner),
					Limit: 20,
				}); err != nil {
					errorsCh <- fmt.Errorf("ScanJobMeta(): %w", err)
					return
				}
			}
		})
	}
	waitGroup.Go(func() {
		for sequence := range operations {
			id := uint64(jobCount + sequence + 1)
			meta := sdq.NewJobMeta(id, "ephemeral", 1, 0, time.Minute)
			if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
				errorsCh <- fmt.Errorf("SaveJob(%d): %w", id, err)
				return
			}
			if err := storage.DeleteJob(ctx, id); err != nil {
				errorsCh <- fmt.Errorf("DeleteJob(%d): %w", id, err)
				return
			}
		}
	})

	waitGroup.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("mixed operations context error = %v", err)
	}

	count, err := storage.CountJobs(ctx, nil)
	if err != nil {
		t.Fatalf("CountJobs() error = %v", err)
	}
	if count != jobCount {
		t.Fatalf("CountJobs() = %d, want %d", count, jobCount)
	}
}

func TestStorageErrorsAndClose(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)

	if err := storage.SaveJob(ctx, nil, nil); !errors.Is(err, ErrNilJobMeta) {
		t.Fatalf("SaveJob(nil) error = %v, want ErrNilJobMeta", err)
	}
	if err := storage.UpdateJobMeta(ctx, nil); !errors.Is(err, ErrNilJobMeta) {
		t.Fatalf("UpdateJobMeta(nil) error = %v, want ErrNilJobMeta", err)
	}
	if err := storage.UpdateJobMeta(ctx, &sdq.JobMeta{ID: 999}); !errors.Is(err, sdq.ErrNotFound) {
		t.Fatalf("UpdateJobMeta(missing) error = %v, want ErrNotFound", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := storage.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := storage.SaveJob(ctx, &sdq.JobMeta{ID: 1}, nil); !errors.Is(err, sdq.ErrStorageClosed) {
		t.Fatalf("SaveJob() after Close error = %v, want ErrStorageClosed", err)
	}
	if _, err := storage.ScanJobMeta(ctx, nil); !errors.Is(err, sdq.ErrStorageClosed) {
		t.Fatalf("ScanJobMeta() after Close error = %v, want ErrStorageClosed", err)
	}
}

func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	storage, err := New(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	return storage
}

func assertPragmaValue(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	name string,
	want int,
	connectionIndex int,
) {
	t.Helper()
	var got int
	if err := conn.QueryRowContext(ctx, "PRAGMA "+name).Scan(&got); err != nil {
		t.Fatalf("query %s on connection %d error = %v", name, connectionIndex, err)
	}
	if got != want {
		t.Fatalf("%s on connection %d = %d, want %d", name, connectionIndex, got, want)
	}
}

func assertIDs(t *testing.T, metas []*sdq.JobMeta, want ...uint64) {
	t.Helper()
	if len(metas) != len(want) {
		t.Fatalf("metadata count = %d, want %d", len(metas), len(want))
	}
	for index := range want {
		if metas[index].ID != want[index] {
			t.Fatalf("metadata[%d].ID = %d, want %d", index, metas[index].ID, want[index])
		}
	}
}

func assertMetaEqual(t *testing.T, got, want *sdq.JobMeta) {
	t.Helper()
	if got.ID != want.ID ||
		got.Topic != want.Topic ||
		got.Priority != want.Priority ||
		got.State != want.State ||
		got.Delay != want.Delay ||
		got.TTR != want.TTR ||
		!got.CreatedAt.Equal(want.CreatedAt) ||
		!got.ReadyAt.Equal(want.ReadyAt) ||
		!got.ReservedAt.Equal(want.ReservedAt) ||
		!got.LastTouchAt.Equal(want.LastTouchAt) ||
		!got.BuriedAt.Equal(want.BuriedAt) ||
		!got.DeletedAt.Equal(want.DeletedAt) ||
		got.Reserves != want.Reserves ||
		got.Timeouts != want.Timeouts ||
		got.Releases != want.Releases ||
		got.Buries != want.Buries ||
		got.Kicks != want.Kicks ||
		got.Touches != want.Touches ||
		got.TotalTouchTime != want.TotalTouchTime {
		t.Fatalf("metadata mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}
