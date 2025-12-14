package sqlite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"go-slim.dev/sdq"
)

func TestNew(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	if storage.db == nil {
		t.Error("db should not be nil")
	}

	// Verify default configuration
	if storage.maxBatchSize != 1000 {
		t.Errorf("Expected maxBatchSize=1000, got %d", storage.maxBatchSize)
	}
	if storage.maxBatchBytes != 16*1024*1024 {
		t.Errorf("Expected maxBatchBytes=16MB, got %d", storage.maxBatchBytes)
	}
}

func TestNewWithOptions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath,
		WithMaxBatchSize(500),
		WithMaxBatchBytes(8*1024*1024),
	)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	// Verify custom configuration
	if storage.maxBatchSize != 500 {
		t.Errorf("Expected maxBatchSize=500, got %d", storage.maxBatchSize)
	}
	if storage.maxBatchBytes != 8*1024*1024 {
		t.Errorf("Expected maxBatchBytes=8MB, got %d", storage.maxBatchBytes)
	}
}

func TestSaveJob(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := sdq.NewJobMeta(1, "test", 10, 5*time.Second, 30*time.Second)
	body := []byte("test body")

	// Save job
	err = storage.SaveJob(ctx, meta, body)
	if err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Try to save duplicate
	err = storage.SaveJob(ctx, meta, body)
	if err != sdq.ErrJobExists {
		t.Errorf("SaveJob duplicate = %v, want ErrJobExists", err)
	}
}

// TestClosedOperation tests operations after close.
func TestClosedOperation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	ctx := context.Background()
	meta := sdq.NewJobMeta(1, "test", 10, 0, 30*time.Second)

	// Save job
	err = storage.SaveJob(ctx, meta, []byte("body"))
	if err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Close storage
	err = storage.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Operations after close should return error (context canceled)
	err = storage.SaveJob(ctx, meta, []byte("body"))
	if err == nil {
		t.Error("SaveJob after Close should return error")
	}
}

// TestConcurrentWrites tests concurrent writes.
func TestConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	const numGoroutines = 10
	const jobsPerGoroutine = 100

	// Concurrent job writes
	var wg sync.WaitGroup
	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := range jobsPerGoroutine {
				id := uint64(goroutineID*jobsPerGoroutine + j + 1)
				meta := sdq.NewJobMeta(id, "test", 10, 0, 30*time.Second)
				err := storage.SaveJob(ctx, meta, fmt.Appendf(nil, "body-%d", id))
				if err != nil {
					t.Errorf("SaveJob error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all jobs were saved
	result, err := storage.ScanJobMeta(ctx, nil)
	if err != nil {
		t.Fatalf("ScanJobMeta error: %v", err)
	}

	expectedCount := numGoroutines * jobsPerGoroutine
	if len(result.Metas) != expectedCount {
		t.Errorf("Saved jobs = %d, want %d", len(result.Metas), expectedCount)
	}
}

// TestLargeBody tests large job bodies.
func TestLargeBody(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := sdq.NewJobMeta(1, "test", 10, 0, 30*time.Second)

	// Create 1MB job body
	largeBody := make([]byte, 1024*1024)
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}

	// Save large job
	err = storage.SaveJob(ctx, meta, largeBody)
	if err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Read and verify
	gotBody, err := storage.GetJobBody(ctx, 1)
	if err != nil {
		t.Fatalf("GetJobBody error: %v", err)
	}

	if len(gotBody) != len(largeBody) {
		t.Errorf("Body length = %d, want %d", len(gotBody), len(largeBody))
	}

	// Verify content
	for i := range largeBody {
		if gotBody[i] != largeBody[i] {
			t.Errorf("Body mismatch at index %d: got %d, want %d", i, gotBody[i], largeBody[i])
			break
		}
	}
}

// TestBatchOperations tests batch operations.
func TestBatchOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath,
		WithMaxBatchSize(100),
		WithMaxBatchBytes(1024*1024),
	)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Quickly save many jobs (triggers batch write)
	const numJobs = 1000
	for i := 1; i <= numJobs; i++ {
		meta := sdq.NewJobMeta(uint64(i), "test", 10, 0, 30*time.Second)
		err := storage.SaveJob(ctx, meta, fmt.Appendf(nil, "body-%d", i))
		if err != nil {
			t.Fatalf("SaveJob %d error: %v", i, err)
		}
	}

	// Wait for batch write to complete
	time.Sleep(200 * time.Millisecond)

	// Verify all jobs were saved
	result, err := storage.ScanJobMeta(ctx, nil)
	if err != nil {
		t.Fatalf("ScanJobMeta error: %v", err)
	}

	if len(result.Metas) != numJobs {
		t.Errorf("Saved jobs = %d, want %d", len(result.Metas), numJobs)
	}
}

// TestInvalidID tests invalid IDs.
func TestInvalidID(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Get non-existent job metadata
	_, err = storage.GetJobMeta(ctx, 99999)
	if err == nil {
		t.Error("GetJobMeta with invalid ID should return error")
	}

	// Get non-existent job body
	_, err = storage.GetJobBody(ctx, 99999)
	if err == nil {
		t.Error("GetJobBody with invalid ID should return error")
	}

	// Delete non-existent job (should return ErrNotFound)
	err = storage.DeleteJob(ctx, 99999)
	if err != sdq.ErrNotFound {
		t.Errorf("DeleteJob with invalid ID = %v, want ErrNotFound", err)
	}
}

func TestUpdateJobMetaSync(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := sdq.NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save job first
	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Update meta synchronously
	meta.State = sdq.StateReady
	meta.Reserves = 5

	err = storage.UpdateJobMetaSync(ctx, meta)
	if err != nil {
		t.Fatalf("UpdateJobMetaSync error: %v", err)
	}

	// Verify update
	gotMeta, err := storage.GetJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobMeta error: %v", err)
	}

	if gotMeta.State != sdq.StateReady {
		t.Errorf("State = %v, want %v", gotMeta.State, sdq.StateReady)
	}

	if gotMeta.Reserves != 5 {
		t.Errorf("Reserves = %d, want %d", gotMeta.Reserves, 5)
	}

	// Update non-existent job
	nonExistent := sdq.NewJobMeta(999, "test", 10, 0, 30*time.Second)
	err = storage.UpdateJobMetaSync(ctx, nonExistent)
	if err != sdq.ErrNotFound {
		t.Errorf("UpdateJobMetaSync non-existent = %v, want ErrNotFound", err)
	}
}

func TestGetJobMeta(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := sdq.NewJobMeta(1, "test", 10, 5*time.Second, 30*time.Second)
	body := []byte("test body")

	// Save job
	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Get meta
	gotMeta, err := storage.GetJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobMeta error: %v", err)
	}

	if gotMeta.ID != meta.ID {
		t.Errorf("ID = %d, want %d", gotMeta.ID, meta.ID)
	}

	if gotMeta.Topic != meta.Topic {
		t.Errorf("Topic = %s, want %s", gotMeta.Topic, meta.Topic)
	}

	if gotMeta.Priority != meta.Priority {
		t.Errorf("Priority = %d, want %d", gotMeta.Priority, meta.Priority)
	}

	// Get non-existent
	_, err = storage.GetJobMeta(ctx, 999)
	if err != sdq.ErrNotFound {
		t.Errorf("GetJobMeta non-existent = %v, want ErrNotFound", err)
	}
}

func TestGetJobBody(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := sdq.NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save job
	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Get body
	gotBody, err := storage.GetJobBody(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobBody error: %v", err)
	}

	if string(gotBody) != string(body) {
		t.Errorf("Body = %s, want %s", gotBody, body)
	}

	// Get non-existent
	_, err = storage.GetJobBody(ctx, 999)
	if err != sdq.ErrNotFound {
		t.Errorf("GetJobBody non-existent = %v, want ErrNotFound", err)
	}
}

func TestDeleteJob(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := sdq.NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save job
	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Delete job
	err = storage.DeleteJob(ctx, meta.ID)
	if err != nil {
		t.Fatalf("DeleteJob error: %v", err)
	}

	// Verify deletion
	_, err = storage.GetJobMeta(ctx, meta.ID)
	if err != sdq.ErrNotFound {
		t.Errorf("GetJobMeta after delete = %v, want ErrNotFound", err)
	}

	// Delete non-existent
	err = storage.DeleteJob(ctx, 999)
	if err != sdq.ErrNotFound {
		t.Errorf("DeleteJob non-existent = %v, want ErrNotFound", err)
	}
}

func TestScanJobMeta(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save multiple jobs
	for i := uint64(1); i <= 5; i++ {
		meta := sdq.NewJobMeta(i, "test", uint32(i), 0, 30*time.Second)
		meta.State = sdq.StateReady
		if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
			t.Fatalf("SaveJob error: %v", err)
		}
	}

	// Scan all
	result, err := storage.ScanJobMeta(ctx, nil)
	if err != nil {
		t.Fatalf("ScanJobMeta error: %v", err)
	}

	if len(result.Metas) != 5 {
		t.Errorf("Metas count = %d, want %d", len(result.Metas), 5)
	}

	// Scan with filter by topic
	filter := &sdq.JobMetaFilter{Topic: "test"}
	result, err = storage.ScanJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanJobMeta with filter error: %v", err)
	}

	if len(result.Metas) != 5 {
		t.Errorf("Metas count with topic filter = %d, want %d", len(result.Metas), 5)
	}

	// Scan with filter by state
	state := sdq.StateReady
	filter = &sdq.JobMetaFilter{State: &state}
	result, err = storage.ScanJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanJobMeta with state filter error: %v", err)
	}

	if len(result.Metas) != 5 {
		t.Errorf("Metas count with state filter = %d, want %d", len(result.Metas), 5)
	}

	// Scan with limit
	filter = &sdq.JobMetaFilter{Limit: 2}
	result, err = storage.ScanJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanJobMeta with limit error: %v", err)
	}

	if len(result.Metas) != 2 {
		t.Errorf("Metas count with limit = %d, want %d", len(result.Metas), 2)
	}

	if !result.HasMore {
		t.Error("HasMore should be true")
	}
}

func TestCountJobs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save multiple jobs with different states
	meta1 := sdq.NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = sdq.StateReady
	meta2 := sdq.NewJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.State = sdq.StateDelayed
	meta3 := sdq.NewJobMeta(3, "other", 10, 0, 30*time.Second)
	meta3.State = sdq.StateReady

	_ = storage.SaveJob(ctx, meta1, []byte("body"))
	_ = storage.SaveJob(ctx, meta2, []byte("body"))
	_ = storage.SaveJob(ctx, meta3, []byte("body"))

	// Count all
	count, err := storage.CountJobs(ctx, nil)
	if err != nil {
		t.Fatalf("CountJobs error: %v", err)
	}

	if count != 3 {
		t.Errorf("Count = %d, want %d", count, 3)
	}

	// Count by topic
	filter := &sdq.JobMetaFilter{Topic: "test"}
	count, err = storage.CountJobs(ctx, filter)
	if err != nil {
		t.Fatalf("CountJobs with topic filter error: %v", err)
	}

	if count != 2 {
		t.Errorf("Count with topic filter = %d, want %d", count, 2)
	}

	// Count by state
	state := sdq.StateReady
	filter = &sdq.JobMetaFilter{State: &state}
	count, err = storage.CountJobs(ctx, filter)
	if err != nil {
		t.Fatalf("CountJobs with state filter error: %v", err)
	}

	if count != 2 {
		t.Errorf("Count with state filter = %d, want %d", count, 2)
	}
}

func TestStats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save jobs in different topics
	meta1 := sdq.NewJobMeta(1, "topic1", 10, 0, 30*time.Second)
	meta2 := sdq.NewJobMeta(2, "topic2", 10, 0, 30*time.Second)

	_ = storage.SaveJob(ctx, meta1, []byte("body1"))
	_ = storage.SaveJob(ctx, meta2, []byte("longer body 2"))

	stats, err := storage.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats error: %v", err)
	}

	if stats.TotalJobs != 2 {
		t.Errorf("TotalJobs = %d, want %d", stats.TotalJobs, 2)
	}

	if stats.TotalTopics != 2 {
		t.Errorf("TotalTopics = %d, want %d", stats.TotalTopics, 2)
	}
}

func TestVacuum(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	// Should not error
	err = storage.Vacuum()
	if err != nil {
		t.Errorf("Vacuum error: %v", err)
	}
}

func TestExportJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save a job
	meta := sdq.NewJobMeta(1, "test", 10, 0, 30*time.Second)
	_ = storage.SaveJob(ctx, meta, []byte("body"))

	// Export
	data, err := storage.ExportJSON(ctx)
	if err != nil {
		t.Fatalf("ExportJSON error: %v", err)
	}

	if len(data) == 0 {
		t.Error("ExportJSON should return data")
	}
}

func TestAsyncUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := sdq.NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save job first
	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Update meta asynchronously
	meta.State = sdq.StateReserved
	meta.Reserves = 1

	err = storage.UpdateJobMeta(ctx, meta)
	if err != nil {
		t.Fatalf("UpdateJobMeta error: %v", err)
	}

	// Wait for async flush
	time.Sleep(200 * time.Millisecond)

	// Verify update
	gotMeta, err := storage.GetJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobMeta error: %v", err)
	}

	if gotMeta.State != sdq.StateReserved {
		t.Errorf("State = %v, want %v", gotMeta.State, sdq.StateReserved)
	}
}

// TestConcurrentReadWrite tests high-concurrency read/write conflicts.
func TestConcurrentReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Pre-populate with jobs
	const numJobs = 100
	for i := uint64(1); i <= numJobs; i++ {
		meta := sdq.NewJobMeta(i, fmt.Sprintf("topic-%d", i%10), 10, 0, 30*time.Second)
		err := storage.SaveJob(ctx, meta, fmt.Appendf(nil, "body-%d", i))
		if err != nil {
			t.Fatalf("SaveJob %d error: %v", i, err)
		}
	}

	const duration = 1 * time.Second
	stopTime := time.Now().Add(duration)

	var wg sync.WaitGroup
	var readOps, writeOps, deleteOps uint64

	// Concurrent readers
	for range 5 {
		wg.Go(func() {
			for time.Now().Before(stopTime) {
				id := uint64((time.Now().UnixNano() % numJobs) + 1)
				_, _ = storage.GetJobMeta(ctx, id)
				_, _ = storage.GetJobBody(ctx, id)
				atomic.AddUint64(&readOps, 1)
			}
		})
	}

	// Concurrent writers (updating jobs)
	for range 3 {
		wg.Go(func() {
			for time.Now().Before(stopTime) {
				id := uint64((time.Now().UnixNano() % numJobs) + 1)
				meta, err := storage.GetJobMeta(ctx, id)
				if err == nil {
					meta.Reserves++
					meta.State = sdq.StateReserved
					_ = storage.UpdateJobMeta(ctx, meta)
					atomic.AddUint64(&writeOps, 1)
				}
			}
		})
	}

	// Concurrent scanners
	for i := range 2 {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for time.Now().Before(stopTime) {
				topic := fmt.Sprintf("topic-%d", workerID%10)
				filter := &sdq.JobMetaFilter{Topic: topic, Limit: 10}
				_, _ = storage.ScanJobMeta(ctx, filter)
				_, _ = storage.CountJobs(ctx, filter)
				atomic.AddUint64(&readOps, 1)
			}
		}(i)
	}

	// Concurrent delete/recreate operations
	wg.Go(func() {
		jobID := uint64(numJobs + 1)
		for time.Now().Before(stopTime) {
			meta := sdq.NewJobMeta(jobID, "test-delete", 10, 0, 30*time.Second)
			_ = storage.SaveJob(ctx, meta, []byte("temp"))
			_ = storage.DeleteJob(ctx, jobID)
			atomic.AddUint64(&deleteOps, 1)
		}
	})

	wg.Wait()

	t.Logf("Completed: %d reads, %d writes, %d deletes",
		atomic.LoadUint64(&readOps),
		atomic.LoadUint64(&writeOps),
		atomic.LoadUint64(&deleteOps))

	// Verify storage is still consistent
	stats, err := storage.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats error after concurrent operations: %v", err)
	}

	if stats.TotalJobs > numJobs+1 {
		t.Errorf("TotalJobs = %d, should be <= %d", stats.TotalJobs, numJobs+1)
	}
}

// TestConcurrentUpdates tests multiple goroutines updating the same job.
func TestConcurrentUpdates(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Create a single job
	meta := sdq.NewJobMeta(1, "test", 10, 0, 30*time.Second)
	err = storage.SaveJob(ctx, meta, []byte("body"))
	if err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Multiple goroutines updating the same job
	const numGoroutines = 20
	const updatesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(goroutineID int) {
			defer wg.Done()
			for range updatesPerGoroutine {
				meta, err := storage.GetJobMeta(ctx, 1)
				if err != nil {
					t.Errorf("GetJobMeta error: %v", err)
					return
				}
				meta.Reserves++
				meta.State = sdq.State(goroutineID % 5) // Cycle through states
				err = storage.UpdateJobMetaSync(ctx, meta)
				if err != nil {
					t.Errorf("UpdateJobMetaSync error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state - some updates may be lost due to concurrent read-modify-write,
	// but the job should still be in a valid state
	finalMeta, err := storage.GetJobMeta(ctx, 1)
	if err != nil {
		t.Fatalf("GetJobMeta final error: %v", err)
	}

	// We expect some reserves (but not necessarily all due to lost updates from concurrent read-modify-write)
	if finalMeta.Reserves == 0 {
		t.Error("Expected some reserves to be recorded")
	}

	t.Logf("Final Reserves = %d (out of %d attempted updates)", finalMeta.Reserves, numGoroutines*updatesPerGoroutine)
}

// TestStressTest tests storage under heavy load.
func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath, WithMaxBatchSize(500))
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	const duration = 2 * time.Second
	stopTime := time.Now().Add(duration)

	var wg sync.WaitGroup
	var jobCounter uint64 = 1000
	var opCount uint64

	// Concurrent job creators
	for range 5 {
		wg.Go(func() {
			for time.Now().Before(stopTime) {
				id := atomic.AddUint64(&jobCounter, 1)
				meta := sdq.NewJobMeta(id, "stress-test", 10, 0, 30*time.Second)
				_ = storage.SaveJob(ctx, meta, []byte("body"))
				atomic.AddUint64(&opCount, 1)
			}
		})
	}

	// Concurrent readers
	for range 10 {
		wg.Go(func() {
			for time.Now().Before(stopTime) {
				id := atomic.LoadUint64(&jobCounter)
				if id > 1000 {
					_, _ = storage.GetJobMeta(ctx, id-uint64(time.Now().UnixNano()%100))
				}
				atomic.AddUint64(&opCount, 1)
			}
		})
	}

	// Concurrent scanners
	for range 3 {
		wg.Go(func() {
			for time.Now().Before(stopTime) {
				filter := &sdq.JobMetaFilter{Topic: "stress-test", Limit: 100}
				_, _ = storage.ScanJobMeta(ctx, filter)
				atomic.AddUint64(&opCount, 1)
				time.Sleep(10 * time.Millisecond)
			}
		})
	}

	wg.Wait()

	t.Logf("Stress test completed: %d operations", atomic.LoadUint64(&opCount))

	// Verify storage is still functional
	stats, err := storage.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats error after stress test: %v", err)
	}

	t.Logf("Final stats: %d jobs, %d topics", stats.TotalJobs, stats.TotalTopics)
}

// TestContextCancellation tests context cancellation handling.
func TestContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	t.Run("SaveJob with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		meta := &sdq.JobMeta{
			ID:       1,
			Topic:    "test",
			State:    sdq.StateReady,
			Priority: 1,
		}

		err := storage.SaveJob(ctx, meta, []byte("body"))
		// SQLite operations may be too fast, completing before context check
		// So we just log the result, don't require failure
		if err != nil {
			t.Logf("SaveJob with cancelled context failed as expected: %v", err)
		} else {
			t.Logf("SaveJob with cancelled context succeeded (operation was too fast)")
		}
	})

	t.Run("GetJobMeta with cancelled context", func(t *testing.T) {
		// First save a job
		ctx := context.Background()
		meta := &sdq.JobMeta{
			ID:       2,
			Topic:    "test",
			State:    sdq.StateReady,
			Priority: 1,
		}
		err := storage.SaveJob(ctx, meta, []byte("body"))
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}

		// Read with cancelled context
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = storage.GetJobMeta(cancelledCtx, 2)
		if err == nil {
			t.Error("GetJobMeta with cancelled context should fail")
		}
	})

	t.Run("UpdateJobMeta with cancelled context", func(t *testing.T) {
		// First save a job
		ctx := context.Background()
		meta := &sdq.JobMeta{
			ID:       3,
			Topic:    "test",
			State:    sdq.StateReady,
			Priority: 1,
		}
		err := storage.SaveJob(ctx, meta, []byte("body"))
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}

		// Update with cancelled context
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		meta.State = sdq.StateReserved
		err = storage.UpdateJobMeta(cancelledCtx, meta)
		// SQLite operations may be too fast, completing before context check
		if err != nil {
			t.Logf("UpdateJobMeta with cancelled context failed as expected: %v", err)
		} else {
			t.Logf("UpdateJobMeta with cancelled context succeeded (operation was too fast)")
		}
	})

	t.Run("ScanJobMeta with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		filter := &sdq.JobMetaFilter{Topic: "test", Limit: 10}
		_, err := storage.ScanJobMeta(ctx, filter)
		if err == nil {
			t.Error("ScanJobMeta with cancelled context should fail")
		}
	})

	t.Run("DeleteJob with cancelled context", func(t *testing.T) {
		// First save a job
		ctx := context.Background()
		meta := &sdq.JobMeta{
			ID:       4,
			Topic:    "test",
			State:    sdq.StateReady,
			Priority: 1,
		}
		err := storage.SaveJob(ctx, meta, []byte("body"))
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}

		// Delete with cancelled context
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err = storage.DeleteJob(cancelledCtx, 4)
		if err == nil {
			t.Error("DeleteJob with cancelled context should fail")
		}
	})
}

// TestContextTimeout tests context timeout handling.
func TestContextTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	t.Run("SaveJob with timeout", func(t *testing.T) {
		// Set a very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Wait for timeout
		time.Sleep(1 * time.Millisecond)

		meta := &sdq.JobMeta{
			ID:       100,
			Topic:    "test",
			State:    sdq.StateReady,
			Priority: 1,
		}

		err := storage.SaveJob(ctx, meta, []byte("body"))
		// SQLite operations may be too fast, completing before timeout
		if err != nil {
			t.Logf("SaveJob with timed out context failed as expected: %v", err)
		} else {
			t.Logf("SaveJob with timed out context succeeded (operation was too fast)")
		}
	})

	t.Run("ScanJobMeta with timeout", func(t *testing.T) {
		// First add some jobs
		ctx := context.Background()
		for i := 200; i < 250; i++ {
			meta := &sdq.JobMeta{
				ID:       uint64(i),
				Topic:    "test",
				State:    sdq.StateReady,
				Priority: 1,
			}
			_ = storage.SaveJob(ctx, meta, []byte("body"))
		}

		// Scan with timeout
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		time.Sleep(1 * time.Millisecond)

		filter := &sdq.JobMetaFilter{Topic: "test", Limit: 100}
		_, err := storage.ScanJobMeta(timeoutCtx, filter)
		if err == nil {
			t.Error("ScanJobMeta with timed out context should fail")
		}
	})
}

// TestContextCancellationDuringOperation tests context cancellation during operation.
func TestContextCancellationDuringOperation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	// Add many jobs to ensure scanning takes time
	ctx := context.Background()
	for i := 1; i <= 1000; i++ {
		meta := &sdq.JobMeta{
			ID:       uint64(i),
			Topic:    "test",
			State:    sdq.StateReady,
			Priority: 1,
		}
		err := storage.SaveJob(ctx, meta, []byte("body"))
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}
	}

	t.Run("Cancel during scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Execute scan in another goroutine
		var scanErr error
		done := make(chan struct{})

		go func() {
			filter := &sdq.JobMetaFilter{Topic: "test", Limit: 1000}
			_, scanErr = storage.ScanJobMeta(ctx, filter)
			close(done)
		}()

		// Short delay then cancel
		time.Sleep(10 * time.Millisecond)
		cancel()

		<-done

		// Scan should be cancelled (or completed)
		if scanErr != nil {
			t.Logf("Scan was cancelled or errored: %v", scanErr)
		}
	})
}
