package sdq

import (
	"context"
	"testing"
	"time"
)

func TestNewMemoryStorage(t *testing.T) {
	storage := NewMemoryStorage()
	if storage == nil {
		t.Fatal("NewMemoryStorage returned nil")
	}
	defer func() { _ = storage.Close() }()

	if storage.metas == nil {
		t.Error("metas map should be initialized")
	}

	if storage.bodies == nil {
		t.Error("bodies map should be initialized")
	}
}

func TestMemoryStorageSaveJob(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save job
	err := storage.SaveJob(ctx, meta, body)
	if err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Try to save duplicate
	err = storage.SaveJob(ctx, meta, body)
	if err != ErrJobExists {
		t.Errorf("SaveJob duplicate = %v, want ErrJobExists", err)
	}

	ReleaseJobMeta(meta)
}

func TestMemoryStorageUpdateJobMeta(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save job first
	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Update meta
	meta.State = StateReady
	meta.Reserves = 5

	err := storage.UpdateJobMeta(ctx, meta)
	if err != nil {
		t.Fatalf("UpdateJobMeta error: %v", err)
	}

	// Verify update
	gotMeta, err := storage.GetJobMeta(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetJobMeta error: %v", err)
	}

	if gotMeta.State != StateReady {
		t.Errorf("State = %v, want %v", gotMeta.State, StateReady)
	}

	if gotMeta.Reserves != 5 {
		t.Errorf("Reserves = %d, want %d", gotMeta.Reserves, 5)
	}

	// Update non-existent job
	nonExistent := NewJobMeta(999, "test", 10, 0, 30*time.Second)
	err = storage.UpdateJobMeta(ctx, nonExistent)
	if err != ErrNotFound {
		t.Errorf("UpdateJobMeta non-existent = %v, want ErrNotFound", err)
	}

	ReleaseJobMeta(meta)
	ReleaseJobMeta(nonExistent)
}

func TestMemoryStorageGetJobMeta(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
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

	// Should return a clone
	if gotMeta == meta {
		t.Error("GetJobMeta should return a clone")
	}

	// Get non-existent
	_, err = storage.GetJobMeta(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("GetJobMeta non-existent = %v, want ErrNotFound", err)
	}

	ReleaseJobMeta(meta)
}

func TestMemoryStorageGetJobBody(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
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
	if err != ErrNotFound {
		t.Errorf("GetJobBody non-existent = %v, want ErrNotFound", err)
	}

	ReleaseJobMeta(meta)
}

func TestMemoryStorageDeleteJob(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save job
	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Delete job
	err := storage.DeleteJob(ctx, meta.ID)
	if err != nil {
		t.Fatalf("DeleteJob error: %v", err)
	}

	// Verify deletion
	_, err = storage.GetJobMeta(ctx, meta.ID)
	if err != ErrNotFound {
		t.Errorf("GetJobMeta after delete = %v, want ErrNotFound", err)
	}

	_, err = storage.GetJobBody(ctx, meta.ID)
	if err != ErrNotFound {
		t.Errorf("GetJobBody after delete = %v, want ErrNotFound", err)
	}

	// Delete non-existent
	err = storage.DeleteJob(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("DeleteJob non-existent = %v, want ErrNotFound", err)
	}

	ReleaseJobMeta(meta)
}

func TestMemoryStorageScanJobMeta(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save multiple jobs
	for i := uint64(1); i <= 5; i++ {
		meta := NewJobMeta(i, "test", uint32(i), 0, 30*time.Second)
		meta.State = StateReady
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

	if result.Total != 5 {
		t.Errorf("Total = %d, want %d", result.Total, 5)
	}

	// Scan with filter by topic
	filter := &JobMetaFilter{Topic: "test"}
	result, err = storage.ScanJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanJobMeta with filter error: %v", err)
	}

	if len(result.Metas) != 5 {
		t.Errorf("Metas count with topic filter = %d, want %d", len(result.Metas), 5)
	}

	// Scan with filter by state
	state := StateReady
	filter = &JobMetaFilter{State: &state}
	result, err = storage.ScanJobMeta(ctx, filter)
	if err != nil {
		t.Fatalf("ScanJobMeta with state filter error: %v", err)
	}

	if len(result.Metas) != 5 {
		t.Errorf("Metas count with state filter = %d, want %d", len(result.Metas), 5)
	}

	// Scan with limit
	filter = &JobMetaFilter{Limit: 2}
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

func TestMemoryStorageCountJobs(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save multiple jobs with different states
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta1.State = StateReady
	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	meta2.State = StateDelayed
	meta3 := NewJobMeta(3, "other", 10, 0, 30*time.Second)
	meta3.State = StateReady

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
	filter := &JobMetaFilter{Topic: "test"}
	count, err = storage.CountJobs(ctx, filter)
	if err != nil {
		t.Fatalf("CountJobs with topic filter error: %v", err)
	}

	if count != 2 {
		t.Errorf("Count with topic filter = %d, want %d", count, 2)
	}

	// Count by state
	state := StateReady
	filter = &JobMetaFilter{State: &state}
	count, err = storage.CountJobs(ctx, filter)
	if err != nil {
		t.Fatalf("CountJobs with state filter error: %v", err)
	}

	if count != 2 {
		t.Errorf("Count with state filter = %d, want %d", count, 2)
	}
}

func TestMemoryStorageClose(t *testing.T) {
	storage := NewMemoryStorage()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	_ = storage.SaveJob(ctx, meta, []byte("body"))

	err := storage.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// After close, maps should be nil
	if storage.metas != nil {
		t.Error("metas should be nil after close")
	}

	if storage.bodies != nil {
		t.Error("bodies should be nil after close")
	}
}

func TestMemoryStorageStats(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save jobs in different topics
	meta1 := NewJobMeta(1, "topic1", 10, 0, 30*time.Second)
	meta2 := NewJobMeta(2, "topic2", 10, 0, 30*time.Second)

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

	if stats.MetaSize == 0 {
		t.Error("MetaSize should be > 0")
	}

	if stats.BodySize == 0 {
		t.Error("BodySize should be > 0")
	}
}

func TestMemoryStorageConcurrency(t *testing.T) {
	storage := NewMemoryStorage()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	done := make(chan bool)

	// Concurrent writes
	for i := range 10 {
		go func(id uint64) {
			meta := NewJobMeta(id, "test", 10, 0, 30*time.Second)
			_ = storage.SaveJob(ctx, meta, []byte("body"))
			done <- true
		}(uint64(i + 1))
	}

	// Wait for all writes
	for range 10 {
		<-done
	}

	// Verify count
	count, _ := storage.CountJobs(ctx, nil)
	if count != 10 {
		t.Errorf("Count = %d, want %d", count, 10)
	}
}
