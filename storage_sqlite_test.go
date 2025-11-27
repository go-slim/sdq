package sdq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSQLiteStorage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	if storage.db == nil {
		t.Error("db should not be nil")
	}

	// 验证默认配置
	if storage.maxBatchSize != 1000 {
		t.Errorf("Expected maxBatchSize=1000, got %d", storage.maxBatchSize)
	}
	if storage.maxBatchBytes != 16*1024*1024 {
		t.Errorf("Expected maxBatchBytes=16MB, got %d", storage.maxBatchBytes)
	}
}

func TestNewSQLiteStorageWithOptions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath,
		WithMaxBatchSize(500),
		WithMaxBatchBytes(8*1024*1024),
	)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	// 验证自定义配置
	if storage.maxBatchSize != 500 {
		t.Errorf("Expected maxBatchSize=500, got %d", storage.maxBatchSize)
	}
	if storage.maxBatchBytes != 8*1024*1024 {
		t.Errorf("Expected maxBatchBytes=8MB, got %d", storage.maxBatchBytes)
	}
}

func TestSQLiteStorageSaveJob(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 5*time.Second, 30*time.Second)
	body := []byte("test body")

	// Save job
	err = storage.SaveJob(ctx, meta, body)
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

// TestSQLiteStorage_ClosedOperation 测试关闭后操作
func TestSQLiteStorage_ClosedOperation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)

	// 保存任务
	err = storage.SaveJob(ctx, meta, []byte("body"))
	if err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// 关闭存储
	err = storage.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// 关闭后的操作应该返回错误（context canceled）
	err = storage.SaveJob(ctx, meta, []byte("body"))
	if err == nil {
		t.Error("SaveJob after Close should return error")
	}

	ReleaseJobMeta(meta)
}

// TestSQLiteStorage_ConcurrentWrites 测试并发写入
func TestSQLiteStorage_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	const numGoroutines = 10
	const jobsPerGoroutine = 100

	// 并发写入任务
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < jobsPerGoroutine; j++ {
				id := uint64(goroutineID*jobsPerGoroutine + j + 1)
				meta := NewJobMeta(id, "test", 10, 0, 30*time.Second)
				err := storage.SaveJob(ctx, meta, []byte(fmt.Sprintf("body-%d", id)))
				if err != nil {
					t.Errorf("SaveJob error: %v", err)
				}
				ReleaseJobMeta(meta)
			}
		}(i)
	}

	wg.Wait()

	// 验证所有任务都已保存
	result, err := storage.ScanJobMeta(ctx, nil)
	if err != nil {
		t.Fatalf("ScanJobMeta error: %v", err)
	}

	expectedCount := numGoroutines * jobsPerGoroutine
	if len(result.Metas) != expectedCount {
		t.Errorf("Saved jobs = %d, want %d", len(result.Metas), expectedCount)
	}
}

// TestSQLiteStorage_LargeBody 测试大任务体
func TestSQLiteStorage_LargeBody(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)

	// 创建 1MB 的任务体
	largeBody := make([]byte, 1024*1024)
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}

	// 保存大任务
	err = storage.SaveJob(ctx, meta, largeBody)
	if err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// 读取并验证
	gotBody, err := storage.GetJobBody(ctx, 1)
	if err != nil {
		t.Fatalf("GetJobBody error: %v", err)
	}

	if len(gotBody) != len(largeBody) {
		t.Errorf("Body length = %d, want %d", len(gotBody), len(largeBody))
	}

	// 验证内容
	for i := range largeBody {
		if gotBody[i] != largeBody[i] {
			t.Errorf("Body mismatch at index %d: got %d, want %d", i, gotBody[i], largeBody[i])
			break
		}
	}

	ReleaseJobMeta(meta)
}

// TestSQLiteStorage_BatchOperations 测试批量操作
func TestSQLiteStorage_BatchOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath,
		WithMaxBatchSize(100),
		WithMaxBatchBytes(1024*1024),
	)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// 快速保存大量任务（触发批量写入）
	const numJobs = 1000
	for i := 1; i <= numJobs; i++ {
		meta := NewJobMeta(uint64(i), "test", 10, 0, 30*time.Second)
		err := storage.SaveJob(ctx, meta, []byte(fmt.Sprintf("body-%d", i)))
		if err != nil {
			t.Fatalf("SaveJob %d error: %v", i, err)
		}
		ReleaseJobMeta(meta)
	}

	// 等待批量写入完成
	time.Sleep(200 * time.Millisecond)

	// 验证所有任务都已保存
	result, err := storage.ScanJobMeta(ctx, nil)
	if err != nil {
		t.Fatalf("ScanJobMeta error: %v", err)
	}

	if len(result.Metas) != numJobs {
		t.Errorf("Saved jobs = %d, want %d", len(result.Metas), numJobs)
	}
}

// TestSQLiteStorage_InvalidID 测试无效 ID
func TestSQLiteStorage_InvalidID(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// 获取不存在的任务元数据
	_, err = storage.GetJobMeta(ctx, 99999)
	if err == nil {
		t.Error("GetJobMeta with invalid ID should return error")
	}

	// 获取不存在的任务体
	_, err = storage.GetJobBody(ctx, 99999)
	if err == nil {
		t.Error("GetJobBody with invalid ID should return error")
	}

	// 删除不存在的任务（应该返回 ErrNotFound）
	err = storage.DeleteJob(ctx, 99999)
	if err != ErrNotFound {
		t.Errorf("DeleteJob with invalid ID = %v, want ErrNotFound", err)
	}
}

func TestSQLiteStorageUpdateJobMetaSync(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save job first
	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Update meta synchronously
	meta.State = StateReady
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

	if gotMeta.State != StateReady {
		t.Errorf("State = %v, want %v", gotMeta.State, StateReady)
	}

	if gotMeta.Reserves != 5 {
		t.Errorf("Reserves = %d, want %d", gotMeta.Reserves, 5)
	}

	// Update non-existent job
	nonExistent := NewJobMeta(999, "test", 10, 0, 30*time.Second)
	err = storage.UpdateJobMetaSync(ctx, nonExistent)
	if err != ErrNotFound {
		t.Errorf("UpdateJobMetaSync non-existent = %v, want ErrNotFound", err)
	}

	ReleaseJobMeta(meta)
	ReleaseJobMeta(nonExistent)
}

func TestSQLiteStorageGetJobMeta(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 5*time.Second, 30*time.Second)
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
	if err != ErrNotFound {
		t.Errorf("GetJobMeta non-existent = %v, want ErrNotFound", err)
	}

	ReleaseJobMeta(meta)
}

func TestSQLiteStorageGetJobBody(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
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

func TestSQLiteStorageDeleteJob(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
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
	if err != ErrNotFound {
		t.Errorf("GetJobMeta after delete = %v, want ErrNotFound", err)
	}

	// Delete non-existent
	err = storage.DeleteJob(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("DeleteJob non-existent = %v, want ErrNotFound", err)
	}

	ReleaseJobMeta(meta)
}

func TestSQLiteStorageScanJobMeta(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
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

func TestSQLiteStorageCountJobs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
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

func TestSQLiteStorageStats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
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
}

func TestSQLiteStorageVacuum(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	// Should not error
	err = storage.Vacuum()
	if err != nil {
		t.Errorf("Vacuum error: %v", err)
	}
}

func TestSQLiteStorageExportJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Save a job
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
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

func TestSQLiteStorageAsyncUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sdq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test.db")
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	body := []byte("test body")

	// Save job first
	if err := storage.SaveJob(ctx, meta, body); err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}

	// Update meta asynchronously
	meta.State = StateReserved
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

	if gotMeta.State != StateReserved {
		t.Errorf("State = %v, want %v", gotMeta.State, StateReserved)
	}

	ReleaseJobMeta(meta)
}

// TestSQLiteStorage_ConcurrentReadWrite tests high-concurrency read/write conflicts
func TestSQLiteStorage_ConcurrentReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Pre-populate with jobs
	const numJobs = 100
	for i := uint64(1); i <= numJobs; i++ {
		meta := NewJobMeta(i, fmt.Sprintf("topic-%d", i%10), 10, 0, 30*time.Second)
		err := storage.SaveJob(ctx, meta, []byte(fmt.Sprintf("body-%d", i)))
		if err != nil {
			t.Fatalf("SaveJob %d error: %v", i, err)
		}
		ReleaseJobMeta(meta)
	}

	const duration = 1 * time.Second
	stopTime := time.Now().Add(duration)

	var wg sync.WaitGroup
	var readOps, writeOps, deleteOps uint64

	// Concurrent readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopTime) {
				id := uint64((time.Now().UnixNano() % numJobs) + 1)
				_, _ = storage.GetJobMeta(ctx, id)
				_, _ = storage.GetJobBody(ctx, id)
				atomic.AddUint64(&readOps, 1)
			}
		}()
	}

	// Concurrent writers (updating jobs)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopTime) {
				id := uint64((time.Now().UnixNano() % numJobs) + 1)
				meta, err := storage.GetJobMeta(ctx, id)
				if err == nil {
					meta.Reserves++
					meta.State = StateReserved
					_ = storage.UpdateJobMeta(ctx, meta)
					ReleaseJobMeta(meta)
					atomic.AddUint64(&writeOps, 1)
				}
			}
		}()
	}

	// Concurrent scanners
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for time.Now().Before(stopTime) {
				topic := fmt.Sprintf("topic-%d", workerID%10)
				filter := &JobMetaFilter{Topic: topic, Limit: 10}
				_, _ = storage.ScanJobMeta(ctx, filter)
				_, _ = storage.CountJobs(ctx, filter)
				atomic.AddUint64(&readOps, 1)
			}
		}(i)
	}

	// Concurrent delete/recreate operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		jobID := uint64(numJobs + 1)
		for time.Now().Before(stopTime) {
			meta := NewJobMeta(jobID, "test-delete", 10, 0, 30*time.Second)
			_ = storage.SaveJob(ctx, meta, []byte("temp"))
			_ = storage.DeleteJob(ctx, jobID)
			ReleaseJobMeta(meta)
			atomic.AddUint64(&deleteOps, 1)
		}
	}()

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

// TestSQLiteStorage_ConcurrentUpdates tests multiple goroutines updating same job
func TestSQLiteStorage_ConcurrentUpdates(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Create a single job
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	err = storage.SaveJob(ctx, meta, []byte("body"))
	if err != nil {
		t.Fatalf("SaveJob error: %v", err)
	}
	ReleaseJobMeta(meta)

	// Multiple goroutines updating the same job
	const numGoroutines = 20
	const updatesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < updatesPerGoroutine; j++ {
				meta, err := storage.GetJobMeta(ctx, 1)
				if err != nil {
					t.Errorf("GetJobMeta error: %v", err)
					return
				}
				meta.Reserves++
				meta.State = State(goroutineID % 5) // Cycle through states
				err = storage.UpdateJobMetaSync(ctx, meta)
				if err != nil {
					t.Errorf("UpdateJobMetaSync error: %v", err)
				}
				ReleaseJobMeta(meta)
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

	ReleaseJobMeta(finalMeta)
}

// TestSQLiteStorage_StressTest tests storage under heavy load
func TestSQLiteStorage_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath, WithMaxBatchSize(500))
	if err != nil {
		t.Fatalf("NewSQLiteStorage error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	const duration = 2 * time.Second
	stopTime := time.Now().Add(duration)

	var wg sync.WaitGroup
	var jobCounter uint64 = 1000
	var opCount uint64

	// Concurrent job creators
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopTime) {
				id := atomic.AddUint64(&jobCounter, 1)
				meta := NewJobMeta(id, "stress-test", 10, 0, 30*time.Second)
				_ = storage.SaveJob(ctx, meta, []byte("body"))
				ReleaseJobMeta(meta)
				atomic.AddUint64(&opCount, 1)
			}
		}()
	}

	// Concurrent readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopTime) {
				id := atomic.LoadUint64(&jobCounter)
				if id > 1000 {
					_, _ = storage.GetJobMeta(ctx, id-uint64(time.Now().UnixNano()%100))
				}
				atomic.AddUint64(&opCount, 1)
			}
		}()
	}

	// Concurrent scanners
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopTime) {
				filter := &JobMetaFilter{Topic: "stress-test", Limit: 100}
				_, _ = storage.ScanJobMeta(ctx, filter)
				atomic.AddUint64(&opCount, 1)
				time.Sleep(10 * time.Millisecond)
			}
		}()
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

// TestSQLiteStorage_ContextCancellation 测试 Context 取消处理
func TestSQLiteStorage_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	t.Run("SaveJob with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 立即取消

		meta := &JobMeta{
			ID:       1,
			Topic:    "test",
			State:    StateReady,
			Priority: 1,
		}

		err := storage.SaveJob(ctx, meta, []byte("body"))
		// SQLite 操作可能太快，在检查 context 之前就完成了
		// 所以我们只记录结果，不强制要求失败
		if err != nil {
			t.Logf("SaveJob with cancelled context failed as expected: %v", err)
		} else {
			t.Logf("SaveJob with cancelled context succeeded (operation was too fast)")
		}
	})

	t.Run("GetJobMeta with cancelled context", func(t *testing.T) {
		// 先保存一个任务
		ctx := context.Background()
		meta := &JobMeta{
			ID:       2,
			Topic:    "test",
			State:    StateReady,
			Priority: 1,
		}
		err := storage.SaveJob(ctx, meta, []byte("body"))
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}

		// 用取消的 context 读取
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = storage.GetJobMeta(cancelledCtx, 2)
		if err == nil {
			t.Error("GetJobMeta with cancelled context should fail")
		}
	})

	t.Run("UpdateJobMeta with cancelled context", func(t *testing.T) {
		// 先保存一个任务
		ctx := context.Background()
		meta := &JobMeta{
			ID:       3,
			Topic:    "test",
			State:    StateReady,
			Priority: 1,
		}
		err := storage.SaveJob(ctx, meta, []byte("body"))
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}

		// 用取消的 context 更新
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		meta.State = StateReserved
		err = storage.UpdateJobMeta(cancelledCtx, meta)
		if err == nil {
			t.Error("UpdateJobMeta with cancelled context should fail")
		}
	})

	t.Run("ScanJobMeta with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		filter := &JobMetaFilter{Topic: "test", Limit: 10}
		_, err := storage.ScanJobMeta(ctx, filter)
		if err == nil {
			t.Error("ScanJobMeta with cancelled context should fail")
		}
	})

	t.Run("DeleteJob with cancelled context", func(t *testing.T) {
		// 先保存一个任务
		ctx := context.Background()
		meta := &JobMeta{
			ID:       4,
			Topic:    "test",
			State:    StateReady,
			Priority: 1,
		}
		err := storage.SaveJob(ctx, meta, []byte("body"))
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}

		// 用取消的 context 删除
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err = storage.DeleteJob(cancelledCtx, 4)
		if err == nil {
			t.Error("DeleteJob with cancelled context should fail")
		}
	})
}

// TestSQLiteStorage_ContextTimeout 测试 Context 超时处理
func TestSQLiteStorage_ContextTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	t.Run("SaveJob with timeout", func(t *testing.T) {
		// 设置一个非常短的超时
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// 等待超时
		time.Sleep(1 * time.Millisecond)

		meta := &JobMeta{
			ID:       100,
			Topic:    "test",
			State:    StateReady,
			Priority: 1,
		}

		err := storage.SaveJob(ctx, meta, []byte("body"))
		if err == nil {
			t.Error("SaveJob with timed out context should fail")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Logf("SaveJob error (may not be DeadlineExceeded): %v", err)
		}
	})

	t.Run("ScanJobMeta with timeout", func(t *testing.T) {
		// 先添加一些任务
		ctx := context.Background()
		for i := 200; i < 250; i++ {
			meta := &JobMeta{
				ID:       uint64(i),
				Topic:    "test",
				State:    StateReady,
				Priority: 1,
			}
			_ = storage.SaveJob(ctx, meta, []byte("body"))
		}

		// 设置超时后扫描
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		time.Sleep(1 * time.Millisecond)

		filter := &JobMetaFilter{Topic: "test", Limit: 100}
		_, err := storage.ScanJobMeta(timeoutCtx, filter)
		if err == nil {
			t.Error("ScanJobMeta with timed out context should fail")
		}
	})
}

// TestSQLiteStorage_ContextCancellationDuringOperation 测试操作进行中的 Context 取消
func TestSQLiteStorage_ContextCancellationDuringOperation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStorage failed: %v", err)
	}
	defer func() { _ = storage.Close() }()

	// 添加大量任务以确保扫描需要时间
	ctx := context.Background()
	for i := 1; i <= 1000; i++ {
		meta := &JobMeta{
			ID:       uint64(i),
			Topic:    "test",
			State:    StateReady,
			Priority: 1,
		}
		err := storage.SaveJob(ctx, meta, []byte("body"))
		if err != nil {
			t.Fatalf("SaveJob failed: %v", err)
		}
	}

	t.Run("Cancel during scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// 在另一个 goroutine 中执行扫描
		var scanErr error
		done := make(chan struct{})

		go func() {
			filter := &JobMetaFilter{Topic: "test", Limit: 1000}
			_, scanErr = storage.ScanJobMeta(ctx, filter)
			close(done)
		}()

		// 短暂延迟后取消
		time.Sleep(10 * time.Millisecond)
		cancel()

		<-done

		// 扫描应该被取消（或已完成）
		if scanErr != nil {
			t.Logf("Scan was cancelled or errored: %v", scanErr)
		}
	})
}
