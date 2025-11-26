package sdq

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
