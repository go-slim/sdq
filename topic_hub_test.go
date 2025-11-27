package sdq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// noopTicker is a no-op implementation of Ticker for testing
// It avoids the deadlock issues in DynamicSleepTicker and TimeWheelTicker
type noopTicker struct{}

func (t *noopTicker) Start()                                  {}
func (t *noopTicker) Stop()                                   {}
func (t *noopTicker) Register(name string, tickable Tickable) {}
func (t *noopTicker) Unregister(name string)                  {}
func (t *noopTicker) Wakeup()                                 {}
func (t *noopTicker) Stats() *TickerStats {
	return &TickerStats{Mode: "noop"}
}

func newNoopTicker() Ticker {
	return &noopTicker{}
}

func TestNewTopicHub(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicHub(&config, testStorage.Storage, ticker)
		if hub == nil {
			t.Fatal("newTopicHub returned nil")
		}

		if hub.topics == nil {
			t.Error("topics map should be initialized")
		}

		if hub.topicWrappers == nil {
			t.Error("topicWrappers map should be initialized")
		}
	})
}

func TestTopicHubGetOrCreateTopic(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Create new topic
	hub.Lock()
	topic, err := hub.GetOrCreateTopic("test")
	hub.Unlock()

	if err != nil {
		t.Fatalf("GetOrCreateTopic error: %v", err)
	}

	if topic == nil {
		t.Fatal("GetOrCreateTopic returned nil")
	}

	if topic.name != "test" {
		t.Errorf("topic name = %s, want test", topic.name)
	}

	// Get existing topic
	hub.Lock()
	topic2, err := hub.GetOrCreateTopic("test")
	hub.Unlock()

	if err != nil {
		t.Fatalf("GetOrCreateTopic second call error: %v", err)
	}

	if topic2 != topic {
		t.Error("Should return the same topic instance")
	}
}

func TestTopicHubMaxTopics(t *testing.T) {
	config := DefaultConfig()
	config.MaxTopics = 2
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Create topics up to limit
	hub.Lock()
	_, err := hub.GetOrCreateTopic("topic1")
	hub.Unlock()
	if err != nil {
		t.Fatalf("GetOrCreateTopic topic1 error: %v", err)
	}

	hub.Lock()
	_, err = hub.GetOrCreateTopic("topic2")
	hub.Unlock()
	if err != nil {
		t.Fatalf("GetOrCreateTopic topic2 error: %v", err)
	}

	// Try to create one more
	hub.Lock()
	_, err = hub.GetOrCreateTopic("topic3")
	hub.Unlock()
	if err != ErrMaxTopicsReached {
		t.Errorf("GetOrCreateTopic topic3 = %v, want ErrMaxTopicsReached", err)
	}
}

func TestTopicHubGetTopic(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Get non-existent topic
	hub.Lock()
	topic := hub.GetTopic("test")
	hub.Unlock()

	if topic != nil {
		t.Error("GetTopic should return nil for non-existent topic")
	}

	// Create and get topic
	hub.Lock()
	_, _ = hub.GetOrCreateTopic("test")
	topic = hub.GetTopic("test")
	hub.Unlock()

	if topic == nil {
		t.Error("GetTopic should return existing topic")
	}
}

func TestTopicHubListTopics(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Empty hub
	topics := hub.ListTopics()
	if len(topics) != 0 {
		t.Errorf("ListTopics = %v, want empty", topics)
	}

	// Add topics
	hub.Lock()
	_, _ = hub.GetOrCreateTopic("beta")
	_, _ = hub.GetOrCreateTopic("alpha")
	_, _ = hub.GetOrCreateTopic("gamma")
	hub.Unlock()

	topics = hub.ListTopics()
	if len(topics) != 3 {
		t.Errorf("ListTopics count = %d, want 3", len(topics))
	}

	// Should be sorted
	if topics[0] != "alpha" || topics[1] != "beta" || topics[2] != "gamma" {
		t.Errorf("ListTopics not sorted: %v", topics)
	}
}

func TestTopicHubTopicStats(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Non-existent topic
	stats := hub.TopicStats("test")
	if stats != nil {
		t.Error("TopicStats should return nil for non-existent topic")
	}

	// Create topic and add jobs
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateReady
	topic.pushReady(meta)
	hub.Unlock()

	stats = hub.TopicStats("test")
	if stats == nil {
		t.Fatal("TopicStats should return stats for existing topic")
	}

	if stats.ReadyJobs != 1 {
		t.Errorf("ReadyJobs = %d, want 1", stats.ReadyJobs)
	}
}

func TestTopicHubAllTopicStats(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Empty hub
	allStats := hub.AllTopicStats()
	if len(allStats) != 0 {
		t.Errorf("AllTopicStats = %v, want empty", allStats)
	}

	// Add topics with jobs
	hub.Lock()
	topic1, _ := hub.GetOrCreateTopic("topic1")
	meta1 := NewJobMeta(1, "topic1", 10, 0, 30*time.Second)
	meta1.State = StateReady
	topic1.pushReady(meta1)

	topic2, _ := hub.GetOrCreateTopic("topic2")
	meta2 := NewJobMeta(2, "topic2", 10, 0, 30*time.Second)
	meta2.State = StateDelayed
	topic2.pushDelayed(meta2)
	hub.Unlock()

	allStats = hub.AllTopicStats()
	if len(allStats) != 2 {
		t.Errorf("AllTopicStats count = %d, want 2", len(allStats))
	}
}

func TestTopicHubTotalJobs(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Empty hub
	total := hub.TotalJobs()
	if total != 0 {
		t.Errorf("TotalJobs = %d, want 0", total)
	}

	// Add jobs
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	topic.pushReady(meta1)
	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	topic.pushDelayed(meta2)
	hub.Unlock()

	total = hub.TotalJobs()
	if total != 2 {
		t.Errorf("TotalJobs = %d, want 2", total)
	}
}

func TestTopicHubValidateTopicName(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	tests := []struct {
		name    string
		wantErr error
	}{
		{"", ErrTopicRequired},
		{"valid", nil},
		{"valid_name", nil},
		{"valid-name", nil},
		{"Valid123", nil},
		{"name with space", ErrInvalidTopic},
		{"name!special", ErrInvalidTopic},
		{string(make([]byte, 201)), ErrInvalidTopic}, // Too long
	}

	for _, tt := range tests {
		err := hub.ValidateTopicName(tt.name)
		if err != tt.wantErr {
			t.Errorf("ValidateTopicName(%q) = %v, want %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestTopicHubPut(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicHub(&config, testStorage.Storage, ticker)

		// Put ready job
		meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		needsNotify, err := hub.Put("test", meta)
		if err != nil {
			t.Fatalf("Put error: %v", err)
		}

		if !needsNotify {
			t.Error("Put ready job should notify")
		}

		// Put delayed job
		meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
		meta2.ReadyAt = time.Now().Add(10 * time.Second)
		needsNotify, err = hub.Put("test", meta2)
		if err != nil {
			t.Fatalf("Put delayed error: %v", err)
		}

		if needsNotify {
			t.Error("Put delayed job should not notify")
		}
	})
}

func TestTopicHubPutMaxJobs(t *testing.T) {
	config := DefaultConfig()
	config.MaxJobsPerTopic = 2
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Add jobs up to limit
	meta1 := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	_, _ = hub.Put("test", meta1)

	meta2 := NewJobMeta(2, "test", 10, 0, 30*time.Second)
	_, _ = hub.Put("test", meta2)

	// Try to add one more
	meta3 := NewJobMeta(3, "test", 10, 0, 30*time.Second)
	_, err := hub.Put("test", meta3)
	if err != ErrMaxJobsReached {
		t.Errorf("Put over limit = %v, want ErrMaxJobsReached", err)
	}
}

func TestTopicHubTryReserve(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicHub(&config, testStorage.Storage, ticker)

		// No jobs
		meta := hub.TryReserve([]string{"test"})
		if meta != nil {
			t.Error("TryReserve should return nil when no jobs")
		}

		// Add ready job
		hub.Lock()
		topic, _ := hub.GetOrCreateTopic("test")
		jobMeta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		jobMeta.State = StateReady
		topic.pushReady(jobMeta)
		hub.Unlock()

		// Reserve job
		meta = hub.TryReserve([]string{"test"})
		if meta == nil {
			t.Fatal("TryReserve should return job")
		}

		if meta.State != StateReserved {
			t.Errorf("State = %v, want StateReserved", meta.State)
		}

		if meta.Reserves != 1 {
			t.Errorf("Reserves = %d, want 1", meta.Reserves)
		}
	})
}

func TestTopicHubDelete(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicHub(&config, testStorage.Storage, ticker)

		// Add and reserve job
		hub.Lock()
		topic, _ := hub.GetOrCreateTopic("test")
		meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.State = StateReserved
		topic.addReserved(meta)
		hub.Unlock()

		// Save to storage
		_ = testStorage.Storage.SaveJob(context.Background(), meta, []byte("body"))

		// Delete job
		err := hub.Delete(1)
		if err != nil {
			t.Fatalf("Delete error: %v", err)
		}

		// Verify deletion
		hub.Lock()
		foundMeta, _ := hub.FindJob(1)
		hub.Unlock()

		if foundMeta != nil {
			t.Error("Job should be deleted")
		}
	})
}

func TestTopicHubDeleteNotReserved(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Add ready job (not reserved)
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateReady
	topic.pushReady(meta)
	hub.Unlock()

	// Try to delete
	err := hub.Delete(1)
	if err != ErrInvalidState {
		t.Errorf("Delete not reserved = %v, want ErrInvalidState", err)
	}
}

func TestTopicHubRelease(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		ticker := newNoopTicker()

		hub := newTopicHub(&config, testStorage.Storage, ticker)

		// Add reserved job
		hub.Lock()
		topic, _ := hub.GetOrCreateTopic("test")
		meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
		meta.State = StateReserved
		topic.addReserved(meta)
		hub.Unlock()

		// Release without delay
		topicName, needsNotify, err := hub.Release(1, 5, 0)
		if err != nil {
			t.Fatalf("Release error: %v", err)
		}

		if topicName != "test" {
			t.Errorf("topicName = %s, want test", topicName)
		}

		if !needsNotify {
			t.Error("Release without delay should notify")
		}

		// Check job is in ready queue
		hub.Lock()
		foundMeta, _ := hub.FindJob(1)
		hub.Unlock()

		if foundMeta.State != StateReady {
			t.Errorf("State = %v, want StateReady", foundMeta.State)
		}

		if foundMeta.Priority != 5 {
			t.Errorf("Priority = %d, want 5", foundMeta.Priority)
		}
	})
}

func TestTopicHubReleaseWithDelay(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Add reserved job
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateReserved
	topic.addReserved(meta)
	hub.Unlock()

	// Release with delay
	_, needsNotify, err := hub.Release(1, 5, 10*time.Second)
	if err != nil {
		t.Fatalf("Release error: %v", err)
	}

	if needsNotify {
		t.Error("Release with delay should not notify")
	}

	// Check job is in delayed queue
	hub.Lock()
	foundMeta, _ := hub.FindJob(1)
	hub.Unlock()

	if foundMeta.State != StateDelayed {
		t.Errorf("State = %v, want StateDelayed", foundMeta.State)
	}
}

func TestTopicHubBury(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Add reserved job
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateReserved
	topic.addReserved(meta)
	hub.Unlock()

	// Bury job
	err := hub.Bury(1, 20)
	if err != nil {
		t.Fatalf("Bury error: %v", err)
	}

	// Check job is buried
	hub.Lock()
	foundMeta, _ := hub.FindJob(1)
	hub.Unlock()

	if foundMeta.State != StateBuried {
		t.Errorf("State = %v, want StateBuried", foundMeta.State)
	}

	if foundMeta.Priority != 20 {
		t.Errorf("Priority = %d, want 20", foundMeta.Priority)
	}

	if foundMeta.Buries != 1 {
		t.Errorf("Buries = %d, want 1", foundMeta.Buries)
	}
}

func TestTopicHubKick(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Add buried jobs
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	for i := uint64(1); i <= 5; i++ {
		meta := NewJobMeta(i, "test", uint32(i), 0, 30*time.Second)
		meta.State = StateBuried
		topic.pushBuried(meta)
	}
	hub.Unlock()

	// Kick 3 jobs
	kicked, needsNotify, err := hub.Kick("test", 3)
	if err != nil {
		t.Fatalf("Kick error: %v", err)
	}

	if kicked != 3 {
		t.Errorf("kicked = %d, want 3", kicked)
	}

	if !needsNotify {
		t.Error("Kick should notify")
	}

	// Check stats
	stats := hub.TopicStats("test")
	if stats.BuriedJobs != 2 {
		t.Errorf("BuriedJobs = %d, want 2", stats.BuriedJobs)
	}

	if stats.ReadyJobs != 3 {
		t.Errorf("ReadyJobs = %d, want 3", stats.ReadyJobs)
	}
}

func TestTopicHubKickJob(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Add buried job
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")
	meta := NewJobMeta(1, "test", 10, 0, 30*time.Second)
	meta.State = StateBuried
	topic.pushBuried(meta)
	hub.Unlock()

	// Kick specific job
	topicName, err := hub.KickJob(1)
	if err != nil {
		t.Fatalf("KickJob error: %v", err)
	}

	if topicName != "test" {
		t.Errorf("topicName = %s, want test", topicName)
	}

	// Check job is ready
	hub.Lock()
	foundMeta, _ := hub.FindJob(1)
	hub.Unlock()

	if foundMeta.State != StateReady {
		t.Errorf("State = %v, want StateReady", foundMeta.State)
	}

	if foundMeta.Kicks != 1 {
		t.Errorf("Kicks = %d, want 1", foundMeta.Kicks)
	}
}

func TestTopicHubFindJob(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Add jobs in different states
	hub.Lock()
	topic, _ := hub.GetOrCreateTopic("test")

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

	// Find each job
	foundMeta, foundTopic := hub.FindJob(1)
	if foundMeta == nil || foundTopic == nil {
		t.Error("Should find ready job")
	}

	foundMeta, foundTopic = hub.FindJob(2)
	if foundMeta == nil || foundTopic == nil {
		t.Error("Should find delayed job")
	}

	foundMeta, foundTopic = hub.FindJob(3)
	if foundMeta == nil || foundTopic == nil {
		t.Error("Should find reserved job")
	}

	foundMeta, foundTopic = hub.FindJob(4)
	if foundMeta == nil || foundTopic == nil {
		t.Error("Should find buried job")
	}

	// Find non-existent
	foundMeta, foundTopic = hub.FindJob(999)
	hub.Unlock()

	if foundMeta != nil || foundTopic != nil {
		t.Error("Should not find non-existent job")
	}
}

func TestTopicHubCleanupEmptyTopics(t *testing.T) {
	config := DefaultConfig()
	ticker := newNoopTicker()

	hub := newTopicHub(&config, nil, ticker)

	// Add topics
	hub.Lock()
	_, _ = hub.GetOrCreateTopic("empty1")
	_, _ = hub.GetOrCreateTopic("empty2")
	topic3, _ := hub.GetOrCreateTopic("notempty")
	meta := NewJobMeta(1, "notempty", 10, 0, 30*time.Second)
	topic3.pushReady(meta)
	hub.Unlock()

	// Cleanup
	cleaned := hub.CleanupEmptyTopics()
	if cleaned != 2 {
		t.Errorf("cleaned = %d, want 2", cleaned)
	}

	topics := hub.ListTopics()
	if len(topics) != 1 {
		t.Errorf("topics count = %d, want 1", len(topics))
	}

	if topics[0] != "notempty" {
		t.Errorf("remaining topic = %s, want notempty", topics[0])
	}
}

// TestStress_ManyTopics 大量 Topic 测试
func TestStress_ManyTopics(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker() // 使用 NoOpTicker 避免死锁
		config.MaxTopics = 10000

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const topicCount = 1000

		startTime := time.Now()

		// 创建大量 topic，每个 topic 1 个任务
		var wg sync.WaitGroup
		for i := range topicCount {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				_, _ = q.Put(fmt.Sprintf("topic-%d", id), []byte("job"), 1, 0, 5*time.Second)
			}(i)
		}
		wg.Wait()

		createTime := time.Since(startTime)
		t.Logf("创建 %d 个 topic 耗时: %v", topicCount, createTime)

		// 统计
		stats := q.Stats()
		if stats.Topics != topicCount {
			t.Errorf("期望 %d 个 topic，实际 %d", topicCount, stats.Topics)
		}

		t.Logf("Topic 统计: %d topics, %d jobs", stats.Topics, stats.TotalJobs)
	})
}

// TestTopicHub_ConcurrentCreate 测试多 goroutine 同时创建同名 Topic
func TestTopicHub_ConcurrentCreate(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const numGoroutines = 100
		const sameTopic = "concurrent-topic"

		var wg sync.WaitGroup
		successCount := make([]int, numGoroutines)

		// 100 个 goroutine 同时尝试在同一个 topic 中创建任务
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				id, err := q.Put(sameTopic, []byte(fmt.Sprintf("job-%d", idx)), 1, 0, 30*time.Second)
				if err == nil && id != 0 {
					successCount[idx] = 1
				}
			}(i)
		}

		wg.Wait()

		// 统计成功创建的任务数
		total := 0
		for _, count := range successCount {
			total += count
		}

		if total != numGoroutines {
			t.Errorf("Expected %d jobs created, got %d", numGoroutines, total)
		}

		// 验证 topic 统计
		stats, err := q.StatsTopic(sameTopic)
		if err != nil {
			t.Fatalf("StatsTopic error: %v", err)
		}
		if stats == nil {
			t.Fatal("Topic stats should not be nil")
		}

		if stats.TotalJobs != numGoroutines {
			t.Errorf("TotalJobs = %d, want %d", stats.TotalJobs, numGoroutines)
		}
	})
}

// TestTopicHub_ConcurrentOperations 测试 Topic 的并发操作
func TestTopicHub_ConcurrentOperations(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const numTopics = 10
		const jobsPerTopic = 50

		var wg sync.WaitGroup

		// 并发创建多个 topics 和 jobs
		for i := 0; i < numTopics; i++ {
			topic := fmt.Sprintf("topic-%d", i)
			for j := 0; j < jobsPerTopic; j++ {
				wg.Add(1)
				go func(t string, idx int) {
					defer wg.Done()
					_, _ = q.Put(t, []byte(fmt.Sprintf("job-%d", idx)), uint32(idx%10), 0, 30*time.Second)
				}(topic, j)
			}
		}

		wg.Wait()

		// 验证所有 topics 都被创建
		topics := q.ListTopics()
		if len(topics) != numTopics {
			t.Errorf("Created topics = %d, want %d", len(topics), numTopics)
		}

		// 验证总任务数
		stats := q.Stats()
		expectedJobs := numTopics * jobsPerTopic
		if stats.TotalJobs != expectedJobs {
			t.Errorf("TotalJobs = %d, want %d", stats.TotalJobs, expectedJobs)
		}
	})
}

// TestTopicHub_ConcurrentReadWrite 测试并发读写操作
func TestTopicHub_ConcurrentReadWrite(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, testStorage *TestStorage) {
		config := DefaultConfig()
		config.Storage = testStorage.Storage
		config.Ticker = NewNoOpTicker()

		q, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = q.Stop() }()

		if err := q.Start(); err != nil {
			t.Fatal(err)
		}

		const duration = 2 * time.Second
		stopChan := make(chan struct{})

		var wg sync.WaitGroup
		var putCount, reserveCount sync.Map

		// Writer goroutines
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				count := 0
				for {
					select {
					case <-stopChan:
						putCount.Store(id, count)
						return
					default:
						_, _ = q.Put("concurrent-test", []byte("data"), 1, 0, 30*time.Second)
						count++
						time.Sleep(time.Millisecond)
					}
				}
			}(i)
		}

		// Reader goroutines
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				count := 0
				for {
					select {
					case <-stopChan:
						reserveCount.Store(id, count)
						return
					default:
						job, err := q.Reserve([]string{"concurrent-test"}, 10*time.Millisecond)
						if err == nil && job != nil {
							_ = job.Delete()
							count++
						}
					}
				}
			}(i)
		}

		// 运行指定时间
		time.Sleep(duration)
		close(stopChan)
		wg.Wait()

		// 统计总数
		totalPut := 0
		putCount.Range(func(_, value interface{}) bool {
			totalPut += value.(int)
			return true
		})

		totalReserve := 0
		reserveCount.Range(func(_, value interface{}) bool {
			totalReserve += value.(int)
			return true
		})

		t.Logf("Concurrent operations: Put=%d, Reserve=%d", totalPut, totalReserve)

		// 验证没有 panic 或 deadlock
		if totalPut == 0 {
			t.Error("No jobs were put")
		}
	})
}
