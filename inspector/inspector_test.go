package inspector

import (
	"context"
	"testing"
	"time"

	"go-slim.dev/sdq"
)

func TestInspector_Overview(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)

	// 空队列概览
	overview := inspector.Overview()
	if overview.TotalJobs != 0 {
		t.Errorf("expected 0 total jobs, got %d", overview.TotalJobs)
	}
	if overview.TotalTopics != 0 {
		t.Errorf("expected 0 topics, got %d", overview.TotalTopics)
	}

	// 添加任务
	for i := 0; i < 10; i++ {
		_, err := q.Put("topic-a", []byte("payload"), 1, 0, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		_, err := q.Put("topic-b", []byte("payload"), 1, time.Hour, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
	}

	overview = inspector.Overview()
	if overview.TotalJobs != 15 {
		t.Errorf("expected 15 total jobs, got %d", overview.TotalJobs)
	}
	if overview.ReadyJobs != 10 {
		t.Errorf("expected 10 ready jobs, got %d", overview.ReadyJobs)
	}
	if overview.DelayedJobs != 5 {
		t.Errorf("expected 5 delayed jobs, got %d", overview.DelayedJobs)
	}
	if overview.TotalTopics != 2 {
		t.Errorf("expected 2 topics, got %d", overview.TotalTopics)
	}
}

func TestInspector_ListTopics(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)

	// 添加任务到多个 topic
	_, _ = q.Put("email", []byte("payload"), 1, 0, 30*time.Second)
	_, _ = q.Put("email", []byte("payload"), 1, 0, 30*time.Second)
	_, _ = q.Put("sms", []byte("payload"), 1, 0, 30*time.Second)
	_, _ = q.Put("push", []byte("payload"), 1, time.Hour, 30*time.Second)

	topics := inspector.ListTopics()
	if len(topics) != 3 {
		t.Fatalf("expected 3 topics, got %d", len(topics))
	}

	// 检查按名称排序
	if topics[0].Name != "email" || topics[1].Name != "push" || topics[2].Name != "sms" {
		t.Errorf("topics not sorted by name: %v", topics)
	}

	// 检查 email topic
	var emailTopic *TopicInfo
	for _, tp := range topics {
		if tp.Name == "email" {
			emailTopic = tp
			break
		}
	}
	if emailTopic == nil {
		t.Fatal("email topic not found")
		return
	}
	if emailTopic.ReadyJobs != 2 {
		t.Errorf("expected 2 ready jobs in email topic, got %d", emailTopic.ReadyJobs)
	}
}

func TestInspector_GetTopic(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)

	// 不存在的 topic
	_, err = inspector.GetTopic("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent topic")
	}

	// 添加任务
	_, _ = q.Put("test-topic", []byte("payload"), 1, 0, 30*time.Second)
	_, _ = q.Put("test-topic", []byte("payload"), 1, time.Hour, 30*time.Second)

	topic, err := inspector.GetTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	if topic.Name != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got '%s'", topic.Name)
	}
	if topic.ReadyJobs != 1 {
		t.Errorf("expected 1 ready job, got %d", topic.ReadyJobs)
	}
	if topic.DelayedJobs != 1 {
		t.Errorf("expected 1 delayed job, got %d", topic.DelayedJobs)
	}
}

func TestInspector_ListJobs(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)
	ctx := context.Background()

	// 添加任务
	for i := 0; i < 25; i++ {
		_, err := q.Put("test", []byte("payload"), uint32(i%5), 0, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
	}

	// 测试分页
	result, err := inspector.ListJobs(ctx, &JobQuery{
		Page:     1,
		PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Jobs) != 10 {
		t.Errorf("expected 10 jobs, got %d", len(result.Jobs))
	}
	if result.Total != 25 {
		t.Errorf("expected total 25, got %d", result.Total)
	}
	if result.TotalPages != 3 {
		t.Errorf("expected 3 pages, got %d", result.TotalPages)
	}
	if !result.HasMore {
		t.Error("expected HasMore to be true")
	}

	// 第二页
	result, err = inspector.ListJobs(ctx, &JobQuery{
		Page:     2,
		PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Jobs) != 10 {
		t.Errorf("expected 10 jobs, got %d", len(result.Jobs))
	}

	// 最后一页
	result, err = inspector.ListJobs(ctx, &JobQuery{
		Page:     3,
		PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Jobs) != 5 {
		t.Errorf("expected 5 jobs, got %d", len(result.Jobs))
	}
	if result.HasMore {
		t.Error("expected HasMore to be false")
	}
}

func TestInspector_ListJobsByState(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)

	// 添加各种状态的任务
	_, _ = q.Put("test", []byte("ready1"), 1, 0, 30*time.Second)
	_, _ = q.Put("test", []byte("ready2"), 1, 0, 30*time.Second)
	_, _ = q.Put("test", []byte("delayed"), 1, time.Hour, 30*time.Second)

	// 保留一个任务并 bury
	job, _ := q.Reserve([]string{"test"}, sdq.TestTimeout(1*time.Second))
	if job != nil {
		_ = job.Bury(1)
	}

	// 使用 Overview 来验证状态（内存队列中的真实状态）
	overview := inspector.Overview()

	if overview.ReadyJobs != 1 {
		t.Errorf("expected 1 ready job, got %d", overview.ReadyJobs)
	}

	if overview.DelayedJobs != 1 {
		t.Errorf("expected 1 delayed job, got %d", overview.DelayedJobs)
	}

	if overview.BuriedJobs != 1 {
		t.Errorf("expected 1 buried job, got %d", overview.BuriedJobs)
	}

	if overview.TotalJobs != 3 {
		t.Errorf("expected 3 total jobs, got %d", overview.TotalJobs)
	}
}

func TestInspector_GetJob(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)
	ctx := context.Background()

	// 添加任务
	id, _ := q.Put("test", []byte("test payload"), 5, 0, 30*time.Second)

	// 获取任务详情
	info, err := inspector.GetJob(ctx, id, false)
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != id {
		t.Errorf("expected id %d, got %d", id, info.ID)
	}
	if info.Topic != "test" {
		t.Errorf("expected topic 'test', got '%s'", info.Topic)
	}
	if info.Priority != 5 {
		t.Errorf("expected priority 5, got %d", info.Priority)
	}
	if info.State != "ready" {
		t.Errorf("expected state 'ready', got '%s'", info.State)
	}

	// 获取带 Body 大小
	info, err = inspector.GetJob(ctx, id, true)
	if err != nil {
		t.Fatal(err)
	}
	if info.BodySize != len("test payload") {
		t.Errorf("expected body size %d, got %d", len("test payload"), info.BodySize)
	}
}

func TestInspector_GetJobBody(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)
	ctx := context.Background()

	// 添加任务
	payload := []byte("test payload content")
	id, _ := q.Put("test", payload, 1, 0, 30*time.Second)

	// 获取 Body
	body, err := inspector.GetJobBody(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != string(payload) {
		t.Errorf("expected body '%s', got '%s'", payload, body)
	}
}

func TestInspector_TakeSnapshot(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)

	// 添加一些任务
	_, _ = q.Put("topic-a", []byte("payload"), 1, 0, 30*time.Second)
	_, _ = q.Put("topic-b", []byte("payload"), 1, 0, 30*time.Second)

	snapshot := inspector.TakeSnapshot()
	if snapshot.Overview == nil {
		t.Fatal("snapshot overview is nil")
	}
	if snapshot.Topics == nil {
		t.Fatal("snapshot topics is nil")
	}
	if snapshot.Overview.TotalJobs != 2 {
		t.Errorf("expected 2 total jobs, got %d", snapshot.Overview.TotalJobs)
	}
	if len(snapshot.Topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(snapshot.Topics))
	}
}

func TestInspector_Watch(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	ch := inspector.Watch(ctx, &WatchOptions{
		Interval: 100 * time.Millisecond,
	})

	snapshots := make([]*Snapshot, 0)
	for snapshot := range ch {
		snapshots = append(snapshots, snapshot)
	}

	// 应该收到多个快照
	if len(snapshots) < 2 {
		t.Errorf("expected at least 2 snapshots, got %d", len(snapshots))
	}
}

func TestInspector_KickAllBuriedJobs(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	// 等待恢复完成
	if err := q.WaitForRecovery(sdq.TestTimeout(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)
	ctx := context.Background()

	// 添加任务并逐个 bury
	for i := 0; i < 5; i++ {
		_, _ = q.Put("test", []byte("payload"), 1, 0, 30*time.Second)
		job, err := q.Reserve([]string{"test"}, sdq.TestTimeout(1*time.Second))
		if err != nil {
			t.Fatalf("reserve failed: %v", err)
		}
		if err := job.Bury(1); err != nil {
			t.Fatalf("bury failed: %v", err)
		}
	}

	// 验证 buried 任务数量
	overview := inspector.Overview()
	if overview.BuriedJobs != 5 {
		t.Errorf("expected 5 buried jobs, got %d", overview.BuriedJobs)
	}

	// 踢出所有
	kicked, err := inspector.KickAllBuriedJobs(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if kicked != 5 {
		t.Errorf("expected 5 kicked, got %d", kicked)
	}

	// 验证没有 buried 任务了
	overview = inspector.Overview()
	if overview.BuriedJobs != 0 {
		t.Errorf("expected 0 buried jobs, got %d", overview.BuriedJobs)
	}
	if overview.ReadyJobs != 5 {
		t.Errorf("expected 5 ready jobs, got %d", overview.ReadyJobs)
	}
}

func TestInspector_DeleteAllBuriedJobs(t *testing.T) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = sdq.NewNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	if err := q.Start(); err != nil {
		t.Fatal(err)
	}

	// 等待恢复完成
	if err := q.WaitForRecovery(sdq.TestTimeout(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	inspector := New(q)
	ctx := context.Background()

	// 添加任务并逐个 bury
	for i := 0; i < 3; i++ {
		_, _ = q.Put("test", []byte("payload"), 1, 0, 30*time.Second)
		job, err := q.Reserve([]string{"test"}, sdq.TestTimeout(1*time.Second))
		if err != nil {
			t.Fatalf("reserve failed: %v", err)
		}
		if err := job.Bury(1); err != nil {
			t.Fatalf("bury failed: %v", err)
		}
	}

	// 验证 buried 任务数量
	overview := inspector.Overview()
	if overview.BuriedJobs != 3 {
		t.Errorf("expected 3 buried jobs, got %d", overview.BuriedJobs)
	}

	// 删除所有
	deleted, err := inspector.DeleteAllBuriedJobs(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Errorf("expected 3 deleted, got %d", deleted)
	}

	// 验证没有任务了
	overview = inspector.Overview()
	if overview.TotalJobs != 0 {
		t.Errorf("expected 0 total jobs, got %d", overview.TotalJobs)
	}
}

func TestThroughputTracker(t *testing.T) {
	tracker := NewThroughputTracker()

	// 记录操作
	for i := 0; i < 100; i++ {
		tracker.RecordPut()
	}
	for i := 0; i < 50; i++ {
		tracker.RecordReserve()
	}
	for i := 0; i < 30; i++ {
		tracker.RecordDelete()
	}

	// 等待一小段时间
	time.Sleep(100 * time.Millisecond)

	// 计算吞吐量
	stats := tracker.Calculate()
	if stats.PutsPerSecond <= 0 {
		t.Error("expected positive puts per second")
	}
	if stats.ReservesPerSecond <= 0 {
		t.Error("expected positive reserves per second")
	}
	if stats.DeletesPerSecond <= 0 {
		t.Error("expected positive deletes per second")
	}

	// 获取统计
	stats = tracker.Stats()
	if stats == nil {
		t.Fatal("stats should not be nil")
	}
}
