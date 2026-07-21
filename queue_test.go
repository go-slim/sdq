package sdq

import (
	"context"
	"errors"
	"testing"
	"time"
)

type saveErrorStorage struct {
	*memoryStorage
	err error
}

func (s *saveErrorStorage) SaveJob(context.Context, *JobMeta, []byte) error {
	return s.err
}

// newTestQueue 创建一个用于测试的 Queue
func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	config := DefaultConfig()
	config.Ticker = &noOpTicker{}
	config.Storage = newMemoryStorage()

	q, err := New(config)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if err := q.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	t.Cleanup(func() {
		_ = q.Stop()
	})

	return q
}

func TestNew(t *testing.T) {
	config := DefaultConfig()
	config.Ticker = &noOpTicker{}
	config.Storage = newMemoryStorage()

	q, err := New(config)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer func() { _ = q.Stop() }()

	if q == nil {
		t.Fatal("New() returned nil")
	}
}

func TestQueue_StartStop(t *testing.T) {
	config := DefaultConfig()
	config.Ticker = &noOpTicker{}
	config.Storage = newMemoryStorage()

	q, err := New(config)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if err := q.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	if err := q.Stop(); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}
}

func TestQueue_PutRequiresSuccessfulStart(t *testing.T) {
	config := DefaultConfig()
	config.Ticker = &noOpTicker{}
	config.Storage = newMemoryStorage()

	q, err := New(config)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	t.Cleanup(func() { _ = q.Stop() })

	if _, err := q.Put("test-topic", []byte("before start"), 10, 0, time.Minute); err != ErrQueueNotStarted {
		t.Fatalf("Put() before Start error = %v, want ErrQueueNotStarted", err)
	}
	if err := q.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	if _, err := q.Put("test-topic", []byte("after start"), 10, 0, time.Minute); err != nil {
		t.Fatalf("Put() after Start error: %v", err)
	}
}

func TestQueue_Put(t *testing.T) {
	q := newTestQueue(t)

	id, err := q.Put("test-topic", []byte("hello"), 10, 0, 30*time.Second)
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	if id == 0 {
		t.Error("Put() returned id = 0")
	}
}

func TestQueue_Put_InvalidTopic(t *testing.T) {
	q := newTestQueue(t)

	_, err := q.Put("", []byte("hello"), 10, 0, 30*time.Second)
	if err != ErrInvalidTopic {
		t.Errorf("Put() with empty topic = %v, want ErrInvalidTopic", err)
	}
}

func TestQueue_PutDoesNotEnqueueWhenStorageSaveFails(t *testing.T) {
	saveErr := errors.New("save failed")
	config := DefaultConfig()
	config.Ticker = &noOpTicker{}
	config.Storage = &saveErrorStorage{
		memoryStorage: newMemoryStorage(),
		err:           saveErr,
	}

	q, err := New(config)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if err := q.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	t.Cleanup(func() { _ = q.Stop() })

	id, err := q.Put("test-topic", []byte("body"), 10, 0, time.Minute)
	if !errors.Is(err, saveErr) {
		t.Fatalf("Put() error = %v, want %v", err, saveErr)
	}
	if id != 0 {
		t.Fatalf("Put() ID = %d, want 0", id)
	}
	if meta := q.TryReserve([]string{"test-topic"}); meta != nil {
		t.Fatalf("TryReserve() returned unsaved job %#v", meta)
	}
}

func TestQueue_PutRollsBackStorageWhenTopicRejectsJob(t *testing.T) {
	storage := newMemoryStorage()
	config := DefaultConfig()
	config.MaxTopics = 1
	config.Ticker = &noOpTicker{}
	config.Storage = storage

	q, err := New(config)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if err := q.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	t.Cleanup(func() { _ = q.Stop() })

	if _, err := q.Put("accepted", []byte("first"), 10, 0, time.Minute); err != nil {
		t.Fatalf("first Put() error: %v", err)
	}
	if _, err := q.Put("rejected", []byte("second"), 10, 0, time.Minute); err != ErrMaxTopicsReached {
		t.Fatalf("second Put() error = %v, want ErrMaxTopicsReached", err)
	}

	// ID 2 was allocated before the Topic limit rejected it. It must not remain in
	// Storage and unexpectedly reappear during the next startup recovery.
	if _, err := storage.GetJobMeta(context.Background(), 2); err != ErrNotFound {
		t.Fatalf("rejected job storage lookup error = %v, want ErrNotFound", err)
	}
}

func TestQueue_TryReserve(t *testing.T) {
	q := newTestQueue(t)

	id, _ := q.Put("test-topic", []byte("hello"), 10, 0, 30*time.Second)

	meta := q.TryReserve([]string{"test-topic"})
	if meta == nil {
		t.Fatal("TryReserve() returned nil")
		return // This line helps linter understand that meta is non-nil after this point
	}

	if meta.ID != id {
		t.Errorf("ID = %d, want %d", meta.ID, id)
	}

	if meta.State != StateReserved {
		t.Errorf("State = %v, want %v", meta.State, StateReserved)
	}
}

func TestTopicProcessTickPersistsDelayedTransition(t *testing.T) {
	q := newTestQueue(t)
	storage := q.storage.(*memoryStorage)

	id, err := q.Put("delayed-topic", []byte("body"), 10, time.Hour, time.Minute)
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	topic := testTopic(t, q, "delayed-topic")
	topic.ProcessTick(time.Now().Add(2 * time.Hour))

	meta, err := storage.GetJobMeta(context.Background(), id)
	if err != nil {
		t.Fatalf("GetJobMeta() error: %v", err)
	}
	if meta.State != StateReady {
		t.Fatalf("persisted state = %v, want Ready", meta.State)
	}
}

func TestTopicProcessTickNotifiesReserveWaiter(t *testing.T) {
	q := newTestQueue(t)

	id, err := q.Put("delayed-topic", []byte("body"), 10, time.Hour, time.Minute)
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	type reserveResult struct {
		job *Job
		err error
	}
	resultCh := make(chan reserveResult, 1)
	go func() {
		job, err := q.Reserve([]string{"delayed-topic"}, time.Second)
		resultCh <- reserveResult{job: job, err: err}
	}()

	deadline := time.Now().Add(500 * time.Millisecond)
	for q.reserveMgr.stats()["delayed-topic"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Reserve waiter was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	testTopic(t, q, "delayed-topic").ProcessTick(time.Now().Add(2 * time.Hour))

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("Reserve() error: %v", result.err)
		}
		if result.job == nil || result.job.Meta.ID != id {
			t.Fatalf("Reserve() job = %#v, want ID %d", result.job, id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Reserve waiter was not notified after delayed job became ready")
	}
}

func TestTopicProcessTickPersistsReservedTimeout(t *testing.T) {
	q := newTestQueue(t)
	storage := q.storage.(*memoryStorage)

	id, err := q.Put("timeout-topic", []byte("body"), 10, 0, time.Minute)
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}
	if meta := q.TryReserve([]string{"timeout-topic"}); meta == nil {
		t.Fatal("TryReserve() returned nil")
	}

	testTopic(t, q, "timeout-topic").ProcessTick(time.Now().Add(2 * time.Minute))

	meta, err := storage.GetJobMeta(context.Background(), id)
	if err != nil {
		t.Fatalf("GetJobMeta() error: %v", err)
	}
	if meta.State != StateReady {
		t.Fatalf("persisted state = %v, want Ready", meta.State)
	}
	if meta.Timeouts != 1 {
		t.Fatalf("persisted timeouts = %d, want 1", meta.Timeouts)
	}
}

func testTopic(t *testing.T, q *Queue, name string) *topic {
	t.Helper()

	q.topicMgr.mu.RLock()
	topic := q.topicMgr.getTopic(name)
	q.topicMgr.mu.RUnlock()
	if topic == nil {
		t.Fatalf("topic %q not found", name)
	}
	return topic
}

func TestQueue_Delete(t *testing.T) {
	q := newTestQueue(t)

	id, _ := q.Put("test-topic", []byte("hello"), 10, 0, 30*time.Second)
	q.TryReserve([]string{"test-topic"})

	if err := q.Delete(id); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}
}

func TestQueue_Release(t *testing.T) {
	q := newTestQueue(t)

	id, _ := q.Put("test-topic", []byte("hello"), 10, 0, 30*time.Second)
	q.TryReserve([]string{"test-topic"})

	if err := q.Release(id, 10, 0); err != nil {
		t.Fatalf("Release() error: %v", err)
	}
}

func TestQueue_Bury(t *testing.T) {
	q := newTestQueue(t)

	id, _ := q.Put("test-topic", []byte("hello"), 10, 0, 30*time.Second)
	q.TryReserve([]string{"test-topic"})

	if err := q.Bury(id, 10); err != nil {
		t.Fatalf("Bury() error: %v", err)
	}
}

func TestQueue_Kick(t *testing.T) {
	q := newTestQueue(t)

	id, _ := q.Put("test-topic", []byte("hello"), 10, 0, 30*time.Second)
	q.TryReserve([]string{"test-topic"})
	_ = q.Bury(id, 10)

	kicked, err := q.Kick("test-topic", 1)
	if err != nil {
		t.Fatalf("Kick() error: %v", err)
	}

	if kicked != 1 {
		t.Errorf("Kick() = %d, want 1", kicked)
	}
}

func TestQueue_Peek(t *testing.T) {
	q := newTestQueue(t)

	id, _ := q.Put("test-topic", []byte("peek-body"), 10, 0, 30*time.Second)

	job, err := q.Peek(id)
	if err != nil {
		t.Fatalf("Peek() error: %v", err)
	}

	if job.Meta.ID != id {
		t.Errorf("ID = %d, want %d", job.Meta.ID, id)
	}
}
