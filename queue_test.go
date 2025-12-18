package sdq

import (
	"testing"
	"time"
)

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
