package sdq

import (
	"testing"
	"time"
)

func TestNewInspector(t *testing.T) {
	q := newTestQueue(t)

	inspector := NewInspector(q)
	if inspector == nil {
		t.Fatal("NewInspector() returned nil")
	}
}

func TestInspector_Stats(t *testing.T) {
	q := newTestQueue(t)
	inspector := NewInspector(q)

	q.Put("topic1", []byte("a"), 10, 0, 30*time.Second)
	q.Put("topic2", []byte("b"), 10, 0, 30*time.Second)

	stats := inspector.Stats()

	if stats.Topics != 2 {
		t.Errorf("Topics = %d, want 2", stats.Topics)
	}

	if stats.TotalJobs != 2 {
		t.Errorf("TotalJobs = %d, want 2", stats.TotalJobs)
	}
}

func TestInspector_TopicStats(t *testing.T) {
	q := newTestQueue(t)
	inspector := NewInspector(q)

	q.Put("topic1", []byte("a"), 10, 0, 30*time.Second)
	q.Put("topic2", []byte("b"), 10, 0, 30*time.Second)

	topicStats := inspector.TopicStats()

	if len(topicStats) != 2 {
		t.Errorf("TopicStats() len = %d, want 2", len(topicStats))
	}
}

func TestInspector_ListTopics(t *testing.T) {
	q := newTestQueue(t)
	inspector := NewInspector(q)

	q.Put("topic1", []byte("a"), 10, 0, 30*time.Second)
	q.Put("topic2", []byte("b"), 10, 0, 30*time.Second)

	topics := inspector.ListTopics()

	if len(topics) != 2 {
		t.Errorf("ListTopics() len = %d, want 2", len(topics))
	}
}

func TestInspector_StatsTopic(t *testing.T) {
	q := newTestQueue(t)
	inspector := NewInspector(q)

	q.Put("test-topic", []byte("a"), 10, 0, 30*time.Second)

	stats, err := inspector.StatsTopic("test-topic")
	if err != nil {
		t.Fatalf("StatsTopic() error: %v", err)
	}

	if stats.Name != "test-topic" {
		t.Errorf("Name = %q, want %q", stats.Name, "test-topic")
	}
}

func TestInspector_StatsTopic_NotFound(t *testing.T) {
	q := newTestQueue(t)
	inspector := NewInspector(q)

	_, err := inspector.StatsTopic("non-existent")
	if err != ErrNotFound {
		t.Errorf("StatsTopic() = %v, want ErrNotFound", err)
	}
}

func TestInspector_StatsJob(t *testing.T) {
	q := newTestQueue(t)
	inspector := NewInspector(q)

	id, _ := q.Put("test-topic", []byte("hello"), 10, 0, 30*time.Second)

	meta, err := inspector.StatsJob(id)
	if err != nil {
		t.Fatalf("StatsJob() error: %v", err)
	}

	if meta.ID != id {
		t.Errorf("ID = %d, want %d", meta.ID, id)
	}
}

func TestInspector_StatsJob_NotFound(t *testing.T) {
	q := newTestQueue(t)
	inspector := NewInspector(q)

	_, err := inspector.StatsJob(99999)
	if err != ErrNotFound {
		t.Errorf("StatsJob() = %v, want ErrNotFound", err)
	}
}
