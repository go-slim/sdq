package sdq

import (
	"testing"
	"time"
)

func TestNewJobMeta(t *testing.T) {
	meta := NewJobMeta(1, "test-topic", 10, 0, 30*time.Second)

	if meta.ID != 1 {
		t.Errorf("ID = %d, want 1", meta.ID)
	}

	if meta.Topic != "test-topic" {
		t.Errorf("Topic = %q, want %q", meta.Topic, "test-topic")
	}

	if meta.Priority != 10 {
		t.Errorf("Priority = %d, want 10", meta.Priority)
	}

	// NewJobMeta 创建的是 StateEnqueued 状态
	if meta.State != StateEnqueued {
		t.Errorf("State = %v, want %v", meta.State, StateEnqueued)
	}
}

func TestNewJobMeta_Delayed(t *testing.T) {
	meta := NewJobMeta(1, "test-topic", 10, time.Hour, 30*time.Second)

	// 延迟任务初始也是 StateEnqueued
	if meta.State != StateEnqueued {
		t.Errorf("State = %v, want %v", meta.State, StateEnqueued)
	}

	if meta.ReadyAt.Before(time.Now()) {
		t.Error("ReadyAt should be in the future for delayed job")
	}
}

func TestJobMeta_Clone(t *testing.T) {
	meta := NewJobMeta(1, "test-topic", 10, 0, 30*time.Second)
	clone := meta.Clone()

	if clone == meta {
		t.Error("Clone() should return a different pointer")
	}

	if clone.ID != meta.ID {
		t.Errorf("Clone ID = %d, want %d", clone.ID, meta.ID)
	}

	if clone.Topic != meta.Topic {
		t.Errorf("Clone Topic = %q, want %q", clone.Topic, meta.Topic)
	}
}

func TestState_String(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateEnqueued, "enqueued"},
		{StateReady, "ready"},
		{StateDelayed, "delayed"},
		{StateReserved, "reserved"},
		{StateBuried, "buried"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}
