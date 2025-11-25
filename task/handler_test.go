package task

import (
	"context"
	"errors"
	"testing"
)

// TestNewHandler 测试创建 Handler
func TestNewHandler(t *testing.T) {
	tests := []struct {
		name        string
		handler     any
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil handler",
			handler:     nil,
			wantErr:     true,
			errContains: "cannot be nil",
		},
		{
			name:        "not a function",
			handler:     "not a function",
			wantErr:     true,
			errContains: "must be a function",
		},
		{
			name: "valid handler with context",
			handler: func(ctx context.Context, data string) error {
				return nil
			},
			wantErr: false,
		},
		{
			name: "valid handler without context",
			handler: func(data string) error {
				return nil
			},
			wantErr: false,
		},
		{
			name: "invalid - no return value",
			handler: func(data string) {
			},
			wantErr:     true,
			errContains: "must return exactly one value",
		},
		{
			name: "invalid - wrong return type",
			handler: func(data string) string {
				return ""
			},
			wantErr:     true,
			errContains: "must return error",
		},
		{
			name: "invalid - too many parameters",
			handler: func(ctx context.Context, data string, extra int) error {
				return nil
			},
			wantErr:     true,
			errContains: "must have 1 or 2 parameters",
		},
		{
			name: "invalid - no parameters",
			handler: func() error {
				return nil
			},
			wantErr:     true,
			errContains: "must have 1 or 2 parameters",
		},
		{
			name: "invalid - first param not context",
			handler: func(data string, ctx context.Context) error {
				return nil
			},
			wantErr:     true,
			errContains: "first parameter must be context.Context",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := newHandler(tt.handler)
			if tt.wantErr {
				if err == nil {
					t.Errorf("newHandler() expected error, got nil")
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("newHandler() error = %v, want to contain %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("newHandler() unexpected error: %v", err)
				return
			}
			if h == nil {
				t.Error("newHandler() returned nil handler")
			}
		})
	}
}

// TestHandlerCall 测试 Handler.Call
func TestHandlerCall(t *testing.T) {
	type TestData struct {
		Value string `json:"value"`
		Count int    `json:"count"`
	}

	t.Run("call with context", func(t *testing.T) {
		called := false
		var receivedData TestData

		h, err := newHandler(func(ctx context.Context, data TestData) error {
			called = true
			receivedData = data
			return nil
		})
		if err != nil {
			t.Fatalf("newHandler() error: %v", err)
		}

		data := `{"value":"test","count":42}`
		err = h.Call(context.Background(), []byte(data))
		if err != nil {
			t.Errorf("Call() error: %v", err)
		}

		if !called {
			t.Error("handler was not called")
		}
		if receivedData.Value != "test" {
			t.Errorf("receivedData.Value = %q, want %q", receivedData.Value, "test")
		}
		if receivedData.Count != 42 {
			t.Errorf("receivedData.Count = %d, want %d", receivedData.Count, 42)
		}
	})

	t.Run("call without context", func(t *testing.T) {
		called := false
		var receivedData TestData

		h, err := newHandler(func(data TestData) error {
			called = true
			receivedData = data
			return nil
		})
		if err != nil {
			t.Fatalf("newHandler() error: %v", err)
		}

		data := `{"value":"hello","count":99}`
		err = h.Call(context.Background(), []byte(data))
		if err != nil {
			t.Errorf("Call() error: %v", err)
		}

		if !called {
			t.Error("handler was not called")
		}
		if receivedData.Value != "hello" {
			t.Errorf("receivedData.Value = %q, want %q", receivedData.Value, "hello")
		}
		if receivedData.Count != 99 {
			t.Errorf("receivedData.Count = %d, want %d", receivedData.Count, 99)
		}
	})

	t.Run("handler returns error", func(t *testing.T) {
		expectedErr := errors.New("test error")

		h, err := newHandler(func(data TestData) error {
			return expectedErr
		})
		if err != nil {
			t.Fatalf("newHandler() error: %v", err)
		}

		data := `{"value":"test","count":1}`
		err = h.Call(context.Background(), []byte(data))
		if err != expectedErr {
			t.Errorf("Call() error = %v, want %v", err, expectedErr)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		h, err := newHandler(func(data TestData) error {
			return nil
		})
		if err != nil {
			t.Fatalf("newHandler() error: %v", err)
		}

		data := `invalid json`
		err = h.Call(context.Background(), []byte(data))
		if err == nil {
			t.Error("Call() expected error for invalid JSON, got nil")
		}
		if !contains(err.Error(), "failed to unmarshal") {
			t.Errorf("Call() error = %v, want to contain %q", err, "failed to unmarshal")
		}
	})

	t.Run("context is passed correctly", func(t *testing.T) {
		type ctxKey string
		key := ctxKey("test-key")
		expectedValue := "test-value"

		var receivedValue any

		h, err := newHandler(func(ctx context.Context, data TestData) error {
			receivedValue = ctx.Value(key)
			return nil
		})
		if err != nil {
			t.Fatalf("newHandler() error: %v", err)
		}

		ctx := context.WithValue(context.Background(), key, expectedValue)
		data := `{"value":"test","count":1}`
		err = h.Call(ctx, []byte(data))
		if err != nil {
			t.Errorf("Call() error: %v", err)
		}

		if receivedValue != expectedValue {
			t.Errorf("context value = %v, want %v", receivedValue, expectedValue)
		}
	})

	t.Run("handler returns nil error", func(t *testing.T) {
		h, err := newHandler(func(data TestData) error {
			return nil
		})
		if err != nil {
			t.Fatalf("newHandler() error: %v", err)
		}

		data := `{"value":"test","count":1}`
		err = h.Call(context.Background(), []byte(data))
		if err != nil {
			t.Errorf("Call() unexpected error: %v", err)
		}
	})
}

// contains 检查字符串是否包含子串
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && indexOf(s, substr) >= 0))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
