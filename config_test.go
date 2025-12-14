package sdq

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.DefaultTTR != 60*time.Second {
		t.Errorf("DefaultTTR = %v, want %v", config.DefaultTTR, 60*time.Second)
	}

	if config.MaxJobSize != 64*1024 {
		t.Errorf("MaxJobSize = %d, want %d", config.MaxJobSize, 64*1024)
	}

	if config.MaxTouches != 10 {
		t.Errorf("MaxTouches = %d, want 10", config.MaxTouches)
	}
}
