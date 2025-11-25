package sdq

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	// Test default values
	if config.DefaultTTR != 60*time.Second {
		t.Errorf("DefaultTTR = %v, want %v", config.DefaultTTR, 60*time.Second)
	}

	if config.MaxJobSize != 64*1024 {
		t.Errorf("MaxJobSize = %d, want %d", config.MaxJobSize, 64*1024)
	}

	if config.MaxTouches != 10 {
		t.Errorf("MaxTouches = %d, want %d", config.MaxTouches, 10)
	}

	if config.MaxTouchDuration != 10*60*time.Second {
		t.Errorf("MaxTouchDuration = %v, want %v", config.MaxTouchDuration, 10*60*time.Second)
	}

	if config.MinTouchInterval != 5*time.Second {
		t.Errorf("MinTouchInterval = %v, want %v", config.MinTouchInterval, 5*time.Second)
	}

	if config.MaxTopics != 0 {
		t.Errorf("MaxTopics = %d, want %d", config.MaxTopics, 0)
	}

	if config.MaxJobsPerTopic != 0 {
		t.Errorf("MaxJobsPerTopic = %d, want %d", config.MaxJobsPerTopic, 0)
	}

	if config.EnableTopicCleanup != false {
		t.Errorf("EnableTopicCleanup = %v, want %v", config.EnableTopicCleanup, false)
	}

	if config.TopicCleanupInterval != 1*time.Hour {
		t.Errorf("TopicCleanupInterval = %v, want %v", config.TopicCleanupInterval, 1*time.Hour)
	}
}

func TestConfigCustomValues(t *testing.T) {
	config := Config{
		DefaultTTR:           30 * time.Second,
		MaxJobSize:           1024,
		MaxTouches:           5,
		MaxTouchDuration:     5 * time.Minute,
		MinTouchInterval:     1 * time.Second,
		MaxTopics:            100,
		MaxJobsPerTopic:      1000,
		EnableTopicCleanup:   true,
		TopicCleanupInterval: 30 * time.Minute,
	}

	if config.DefaultTTR != 30*time.Second {
		t.Errorf("DefaultTTR = %v, want %v", config.DefaultTTR, 30*time.Second)
	}

	if config.MaxJobSize != 1024 {
		t.Errorf("MaxJobSize = %d, want %d", config.MaxJobSize, 1024)
	}

	if config.MaxTopics != 100 {
		t.Errorf("MaxTopics = %d, want %d", config.MaxTopics, 100)
	}

	if config.MaxJobsPerTopic != 1000 {
		t.Errorf("MaxJobsPerTopic = %d, want %d", config.MaxJobsPerTopic, 1000)
	}
}

func TestConfigWithTicker(t *testing.T) {
	ticker := NewDynamicSleepTicker(10*time.Millisecond, 1*time.Second)
	config := Config{
		Ticker: ticker,
	}

	if config.Ticker == nil {
		t.Error("Ticker should not be nil")
	}
}

func TestConfigWithStorage(t *testing.T) {
	RunWithAllStorages(t, func(t *testing.T, storage *TestStorage) {
		config := Config{
			Storage: storage.Storage,
		}

		if config.Storage == nil {
			t.Error("Storage should not be nil")
		}
	})
}
