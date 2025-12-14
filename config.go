package sdq

import (
	"context"
	"log/slog"
	"time"
)

// NewTickerFunc Ticker 构造函数
type NewTickerFunc func(ctx context.Context, config *Config) (Ticker, error)

// NewStorageFunc Storage 构造函数
type NewStorageFunc func(ctx context.Context, config *Config) (Storage, error)

// Config Queue 配置
type Config struct {
	// DefaultTTR 默认 TTR
	DefaultTTR time.Duration
	// MaxJobSize 最大任务大小（字节）
	MaxJobSize int

	// === Ticker 配置 ===

	// Ticker 自定义 Ticker 实例（优先级高于 NewTickerFunc）
	Ticker Ticker
	// NewTickerFunc Ticker 构造函数（当 Ticker 为 nil 时使用）
	NewTickerFunc NewTickerFunc

	// === Touch 限制配置 ===

	// MaxTouches 最大 Touch 次数
	MaxTouches int
	// MaxTouchDuration 最大延长时间
	MaxTouchDuration time.Duration
	// MinTouchInterval 最小 Touch 间隔
	MinTouchInterval time.Duration

	// === Topic 配置 ===

	// MaxTopics 最大 topic 数（0 表示无限制）
	MaxTopics int
	// MaxJobsPerTopic 每个 topic 最大任务数（0 表示无限制）
	MaxJobsPerTopic int
	// EnableTopicCleanup 启用 Topic 惰性清理（定期清理空 Topic）
	EnableTopicCleanup bool
	// TopicCleanupInterval Topic 清理间隔（默认 1 小时）
	TopicCleanupInterval time.Duration

	// === 持久化配置 ===

	// Storage 存储后端实例（优先级高于 NewStorageFunc）
	Storage Storage
	// NewStorageFunc Storage 构造函数（当 Storage 为 nil 时使用）
	NewStorageFunc NewStorageFunc

	// === 日志配置 ===

	// Logger 日志记录器（可选）
	// 如果为 nil，将使用 slog.Default()
	Logger *slog.Logger
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		DefaultTTR:           60 * time.Second,
		MaxJobSize:           64 * 1024, // 64KB
		MaxTouches:           10,
		MaxTouchDuration:     10 * 60 * time.Second, // 10 分钟
		MinTouchInterval:     5 * time.Second,
		MaxTopics:            0,     // 无限制
		MaxJobsPerTopic:      0,     // 无限制
		EnableTopicCleanup:   false, // 默认不启用
		TopicCleanupInterval: 1 * time.Hour,
	}
}
