package task

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go-slim.dev/sdq"
)

// Task 表示一个注册的任务类型
type Task struct {
	name    string
	handler *Handler
	config  *Config
	queue   *sdq.Queue
}

// Config 任务配置
type Config struct {
	// Handler 任务处理函数，必须是 func(ctx context.Context, data T) error 形式
	Handler any

	// FallbackHandler 失败处理函数（可选）
	FallbackHandler any

	// Priority 默认优先级（数字越小优先级越高）
	Priority uint32

	// TTR 默认超时时间
	TTR time.Duration

	// MaxRetries 最大重试次数
	MaxRetries int
}

// Option 任务发布选项
type Option func(*options)

type options struct {
	delay    time.Duration
	priority uint32
	ttr      time.Duration
}

// Delay 设置任务延迟执行时间
func Delay(d time.Duration) Option {
	return func(o *options) {
		o.delay = d
	}
}

// DelayUntil 设置任务在指定时间执行
func DelayUntil(t time.Time) Option {
	return func(o *options) {
		if delay := time.Until(t); delay > 0 {
			o.delay = delay
		}
	}
}

// Priority 设置任务优先级
func Priority(p uint32) Option {
	return func(o *options) {
		o.priority = p
	}
}

// TTR 设置任务超时时间
func TTR(d time.Duration) Option {
	return func(o *options) {
		o.ttr = d
	}
}

var (
	registry = make(map[string]*Task)
	mu       sync.RWMutex
)

// Register 注册一个任务类型
//
// Example:
//
//	type OrderData struct {
//	    OrderID uint
//	}
//
//	var orderTask = task.Register("process-order", &task.Config{
//	    Handler: func(ctx context.Context, data OrderData) error {
//	        // 处理订单
//	        return nil
//	    },
//	    Priority: 10,
//	    TTR: 60 * time.Second,
//	})
func Register(name string, config *Config) *Task {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("task %q already registered", name))
	}

	if config == nil {
		config = &Config{}
	}

	// 设置默认值
	if config.Priority == 0 {
		config.Priority = 10
	}
	if config.TTR == 0 {
		config.TTR = 60 * time.Second
	}

	// 创建处理器
	handler, err := newHandler(config.Handler)
	if err != nil {
		panic(fmt.Sprintf("invalid handler for task %q: %v", name, err))
	}

	t := &Task{
		name:    name,
		handler: handler,
		config:  config,
	}

	registry[name] = t
	return t
}

// Get 获取已注册的任务
func Get(name string) (*Task, bool) {
	mu.RLock()
	defer mu.RUnlock()
	t, ok := registry[name]
	return t, ok
}

// All 返回所有已注册的任务
func All() map[string]*Task {
	mu.RLock()
	defer mu.RUnlock()
	result := make(map[string]*Task, len(registry))
	for name, t := range registry {
		result[name] = t
	}
	return result
}

// SetQueue 设置任务使用的队列
func (t *Task) SetQueue(q *sdq.Queue) {
	t.queue = q
}

// Publish 发布任务到队列
//
// Example:
//
//	err := orderTask.Publish(ctx, OrderData{OrderID: 123})
//	err := orderTask.Publish(ctx, OrderData{OrderID: 456}, task.Delay(5*time.Minute))
func (t *Task) Publish(ctx context.Context, data any, opts ...Option) error {
	if t.queue == nil {
		return fmt.Errorf("task %q has no queue set", t.name)
	}

	// 应用选项
	o := options{
		priority: t.config.Priority,
		ttr:      t.config.TTR,
	}
	for _, opt := range opts {
		opt(&o)
	}

	// 序列化数据
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal task data: %w", err)
	}

	// 发布任务
	_, err = t.queue.Put(
		t.name,
		body,
		o.priority,
		o.delay,
		o.ttr,
	)

	return err
}
