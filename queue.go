// Package sdq 提供简单高效的延迟队列实现
// 受 beanstalkd 启发，提供 topic、优先级、bury/kick、TTR 等特性
package sdq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrNotFound 任务不存在
	ErrNotFound = errors.New("sdq: job not found")
	// ErrNotReserved 任务未被保留
	ErrNotReserved = errors.New("sdq: job not reserved")
	// ErrNotBuried 任务未被埋葬
	ErrNotBuried = errors.New("sdq: job not buried")
	// ErrInvalidState 任务状态无效
	ErrInvalidState = errors.New("sdq: invalid job state")
	// ErrTimeout 操作超时
	ErrTimeout = errors.New("sdq: timeout")
	// ErrInvalidTopic topic 名称无效（空、过长或包含非法字符）
	ErrInvalidTopic = errors.New("sdq: invalid topic name")
	// ErrMaxTopicsReached 达到最大 topic 数量
	ErrMaxTopicsReached = errors.New("sdq: max topics reached")
	// ErrMaxJobsReached 达到最大任务数量
	ErrMaxJobsReached = errors.New("sdq: max jobs reached")
	// ErrTouchLimitExceeded Touch 次数超限
	ErrTouchLimitExceeded = errors.New("sdq: touch limit exceeded")
	// ErrInvalidTouchTime Touch 时间无效
	ErrInvalidTouchTime = errors.New("sdq: invalid touch time")
	// ErrTooManyWaiters 等待队列已满
	ErrTooManyWaiters = errors.New("sdq: too many waiters")
	// ErrInvalidTimeout timeout 必须大于 0
	ErrInvalidTimeout = errors.New("sdq: timeout must be greater than 0")
	// ErrQueueStopped Queue 已停止，无法重新启动
	ErrQueueStopped = errors.New("sdq: queue already stopped")
	// ErrInvalidConfig 配置无效
	ErrInvalidConfig = errors.New("sdq: invalid config")
)

// Queue 延迟队列
// 架构设计：
// - 每个 Topic 独立管理自己的 Ready/Delayed/Reserved/Buried 队列
// - 只存储 JobMeta（轻量级），Body 按需从 Storage 加载
// - Wheel Tick 负责定时通知 Topic 处理到期任务
// - Storage 负责持久化
type Queue struct {
	config Config

	// ID 生成器（内存中维护，原子递增）
	nextID atomic.Uint64

	// === 管理器 ===
	// Topic 管理
	topicMgr *topicManager
	// Reserve 管理
	reserveMgr *reserveManager

	// Ticker 定时器
	ticker Ticker

	// Storage 存储后端
	storage Storage

	// Logger 日志记录器
	logger *slog.Logger

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 恢复完成通知（用于等待异步恢复）
	recoveryDone chan struct{}
	recoveryOnce sync.Once

	// 启动控制
	startOnce sync.Once
	startErr  error
	stopped   atomic.Bool

	// 启动时间（用于 Inspector）
	startedAt time.Time

	// 操作统计（用于 Prometheus metrics）
	stats *Stats
}

// New 创建新的 Queue 实例
func New(config Config) (*Queue, error) {
	// 设置默认值
	if config.DefaultTTR == 0 {
		config.DefaultTTR = time.Minute
	}
	if config.MaxJobSize == 0 {
		config.MaxJobSize = 64 * 1024
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 初始化 logger
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	q := &Queue{
		config:       config,
		ctx:          ctx,
		cancel:       cancel,
		logger:       logger,
		reserveMgr:   newReserveManager(ctx),
		recoveryDone: make(chan struct{}),
		stats:        &Stats{},
	}

	// 创建 Ticker（必须提供）
	if config.Ticker != nil {
		// 优先使用提供的 Ticker 实例
		q.ticker = config.Ticker
	} else if config.NewTickerFunc != nil {
		// 使用构造函数创建
		ticker, err := config.NewTickerFunc(ctx, &config)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("%w: create ticker: %v", ErrInvalidConfig, err)
		}
		q.ticker = ticker
	} else {
		cancel()
		return nil, fmt.Errorf("%w: ticker is required", ErrInvalidConfig)
	}

	// 创建 Storage（必须提供）
	if config.Storage != nil {
		// 优先使用提供的 Storage 实例
		q.storage = config.Storage
	} else if config.NewStorageFunc != nil {
		// 使用构造函数创建
		storage, err := config.NewStorageFunc(ctx, &config)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("%w: create storage: %v", ErrInvalidConfig, err)
		}
		q.storage = storage
	} else {
		cancel()
		return nil, fmt.Errorf("%w: storage is required", ErrInvalidConfig)
	}

	// 设置初始 ID
	q.nextID.Store(1)

	// 创建管理器
	q.topicMgr = newTopicManager(q)

	return q, nil
}

// Start 启动 Queue
// 如果配置了 Storage，会从 Storage 恢复任务
// StartOptions 启动选项
type StartOptions struct {
	// RecoveryCallback 恢复进度回调
	// 如果提供了回调，会在恢复过程中调用它报告进度（Start, Recovering 阶段）
	RecoveryCallback RecoveryCallback
}

// Start 启动 Queue（使用异步恢复）
func (q *Queue) Start() error {
	return q.StartWithOptions(StartOptions{})
}

// StartWithOptions 使用选项启动 Queue
// 此方法是幂等的，多次调用只会启动一次
func (q *Queue) StartWithOptions(opts StartOptions) error {
	q.startOnce.Do(func() {
		q.startErr = q.doStart(opts)
	})
	return q.startErr
}

// doStart 实际启动逻辑
func (q *Queue) doStart(opts StartOptions) error {
	if q.stopped.Load() {
		return ErrQueueStopped
	}
	q.startedAt = time.Now()

	q.logger.Info("starting queue",
		slog.Bool("topic_cleanup_enabled", q.config.EnableTopicCleanup),
	)

	// 从 Storage 恢复（异步模式）
	recoveryMgr := newRecoveryManager(q.ctx, q.storage)

	// 1. 快速获取 MaxID，初始化 ID 生成器
	maxID, err := recoveryMgr.GetMaxID()
	if err != nil {
		q.logger.Error("failed to get max id", slog.Any("error", err))
		return fmt.Errorf("get max id: %w", err)
	}
	q.nextID.Store(maxID + 1)
	q.logger.Debug("initialized job id generator", slog.Uint64("next_id", maxID+1))

	// 2. 后台异步恢复任务
	q.wg.Go(func() {
		defer q.recoveryOnce.Do(func() { close(q.recoveryDone) })

		q.logger.Info("starting async job recovery")
		recoveryMgr.Recover(func(progress *RecoveryProgress) {
			// 用户回调
			if opts.RecoveryCallback != nil {
				opts.RecoveryCallback(progress)
			}

			// 处理恢复结果
			switch progress.Phase {
			case RecoveryPhaseComplete:
				q.logger.Info("job recovery completed",
					slog.Int("total_jobs", progress.TotalJobs),
					slog.Int("loaded_jobs", progress.LoadedJobs),
					slog.Int("failed_jobs", progress.FailedJobs),
				)
				if progress.Result != nil {
					if err := q.applyRecoveryJobs(progress.Result); err != nil {
						q.logger.Error("failed to apply recovery jobs", slog.Any("error", err))
					}
				}
			case RecoveryPhaseError:
				q.logger.Error("job recovery failed", slog.Any("error", progress.Error))
			}
		})
	})

	// 启动 Ticker
	q.ticker.Start()
	q.logger.Debug("started ticker")

	// 启动 Reserve 管理器
	q.reserveMgr.start()
	q.logger.Debug("started reserve manager")

	// 启动 Topic 清理（如果启用）
	if q.config.EnableTopicCleanup {
		q.wg.Add(1)
		go q.cleanupLoop()
		q.logger.Debug("started topic cleanup loop")
	}

	q.logger.Info("queue started successfully")
	return nil
}

// WaitForRecovery 等待恢复完成
// timeout: 超时时间，0 表示无限等待
// 返回: 如果超时返回 ErrTimeout，如果队列已关闭返回 context.Canceled
func (q *Queue) WaitForRecovery(timeout time.Duration) error {
	if timeout == 0 {
		// 无限等待
		select {
		case <-q.recoveryDone:
			return nil
		case <-q.ctx.Done():
			return q.ctx.Err()
		}
	}

	// 有超时限制
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-q.recoveryDone:
		return nil
	case <-timer.C:
		return ErrTimeout
	case <-q.ctx.Done():
		return q.ctx.Err()
	}
}

// Stop 停止 Queue
// 停止后无法重新启动，需要创建新的 Queue 实例
// 此方法是幂等的，多次调用只会停止一次
func (q *Queue) Stop() error {
	if !q.stopped.CompareAndSwap(false, true) {
		return nil
	}
	q.logger.Info("stopping queue")

	// 停止 Ticker
	q.ticker.Stop()
	q.logger.Debug("stopped ticker")

	// 停止 Reserve 管理器
	q.reserveMgr.stop()
	q.logger.Debug("stopped reserve manager")

	// 取消 context，停止所有后台 goroutine
	q.cancel()

	// 等待所有后台任务完成
	q.logger.Debug("waiting for background tasks to finish")
	q.wg.Wait()
	q.logger.Debug("all background tasks finished")

	// 关闭 Storage
	if err := q.storage.Close(); err != nil {
		q.logger.Error("failed to close storage", slog.Any("error", err))
		return err
	}
	q.logger.Debug("closed storage")

	q.logger.Info("queue stopped successfully")
	return nil
}

// === 内部辅助方法 ===

// allocateID 分配新的任务 ID
func (q *Queue) allocateID() uint64 {
	return q.nextID.Add(1) - 1
}

// applyRecovery 应用恢复结果到 Queue
// applyRecoveryJobs 应用恢复的任务（用于异步恢复模式）
// MaxID 已经在快速启动阶段设置，这里只应用任务
func (q *Queue) applyRecoveryJobs(result *RecoveryResult) error {
	return q.topicMgr.applyRecovery(result)
}

// cleanupLoop 定期清理空 Topic
func (q *Queue) cleanupLoop() {
	defer q.wg.Done()

	interval := q.config.TopicCleanupInterval
	if interval == 0 {
		interval = 1 * time.Hour // 默认 1 小时
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-q.ctx.Done():
			return
		case <-ticker.C:
			cleaned := q.topicMgr.cleanupEmptyTopics()
			// 可选：记录日志
			// 如果清理了 topic，可以在这里添加日志
			_ = cleaned
		}
	}
}

// === 写操作 API（委托给 TopicHub）===

// Put 添加任务到队列
// topic: topic 名称，不能为空
// body: 任务数据
// priority: 优先级（数字越小优先级越高）
// delay: 延迟时间
// ttr: 执行超时时间，0 使用默认值
func (q *Queue) Put(topic string, body []byte, priority uint32, delay, ttr time.Duration) (uint64, error) {
	// 1. 验证 topic
	if err := ValidateTopicName(topic); err != nil {
		q.logger.Warn("invalid topic name",
			slog.String("topic", topic),
			slog.Any("error", err),
		)
		return 0, err
	}

	// 2. 验证任务大小
	if len(body) > q.config.MaxJobSize {
		q.logger.Warn("job size exceeds max size",
			slog.String("topic", topic),
			slog.Int("size", len(body)),
			slog.Int("max_size", q.config.MaxJobSize),
		)
		return 0, fmt.Errorf("sdq: job size %d exceeds max size %d", len(body), q.config.MaxJobSize)
	}

	// 3. 使用默认 TTR
	if ttr == 0 {
		ttr = q.config.DefaultTTR
	}

	// 4. 分配 ID 并创建 JobMeta
	id := q.allocateID()
	meta := NewJobMeta(id, topic, priority, delay, ttr)

	q.logger.Debug("putting job",
		slog.Uint64("id", id),
		slog.String("topic", topic),
		slog.Uint64("priority", uint64(priority)),
		slog.Duration("delay", delay),
		slog.Duration("ttr", ttr),
		slog.Int("body_size", len(body)),
	)

	// 5. 同步持久化 Body 到 Storage
	// 必须在加入内存队列前完成,否则 Reserve 时可能找不到 body
	// SaveJob 内部通过 batchSaveLoop 实现批量优化
	ctx := context.Background()
	if err := q.storage.SaveJob(ctx, meta, body); err != nil {
		// 保存失败不影响任务执行(内存队列中仍然可用)
		// 但重启后会丢失
		q.logger.Error("failed to save job to storage",
			slog.Uint64("id", id),
			slog.String("topic", topic),
			slog.Any("error", err),
		)
	}

	// 6. 加入内存队列
	needsNotify, err := q.topicMgr.put(topic, meta)
	if err != nil {
		q.logger.Error("failed to put job to topic hub",
			slog.Uint64("id", id),
			slog.String("topic", topic),
			slog.Any("error", err),
		)
		return 0, err
	}

	// 7. 记录统计
	q.stats.recordPut()

	// 8. 通知等待队列
	if needsNotify {
		q.notifyWaiters(topic)
	}

	return id, nil
}

// Delete 删除任务（必须是已保留状态）
func (q *Queue) Delete(id uint64) error {
	q.logger.Debug("deleting job", slog.Uint64("id", id))
	err := q.topicMgr.delete(id, false)
	if err != nil {
		q.logger.Error("failed to delete job",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}
	q.stats.recordDelete()
	return nil
}

// ForceDelete 强制删除任务（支持任何状态）
func (q *Queue) ForceDelete(id uint64) error {
	q.logger.Debug("force deleting job", slog.Uint64("id", id))
	err := q.topicMgr.delete(id, true)
	if err != nil {
		q.logger.Error("failed to force delete job",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}
	q.stats.recordDelete()
	return nil
}

// Release 释放已保留的任务
// id: 任务 ID
// priority: 新的优先级
// delay: 延迟时间
func (q *Queue) Release(id uint64, priority uint32, delay time.Duration) error {
	q.logger.Debug("releasing job",
		slog.Uint64("id", id),
		slog.Uint64("priority", uint64(priority)),
		slog.Duration("delay", delay),
	)

	topicName, needsNotify, err := q.topicMgr.release(id, priority, delay)
	if err != nil {
		q.logger.Error("failed to release job",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}

	q.stats.recordRelease()

	if needsNotify {
		q.notifyWaiters(topicName)
	}

	return nil
}

// Bury 埋葬已保留的任务
func (q *Queue) Bury(id uint64, priority uint32) error {
	q.logger.Debug("burying job",
		slog.Uint64("id", id),
		slog.Uint64("priority", uint64(priority)),
	)

	err := q.topicMgr.bury(id, priority)
	if err != nil {
		q.logger.Error("failed to bury job",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}
	q.stats.recordBury()
	return nil
}

// Kick 踢出埋葬的任务
// topic: topic 名称
// bound: 最多踢出的任务数
func (q *Queue) Kick(topic string, bound int) (int, error) {
	if err := ValidateTopicName(topic); err != nil {
		q.logger.Warn("invalid topic name in kick",
			slog.String("topic", topic),
			slog.Any("error", err),
		)
		return 0, err
	}

	q.logger.Debug("kicking jobs",
		slog.String("topic", topic),
		slog.Int("bound", bound),
	)

	kicked, needsNotify, err := q.topicMgr.kick(topic, bound)
	if err != nil {
		q.logger.Error("failed to kick jobs",
			slog.String("topic", topic),
			slog.Any("error", err),
		)
		return 0, err
	}

	q.stats.recordKick(kicked)

	if needsNotify {
		q.notifyWaiters(topic)
	}

	q.logger.Debug("kicked jobs",
		slog.String("topic", topic),
		slog.Int("count", kicked),
	)
	return kicked, nil
}

// KickJob 踢出指定的埋葬任务
func (q *Queue) KickJob(id uint64) error {
	q.logger.Debug("kicking job", slog.Uint64("id", id))

	topicName, err := q.topicMgr.kickJob(id)
	if err != nil {
		q.logger.Error("failed to kick job",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}

	q.stats.recordKick(1)
	q.notifyWaiters(topicName)
	return nil
}

// Touch 延长任务的 TTR
// 支持两种模式：
// - Touch(id) - 重置为原始 TTR
// - Touch(id, duration) - 延长指定时间
func (q *Queue) Touch(id uint64, duration ...time.Duration) error {
	q.logger.Debug("touching job",
		slog.Uint64("id", id),
		slog.Any("duration", duration),
	)

	err := q.topicMgr.touch(id, &q.config, duration...)
	if err != nil {
		q.logger.Error("failed to touch job",
			slog.Uint64("id", id),
			slog.Any("error", err),
		)
		return err
	}

	q.stats.recordTouch()

	// 通知 Ticker 重新计算
	q.ticker.Wakeup()
	return nil
}

// === 查询操作 API（委托给 QueryManager）===

// Peek 查看任务但不保留
func (q *Queue) Peek(id uint64) (*Job, error) {
	meta := q.topicMgr.findJobByID(id)
	if meta == nil {
		return nil, ErrNotFound
	}

	// 从 Storage 加载 Body
	body, err := q.storage.GetJobBody(context.Background(), meta.ID)
	if err != nil {
		return nil, err
	}

	return NewJob(meta, body, q), nil
}

// PeekReady 查看指定 topic 的下一个就绪任务
func (q *Queue) PeekReady(topicName string) (*Job, error) {
	if err := ValidateTopicName(topicName); err != nil {
		return nil, err
	}

	meta := q.topicMgr.peekReady(topicName)
	if meta == nil {
		return nil, ErrNotFound
	}

	// 从 Storage 加载 Body
	body, err := q.storage.GetJobBody(context.Background(), meta.ID)
	if err != nil {
		return nil, err
	}

	return NewJob(meta, body, q), nil
}

// PeekDelayed 查看指定 topic 的下一个将要就绪的延迟任务
func (q *Queue) PeekDelayed(topicName string) (*Job, error) {
	if err := ValidateTopicName(topicName); err != nil {
		return nil, err
	}

	meta := q.topicMgr.peekDelayed(topicName)
	if meta == nil {
		return nil, ErrNotFound
	}

	// 从 Storage 加载 Body
	body, err := q.storage.GetJobBody(context.Background(), meta.ID)
	if err != nil {
		return nil, err
	}

	return NewJob(meta, body, q), nil
}

// PeekBuried 查看指定 topic 的下一个埋葬任务
func (q *Queue) PeekBuried(topicName string) (*Job, error) {
	if err := ValidateTopicName(topicName); err != nil {
		return nil, err
	}

	meta := q.topicMgr.peekBuried(topicName)
	if meta == nil {
		return nil, ErrNotFound
	}

	// 从 Storage 加载 Body
	body, err := q.storage.GetJobBody(context.Background(), meta.ID)
	if err != nil {
		return nil, err
	}

	return NewJob(meta, body, q), nil
}

// === Reserve 操作 API ===

// TryReserve 尝试立即预留任务
func (q *Queue) TryReserve(topics []string) *JobMeta {
	return q.topicMgr.tryReserve(topics)
}

// Reserve 从指定 topics 保留一个任务
// topics: topic 名称列表，不能为空
// timeout: 等待超时时间，0 表示立即返回
func (q *Queue) Reserve(topics []string, timeout time.Duration) (*Job, error) {
	// 验证 topics
	if len(topics) == 0 {
		q.logger.Warn("reserve called with empty topics")
		return nil, ErrInvalidTopic
	}

	for _, topic := range topics {
		if err := ValidateTopicName(topic); err != nil {
			q.logger.Warn("invalid topic name in reserve",
				slog.String("topic", topic),
				slog.Any("error", err),
			)
			return nil, err
		}
	}

	q.logger.Debug("reserving job",
		slog.Any("topics", topics),
		slog.Duration("timeout", timeout),
	)

	// 委托给 ReserveManager
	job, err := q.reserveMgr.Reserve(topics, timeout, q)
	if err != nil {
		if err == ErrTimeout {
			q.logger.Debug("reserve timeout",
				slog.Any("topics", topics),
			)
		} else {
			q.logger.Error("reserve failed",
				slog.Any("topics", topics),
				slog.Any("error", err),
			)
		}
		return nil, err
	}

	q.stats.recordReserve()

	q.logger.Debug("reserved job",
		slog.Uint64("id", job.Meta.ID),
		slog.String("topic", job.Meta.Topic),
	)
	return job, nil
}

// notifyWaiters 通知等待队列（内部方法，供 Put/Kick 调用）
func (q *Queue) notifyWaiters(topic string) {
	q.reserveMgr.notifyWaiters(topic, q)
}
