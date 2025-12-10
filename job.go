package sdq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// State 任务状态
type State int

const (
	// StateEnqueued 已入队，等待加载到内存
	// 临时状态，任务刚创建时的初始状态
	StateEnqueued State = iota

	// StateReady 就绪，等待 Reserve
	// 任务在 Topic.ReadyHeap 中，等待 Worker 拉取
	StateReady

	// StateDelayed 延迟中，等待到期
	// 任务在 Topic.DelayedHeap 中，到期后转为 Ready
	StateDelayed

	// StateReserved 已保留，Worker 处理中
	// 任务在 Topic.ReservedMap 中，受 TTR 保护
	StateReserved

	// StateBuried 已埋葬，暂时搁置
	// 任务在 Topic.BuriedHeap 中，需要 Kick 才能恢复
	StateBuried
)

// String 返回状态的字符串表示
func (s State) String() string {
	switch s {
	case StateEnqueued:
		return "enqueued"
	case StateReady:
		return "ready"
	case StateDelayed:
		return "delayed"
	case StateReserved:
		return "reserved"
	case StateBuried:
		return "buried"
	default:
		return "unknown"
	}
}

// JobMeta 任务元数据
// 轻量级结构，常驻内存用于调度
// 大小约 200 字节
type JobMeta struct {
	// === 基本信息 ===
	ID       uint64 // 任务 ID
	Topic    string // 所属 topic
	Priority uint32 // 优先级（数字越小优先级越高）
	State    State  // 当前状态

	// === 时间参数 ===
	Delay time.Duration // 延迟时间（创建时指定）
	TTR   time.Duration // Time To Run - 执行超时时间

	// === 时间戳 ===
	CreatedAt   time.Time // 创建时间
	ReadyAt     time.Time // 就绪时间（延迟任务的到期时间）
	ReservedAt  time.Time // 被保留时间
	LastTouchAt time.Time // 最后一次 Touch 的时间
	BuriedAt    time.Time // 被埋葬时间
	DeletedAt   time.Time // 删除时间（软删除时使用）

	// === 统计信息 ===
	Reserves       int           // 被保留次数
	Timeouts       int           // 超时次数
	Releases       int           // 被释放次数
	Buries         int           // 被埋葬次数
	Kicks          int           // 被踢出次数
	Touches        int           // Touch 次数（TTR 延长）
	TotalTouchTime time.Duration // 累计延长的总时间（用于 MaxTouchDuration 检查）
}

// Job 完整任务
// 包含元数据和 Body
// Reserve 时才组装完整的 Job 返回给 Worker
type Job struct {
	Meta *JobMeta // 元数据
	body []byte   // 任务数据（可能很大，按需加载）私有字段

	// 延迟加载支持
	storage  Storage   // 存储后端引用（nil 表示 body 已加载）
	bodyOnce sync.Once // 确保 Body 只加载一次（并发安全）
	bodyErr  error     // Body 加载错误

	// Queue 引用（用于操作方法）
	queue *Queue // 所属队列引用
}

// GetBody 获取任务 Body（延迟加载，并发安全）
func (j *Job) GetBody() ([]byte, error) {
	// 快速路径：storage 为 nil 说明 body 已在创建时传入
	if j.storage == nil {
		return j.body, nil
	}

	// 延迟加载路径
	j.bodyOnce.Do(func() {
		j.body, j.bodyErr = j.storage.GetJobBody(context.Background(), j.Meta.ID)
		if j.bodyErr != nil {
			j.bodyErr = fmt.Errorf("load job body: %w", j.bodyErr)
		}
	})
	return j.body, j.bodyErr
}

// Body 返回任务 Body（便捷方法，忽略错误）
// 如果加载失败，返回 nil
// 建议使用 GetBody() 以获得错误信息
func (j *Job) Body() []byte {
	body, _ := j.GetBody()
	return body
}

// NewJobMeta 创建新的任务元数据
func NewJobMeta(id uint64, topic string, priority uint32, delay, ttr time.Duration) *JobMeta {
	now := time.Now()

	// 直接创建新对象（暂时禁用对象池以排查问题）
	meta := &JobMeta{}

	// 设置字段
	meta.ID = id
	meta.Topic = topic
	meta.Priority = priority
	meta.State = StateEnqueued
	meta.Delay = delay
	meta.TTR = ttr
	meta.CreatedAt = now

	// 设置就绪时间
	if delay > 0 {
		meta.ReadyAt = now.Add(delay)
	} else {
		// delay = 0 时，设置 ReadyAt 为现在（确保任务立即就绪）
		meta.ReadyAt = now
	}

	return meta
}

// NewJob 创建完整任务（立即加载模式）
// storage 为 nil 表示 body 已传入，GetBody 会直接返回
func NewJob(meta *JobMeta, body []byte, queue *Queue) *Job {
	return &Job{
		Meta:  meta,
		body:  body,
		queue: queue,
		// storage 保持 nil，GetBody 会走快速路径
	}
}

// NewJobWithStorage 创建任务（延迟加载模式）
// 注意：meta 应该已经是克隆的副本（由 TryReserve 返回）
// 此函数直接使用传入的 meta，不再重复克隆
func NewJobWithStorage(meta *JobMeta, storage Storage, queue *Queue) *Job {
	return &Job{
		Meta:    meta, // 直接使用，调用方需确保 meta 是独立副本
		storage: storage,
		queue:   queue,
	}
}

// Clone 克隆元数据（用于返回副本，避免外部修改）
func (m *JobMeta) Clone() *JobMeta {
	clone := *m
	return &clone
}

// ShouldBeReady 判断延迟任务是否应该转为 Ready 状态
// 只对 StateDelayed 状态有效
func (m *JobMeta) ShouldBeReady(now time.Time) bool {
	return m.State == StateDelayed && !m.ReadyAt.After(now)
}

// ShouldTimeout 判断保留任务是否应该超时转回 Ready 状态
// 只对 StateReserved 状态有效
func (m *JobMeta) ShouldTimeout(now time.Time) bool {
	if m.State != StateReserved {
		return false
	}
	deadline := m.ReservedAt.Add(m.TTR)
	return !deadline.After(now)
}

// ReserveDeadline 返回保留任务的截止时间
// 只对 StateReserved 状态有效
func (m *JobMeta) ReserveDeadline() time.Time {
	if m.State == StateReserved {
		return m.ReservedAt.Add(m.TTR)
	}
	return time.Time{}
}

// TimeUntilReady 返回延迟任务距离就绪的时间
// 只对 StateDelayed 状态有效
func (m *JobMeta) TimeUntilReady(now time.Time) time.Duration {
	if m.State != StateDelayed {
		return 0
	}
	if m.ReadyAt.Before(now) {
		return 0
	}
	return m.ReadyAt.Sub(now)
}

// TimeUntilTimeout 返回保留任务距离超时的时间
// 只对 StateReserved 状态有效
func (m *JobMeta) TimeUntilTimeout(now time.Time) time.Duration {
	if m.State != StateReserved {
		return 0
	}
	deadline := m.ReserveDeadline()
	if deadline.Before(now) {
		return 0
	}
	return deadline.Sub(now)
}

// === Job 操作方法 ===

// Delete 删除任务（必须是已保留状态）
func (j *Job) Delete() error {
	if j.queue == nil {
		return fmt.Errorf("job has no queue reference")
	}
	return j.queue.Delete(j.Meta.ID)
}

// Release 释放已保留的任务
// priority: 新的优先级
// delay: 延迟时间
func (j *Job) Release(priority uint32, delay time.Duration) error {
	if j.queue == nil {
		return fmt.Errorf("job has no queue reference")
	}
	return j.queue.Release(j.Meta.ID, priority, delay)
}

// Bury 埋葬已保留的任务
// priority: 新的优先级
func (j *Job) Bury(priority uint32) error {
	if j.queue == nil {
		return fmt.Errorf("job has no queue reference")
	}
	return j.queue.Bury(j.Meta.ID, priority)
}

// Kick 踢出埋葬任务（将此任务从埋葬状态恢复）
// 注意：此方法只能用于已埋葬的任务
func (j *Job) Kick() error {
	if j.queue == nil {
		return fmt.Errorf("job has no queue reference")
	}
	return j.queue.KickJob(j.Meta.ID)
}

// Touch 延长任务的 TTR
// 支持两种模式：
// - Touch() - 重置为原始 TTR
// - Touch(duration) - 延长指定时间
func (j *Job) Touch(duration ...time.Duration) error {
	if j.queue == nil {
		return fmt.Errorf("job has no queue reference")
	}
	return j.queue.Touch(j.Meta.ID, duration...)
}
