package sdq

import (
	"context"
	"sort"
	"sync"
	"time"
)

// TopicHub 管理所有 topic
// 负责 topic 的创建、查找、任务分配等
type TopicHub struct {
	mu            sync.RWMutex
	topics        map[string]*topic
	topicWrappers map[string]*topicWrapper

	config  *Config
	storage Storage
	ticker  Ticker
}

// newTopicHub 创建新的 TopicHub
func newTopicHub(config *Config, storage Storage, ticker Ticker) *TopicHub {
	// 如果 storage 为 nil，使用 MemoryStorage（主要用于测试）
	if storage == nil {
		storage = NewMemoryStorage()
	}

	return &TopicHub{
		topics:        make(map[string]*topic),
		topicWrappers: make(map[string]*topicWrapper),
		config:        config,
		storage:       storage,
		ticker:        ticker,
	}
}

// GetOrCreateTopic 获取或创建 topic（调用者必须持有锁）
func (h *TopicHub) GetOrCreateTopic(name string) (*topic, error) {
	if t, ok := h.topics[name]; ok {
		return t, nil
	}

	// 检查最大 topic 数
	if h.config.MaxTopics > 0 && len(h.topics) >= h.config.MaxTopics {
		return nil, ErrMaxTopicsReached
	}

	t := newTopic(name)
	h.topics[name] = t
	return t, nil
}

// GetTopic 获取 topic（不创建，调用者必须持有锁）
func (h *TopicHub) GetTopic(name string) *topic {
	return h.topics[name]
}

// GetOrCreateTopicWrapper 获取或创建 topicWrapper
func (h *TopicHub) GetOrCreateTopicWrapper(name string, t *topic) *topicWrapper {
	if wrapper, ok := h.topicWrappers[name]; ok {
		return wrapper
	}
	wrapper := &topicWrapper{
		topic: t,
		// mu 是独立的锁，在 topicWrapper 结构体中初始化
	}
	h.topicWrappers[name] = wrapper
	return wrapper
}

// RegisterToTicker 如果需要则注册到 ticker
func (h *TopicHub) RegisterToTicker(name string, t *topic) {
	if t.NeedsTick() {
		// 需要加锁保护 topicWrappers map 的访问
		h.mu.Lock()
		h.registerToTickerLocked(name, t)
		h.mu.Unlock()
	}
}

// registerToTickerLocked 在已持有锁的情况下注册到 ticker
// 调用者必须已持有 h.mu 锁
func (h *TopicHub) registerToTickerLocked(name string, t *topic) {
	if t.NeedsTick() {
		wrapper := h.GetOrCreateTopicWrapper(name, t)
		// wrapper 有独立的锁，可以安全调用 Register
		h.ticker.Register(name, wrapper)
	}
}

// UnregisterFromTicker 从 ticker 注销
func (h *TopicHub) UnregisterFromTicker(name string) {
	h.ticker.Unregister(name)
}

// TryReserve 尝试从指定 topics 预留任务
func (h *TopicHub) TryReserve(topics []string) *JobMeta {
	var reservedTopicName string
	var reservedTopic *topic
	var meta *JobMeta

	h.mu.Lock()
	for _, topicName := range topics {
		t, ok := h.topics[topicName]
		if !ok {
			continue
		}

		meta = t.popReady()
		if meta == nil {
			continue
		}

		// 保留任务
		now := time.Now()
		meta.State = StateReserved
		meta.ReservedAt = now
		meta.Reserves++

		t.addReserved(meta)

		// 更新到 Storage
		_ = h.storage.UpdateJobMeta(context.Background(), meta)

		// 记录需要注册到 Ticker 的 topic
		reservedTopicName = topicName
		reservedTopic = t
		break
	}
	h.mu.Unlock()

	// 在锁外注册到 Ticker，避免死锁
	if reservedTopic != nil {
		h.RegisterToTicker(reservedTopicName, reservedTopic)
	}

	return meta
}

// CleanupEmptyTopics 清理空 Topic
// 返回清理的 topic 数量
func (h *TopicHub) CleanupEmptyTopics() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	cleaned := 0
	for name, t := range h.topics {
		stats := t.stats()
		if stats.TotalJobs == 0 {
			// 从 Ticker 注销
			h.UnregisterFromTicker(name)

			// 删除 topic
			delete(h.topics, name)
			delete(h.topicWrappers, name)
			cleaned++
		}
	}

	return cleaned
}

// FindJob 查找任务（调用者必须持有锁）
func (h *TopicHub) FindJob(id uint64) (*JobMeta, *topic) {
	for _, t := range h.topics {
		// 检查 Ready
		if meta := t.ready.find(id); meta != nil {
			return meta, t
		}
		// 检查 Delayed
		if meta := t.delayed.find(id); meta != nil {
			return meta, t
		}
		// 检查 Reserved
		if meta := t.getReserved(id); meta != nil {
			return meta, t
		}
		// 检查 Buried
		if meta := t.buried.find(id); meta != nil {
			return meta, t
		}
	}
	return nil, nil
}

// ListTopics 列出所有 topic 名称（按字母排序）
func (h *TopicHub) ListTopics() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	names := make([]string, 0, len(h.topics))
	for name := range h.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TopicStats 获取单个 topic 的统计信息
func (h *TopicHub) TopicStats(name string) *TopicStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	t, ok := h.topics[name]
	if !ok {
		return nil
	}
	return t.stats()
}

// AllTopicStats 获取所有 topic 的统计信息
func (h *TopicHub) AllTopicStats() []*TopicStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := make([]*TopicStats, 0, len(h.topics))
	for _, t := range h.topics {
		stats = append(stats, t.stats())
	}
	return stats
}

// TotalJobs 获取总任务数
func (h *TopicHub) TotalJobs() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, t := range h.topics {
		total += t.ready.Len() + t.delayed.Len() + len(t.reserved) + t.buried.Len()
	}
	return total
}

// Lock 获取写锁（供外部操作使用）
func (h *TopicHub) Lock() {
	h.mu.Lock()
}

// Unlock 释放写锁
func (h *TopicHub) Unlock() {
	h.mu.Unlock()
}

// RLock 获取读锁
func (h *TopicHub) RLock() {
	h.mu.RLock()
}

// RUnlock 释放读锁
func (h *TopicHub) RUnlock() {
	h.mu.RUnlock()
}

// GetStorage 获取 storage
func (h *TopicHub) GetStorage() Storage {
	return h.storage
}

// ValidateTopicName 验证 topic 名称
func (h *TopicHub) ValidateTopicName(name string) error {
	if name == "" {
		return ErrTopicRequired
	}

	// 检查长度
	if len(name) > 200 {
		return ErrInvalidTopic
	}

	// 检查字符（字母、数字、下划线、中划线）
	for _, ch := range name {
		isLower := ch >= 'a' && ch <= 'z'
		isUpper := ch >= 'A' && ch <= 'Z'
		isDigit := ch >= '0' && ch <= '9'
		isSpecial := ch == '_' || ch == '-'
		if !isLower && !isUpper && !isDigit && !isSpecial {
			return ErrInvalidTopic
		}
	}

	return nil
}

// === 写操作 ===

// Put 添加任务到 topic
// 返回 (needsNotify bool, error)
func (h *TopicHub) Put(topicName string, meta *JobMeta) (bool, error) {
	h.mu.Lock()

	// 获取或创建 topic
	t, err := h.GetOrCreateTopic(topicName)
	if err != nil {
		h.mu.Unlock()
		return false, err
	}

	// 检查 MaxJobsPerTopic 限制
	if h.config.MaxJobsPerTopic > 0 {
		topicStats := t.stats()
		if topicStats.TotalJobs >= h.config.MaxJobsPerTopic {
			h.mu.Unlock()
			return false, ErrMaxJobsReached
		}
	}

	// 加载到内存队列
	needsNotify := false
	needsWakeup := false
	if meta.ReadyAt.IsZero() || time.Now().After(meta.ReadyAt) {
		// Ready 任务
		meta.State = StateReady
		t.pushReady(meta)
		needsNotify = true
	} else {
		// Delayed 任务
		meta.State = StateDelayed
		t.pushDelayed(meta)
		needsWakeup = true // 需要唤醒 ticker
	}

	h.mu.Unlock()

	// 注册到 Ticker（在锁外，避免死锁）
	h.RegisterToTicker(topicName, t)

	// 唤醒 Ticker（在锁外，避免死锁）
	if needsWakeup {
		h.ticker.Wakeup()
	}

	// 注意：不在这里持久化，由 Queue.Put() 的异步 worker 负责持久化
	// 这样可以避免重复持久化，并且可以批量写入提高性能

	return needsNotify, nil
}

// Delete 删除任务（必须是已保留状态）
func (h *TopicHub) Delete(id uint64) error {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.FindJob(id)
	if meta == nil {
		h.mu.Unlock()
		return ErrNotFound
	}

	// 只能删除 Reserved 状态的任务
	if meta.State != StateReserved {
		h.mu.Unlock()
		return ErrInvalidState
	}

	topicName := meta.Topic

	// 从 topic 移除
	topic.removeReserved(id)

	// 如果 topic 不再需要 tick，取消注册
	if !topic.NeedsTick() {
		h.UnregisterFromTicker(topicName)
	}

	h.mu.Unlock()

	// 从 Storage 删除（移到锁外）
	if err := h.storage.DeleteJob(context.Background(), id); err != nil {
		// 即使 Storage 删除失败，内存中已经删除了
		// 这里可以记录日志，但不影响返回结果
		_ = err
	}

	// 释放 JobMeta 回对象池
	ReleaseJobMeta(meta)

	return nil
}

// Release 释放已保留的任务
// 返回 (topicName string, needsNotify bool, error)
func (h *TopicHub) Release(id uint64, priority uint32, delay time.Duration) (string, bool, error) {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.FindJob(id)
	if meta == nil {
		h.mu.Unlock()
		return "", false, ErrNotFound
	}

	if meta.State != StateReserved {
		h.mu.Unlock()
		return "", false, ErrNotReserved
	}

	topicName := meta.Topic

	// 从 Reserved 移除
	topic.removeReserved(id)

	// 更新任务
	meta.Priority = priority
	meta.Releases++
	meta.ReservedAt = time.Time{}

	now := time.Now()
	needsNotify := false
	if delay > 0 {
		meta.State = StateDelayed
		meta.ReadyAt = now.Add(delay)
		topic.pushDelayed(meta)
	} else {
		meta.State = StateReady
		meta.ReadyAt = now
		topic.pushReady(meta)
		needsNotify = true
	}

	// 注册到 Ticker（使用 locked 版本，因为已持有锁）
	h.registerToTickerLocked(topicName, topic)

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	_ = h.storage.UpdateJobMeta(context.Background(), meta)

	return topicName, needsNotify, nil
}

// Bury 埋葬已保留的任务
func (h *TopicHub) Bury(id uint64, priority uint32) error {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.FindJob(id)
	if meta == nil {
		h.mu.Unlock()
		return ErrNotFound
	}

	if meta.State != StateReserved {
		h.mu.Unlock()
		return ErrNotReserved
	}

	topicName := meta.Topic

	// 从 Reserved 移除
	topic.removeReserved(id)

	// 埋葬任务
	meta.State = StateBuried
	meta.Priority = priority
	meta.BuriedAt = time.Now()
	meta.Buries++
	meta.ReservedAt = time.Time{}

	topic.pushBuried(meta)

	// 如果 topic 不再需要 tick，取消注册
	if !topic.NeedsTick() {
		h.UnregisterFromTicker(topicName)
	}

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	_ = h.storage.UpdateJobMeta(context.Background(), meta)

	return nil
}

// Kick 踢出埋葬的任务
// 返回 (kicked int, needsNotify bool, error)
func (h *TopicHub) Kick(topicName string, bound int) (int, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	t := h.GetTopic(topicName)
	if t == nil {
		return 0, false, nil
	}

	kicked := 0
	for range bound {
		meta := t.popBuried()
		if meta == nil {
			break
		}

		meta.State = StateReady
		meta.BuriedAt = time.Time{}
		meta.Kicks++
		meta.ReadyAt = time.Now()

		t.pushReady(meta)

		// 更新到 Storage
		_ = h.storage.UpdateJobMeta(context.Background(), meta)

		kicked++
	}

	needsNotify := kicked > 0
	return kicked, needsNotify, nil
}

// KickJob 踢出指定的埋葬任务
// 返回 (topicName string, error)
func (h *TopicHub) KickJob(id uint64) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 查找任务
	meta, topic := h.FindJob(id)
	if meta == nil {
		return "", ErrNotFound
	}

	if meta.State != StateBuried {
		return "", ErrNotBuried
	}

	topicName := topic.name

	topic.removeBuried(id)

	meta.State = StateReady
	meta.BuriedAt = time.Time{}
	meta.Kicks++
	meta.ReadyAt = time.Now()

	topic.pushReady(meta)

	// 更新到 Storage
	_ = h.storage.UpdateJobMeta(context.Background(), meta)

	return topicName, nil
}

// Touch 延长任务的 TTR
func (h *TopicHub) Touch(id uint64, config *Config, duration ...time.Duration) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 查找任务
	meta, topic := h.FindJob(id)
	if meta == nil {
		return ErrNotFound
	}

	if meta.State != StateReserved {
		return ErrNotReserved
	}

	// 检查 Touch 次数限制
	if config.MaxTouches > 0 && meta.Touches >= config.MaxTouches {
		return ErrTouchLimitExceeded
	}

	now := time.Now()

	// 检查最小 Touch 间隔
	if config.MinTouchInterval > 0 && meta.Touches > 0 {
		// 使用 LastTouchAt 检查间隔
		if now.Sub(meta.LastTouchAt) < config.MinTouchInterval {
			return ErrInvalidTouchTime
		}
	}

	// 计算本次延长的时间
	var extendDuration time.Duration
	if len(duration) > 0 {
		// 模式1：延长指定时间
		extendDuration = duration[0]
	} else {
		// 模式2：重置为原始 TTR
		extendDuration = meta.TTR
	}

	// 检查最大延长时间限制
	if config.MaxTouchDuration > 0 {
		totalExtended := meta.TotalTouchTime + extendDuration
		if totalExtended > config.MaxTouchDuration {
			return ErrTouchLimitExceeded
		}
	}

	// 更新时间和统计
	if len(duration) > 0 {
		// 延长模式：在当前 ReservedAt 基础上延长
		meta.ReservedAt = meta.ReservedAt.Add(extendDuration)
	} else {
		// 重置模式：将 ReservedAt 设为现在
		meta.ReservedAt = now
	}

	meta.Touches++
	meta.LastTouchAt = now
	meta.TotalTouchTime += extendDuration

	// 更新 Reserved 映射中的引用
	topic.addReserved(meta)

	// 更新到 Storage
	_ = h.storage.UpdateJobMeta(context.Background(), meta)

	return nil
}

// ApplyRecovery 应用恢复结果
func (h *TopicHub) ApplyRecovery(result *RecoveryResult) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 按 Topic 恢复任务
	skipped := 0
	for topicName, jobs := range result.TopicJobs {
		// 确保 topic 存在
		t, err := h.GetOrCreateTopic(topicName)
		if err != nil {
			return err
		}

		// 将任务加入对应队列
		for _, meta := range jobs {
			// 检查任务是否已存在（防止异步恢复导致的重复）
			if existingMeta, _ := h.FindJob(meta.ID); existingMeta != nil {
				skipped++
				continue
			}

			switch meta.State {
			case StateReady:
				t.pushReady(meta)
			case StateDelayed:
				t.pushDelayed(meta)
			case StateBuried:
				t.pushBuried(meta)
			}
		}

		// 注册到 Ticker（使用 locked 版本，因为已持有锁）
		h.registerToTickerLocked(topicName, t)
	}

	// 可选：记录跳过的任务数（用于调试）
	_ = skipped

	return nil
}
