package sdq

import (
	"context"
	"sort"
	"sync"
	"time"
)

// topicManager 管理所有 topic
// 负责 topic 的创建、查找、任务分配等
type topicManager struct {
	mu     sync.RWMutex
	topics map[string]*topic
	queue  *Queue
}

// newTopicManager 创建新的 topicManager
func newTopicManager(q *Queue) *topicManager {
	return &topicManager{
		topics: make(map[string]*topic),
		queue:  q,
	}
}

// getOrCreateTopic 获取或创建 topic（调用者必须持有锁）
func (h *topicManager) getOrCreateTopic(name string) (*topic, error) {
	if t, ok := h.topics[name]; ok {
		return t, nil
	}

	// 检查最大 topic 数
	if h.queue.config.MaxTopics > 0 && len(h.topics) >= h.queue.config.MaxTopics {
		return nil, ErrMaxTopicsReached
	}

	t := newTopic(name, h.queue)
	h.topics[name] = t
	return t, nil
}

// getTopic 获取 topic（不创建，调用者必须持有锁）
func (h *topicManager) getTopic(name string) *topic {
	return h.topics[name]
}

// registerToTicker 如果需要则注册到 ticker（调用者必须持有锁）
func (h *topicManager) registerToTicker(name string, t *topic) {
	if t.needsTick() {
		h.queue.ticker.Register(name, t)
	}
}

// unregisterFromTicker 从 ticker 注销
func (h *topicManager) unregisterFromTicker(name string) {
	h.queue.ticker.Unregister(name)
}

// tryReserve 尝试从指定 topics 预留任务
// 返回克隆的 JobMeta，避免数据竞争
func (h *topicManager) tryReserve(topics []string) *JobMeta {
	var metaClone *JobMeta

	h.mu.Lock()

	now := time.Now()
	for _, topicName := range topics {
		t, ok := h.topics[topicName]
		if !ok {
			continue
		}

		// 使用原子操作：弹出并加入 Reserved
		meta := t.popReadyAndAddReserved(now)
		if meta == nil {
			continue
		}

		// 克隆用于返回和 Storage 更新
		// 必须在锁内克隆，避免数据竞争
		metaClone = meta.Clone()

		// 注册到 Ticker（已持有锁）
		h.registerToTicker(topicName, t)
		break
	}

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	if metaClone != nil {
		_ = h.queue.storage.UpdateJobMeta(context.Background(), metaClone)
	}

	return metaClone
}

// cleanupEmptyTopics 清理空 Topic
// 返回清理的 topic 数量
func (h *topicManager) cleanupEmptyTopics() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	cleaned := 0
	for name, t := range h.topics {
		stats := t.stats()
		if stats.TotalJobs == 0 {
			// 从 Ticker 注销
			h.unregisterFromTicker(name)

			// 删除 topic
			delete(h.topics, name)
			cleaned++
		}
	}

	return cleaned
}

// findJob 查找任务（调用者必须持有锁）
func (h *topicManager) findJob(id uint64) (*JobMeta, *topic) {
	for _, t := range h.topics {
		if meta := t.findJob(id); meta != nil {
			return meta, t
		}
	}
	return nil, nil
}

// listTopics 列出所有 topic 名称（按字母排序）
func (h *topicManager) listTopics() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	names := make([]string, 0, len(h.topics))
	for name := range h.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// topicStats 获取单个 topic 的统计信息
func (h *topicManager) topicStats(name string) *TopicStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	t, ok := h.topics[name]
	if !ok {
		return nil
	}
	return t.stats()
}

// allTopicStats 获取所有 topic 的统计信息
func (h *topicManager) allTopicStats() []*TopicStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := make([]*TopicStats, 0, len(h.topics))
	for _, t := range h.topics {
		stats = append(stats, t.stats())
	}
	return stats
}

// totalJobs 获取总任务数
func (h *topicManager) totalJobs() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, t := range h.topics {
		total += t.stats().TotalJobs
	}
	return total
}

// findJobByID 根据 ID 查找任务（带锁）
func (h *topicManager) findJobByID(id uint64) *JobMeta {
	h.mu.RLock()
	defer h.mu.RUnlock()
	meta, _ := h.findJob(id)
	if meta == nil {
		return nil
	}
	return meta.Clone()
}

// peekReady 查看指定 topic 的下一个就绪任务（带锁）
func (h *topicManager) peekReady(topicName string) *JobMeta {
	h.mu.RLock()
	defer h.mu.RUnlock()
	t := h.getTopic(topicName)
	if t == nil {
		return nil
	}
	meta := t.peekReady()
	if meta == nil {
		return nil
	}
	return meta.Clone()
}

// peekDelayed 查看指定 topic 的下一个延迟任务（带锁）
func (h *topicManager) peekDelayed(topicName string) *JobMeta {
	h.mu.RLock()
	defer h.mu.RUnlock()
	t := h.getTopic(topicName)
	if t == nil {
		return nil
	}
	meta := t.peekDelayed()
	if meta == nil {
		return nil
	}
	return meta.Clone()
}

// peekBuried 查看指定 topic 的下一个埋葬任务（带锁）
func (h *topicManager) peekBuried(topicName string) *JobMeta {
	h.mu.RLock()
	defer h.mu.RUnlock()
	t := h.getTopic(topicName)
	if t == nil {
		return nil
	}
	meta := t.peekBuried()
	if meta == nil {
		return nil
	}
	return meta.Clone()
}

// === 写操作 ===

// put 添加任务到 topic
// 返回 (needsNotify bool, error)
func (h *topicManager) put(topicName string, meta *JobMeta) (bool, error) {
	h.mu.Lock()

	// 获取或创建 topic
	t, err := h.getOrCreateTopic(topicName)
	if err != nil {
		h.mu.Unlock()
		return false, err
	}

	// 检查 MaxJobsPerTopic 限制
	if h.queue.config.MaxJobsPerTopic > 0 {
		topicStats := t.stats()
		if topicStats.TotalJobs >= h.queue.config.MaxJobsPerTopic {
			h.mu.Unlock()
			return false, ErrMaxJobsReached
		}
	}

	// 加载到内存队列
	needsNotify := false
	needsWakeup := false
	if meta.ReadyAt.IsZero() || !time.Now().Before(meta.ReadyAt) {
		// Ready 任务（当前时间 >= ReadyAt）
		meta.State = StateReady
		t.pushReady(meta)
		needsNotify = true
	} else {
		// Delayed 任务
		meta.State = StateDelayed
		t.pushDelayed(meta)
		needsWakeup = true // 需要唤醒 ticker
	}

	// 注册到 Ticker（已持有锁）
	h.registerToTicker(topicName, t)

	h.mu.Unlock()

	// 唤醒 Ticker（在锁外，避免死锁）
	if needsWakeup {
		h.queue.ticker.Wakeup()
	}

	// 注意：不在这里持久化，由 Queue.Put() 的异步 worker 负责持久化
	// 这样可以避免重复持久化，并且可以批量写入提高性能

	return needsNotify, nil
}

// delete 删除任务（必须是已保留状态）
func (h *topicManager) delete(id uint64) error {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.findJob(id)
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
	_, needsTick := topic.removeReserved(id)

	// 如果 topic 不再需要 tick，取消注册
	if !needsTick {
		h.unregisterFromTicker(topicName)
	}

	h.mu.Unlock()

	// 从 Storage 删除（移到锁外）
	if err := h.queue.storage.DeleteJob(context.Background(), id); err != nil {
		// 即使 Storage 删除失败，内存中已经删除了
		// 这里可以记录日志，但不影响返回结果
		_ = err
	}

	return nil
}

// release 释放已保留的任务
// 返回 (topicName string, needsNotify bool, error)
func (h *topicManager) release(id uint64, priority uint32, delay time.Duration) (string, bool, error) {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.findJob(id)
	if meta == nil {
		h.mu.Unlock()
		return "", false, ErrNotFound
	}

	if meta.State != StateReserved {
		h.mu.Unlock()
		return "", false, ErrNotReserved
	}

	topicName := meta.Topic

	// 使用原子操作
	now := time.Now()
	needsNotify := false
	var metaClone *JobMeta
	if delay > 0 {
		meta = topic.removeReservedAndPushDelayed(id, priority, now.Add(delay))
		if meta != nil {
			metaClone = meta.Clone()
		}
	} else {
		meta = topic.removeReservedAndPushReady(id, priority, now)
		if meta != nil {
			metaClone = meta.Clone()
			needsNotify = true
		}
	}

	// 注册到 Ticker（使用 locked 版本，因为已持有锁）
	h.registerToTicker(topicName, topic)

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	if metaClone != nil {
		_ = h.queue.storage.UpdateJobMeta(context.Background(), metaClone)
	}

	return topicName, needsNotify, nil
}

// bury 埋葬已保留的任务
func (h *topicManager) bury(id uint64, priority uint32) error {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.findJob(id)
	if meta == nil {
		h.mu.Unlock()
		return ErrNotFound
	}

	if meta.State != StateReserved {
		h.mu.Unlock()
		return ErrNotReserved
	}

	topicName := meta.Topic

	// 使用原子操作
	now := time.Now()
	meta, needsTick := topic.removeReservedAndPushBuried(id, priority, now)

	var metaClone *JobMeta
	if meta != nil {
		metaClone = meta.Clone()
	}

	// 如果 topic 不再需要 tick，取消注册
	if !needsTick {
		h.unregisterFromTicker(topicName)
	}

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	if metaClone != nil {
		_ = h.queue.storage.UpdateJobMeta(context.Background(), metaClone)
	}

	return nil
}

// kick 踢出埋葬的任务
// 返回 (kicked int, needsNotify bool, error)
func (h *topicManager) kick(topicName string, bound int) (int, bool, error) {
	h.mu.Lock()

	t := h.getTopic(topicName)
	if t == nil {
		h.mu.Unlock()
		return 0, false, nil
	}

	now := time.Now()
	var metaClones []*JobMeta
	kicked := 0
	for range bound {
		// 使用原子操作
		meta := t.popBuriedAndPushReady(now)
		if meta == nil {
			break
		}

		// 克隆用于 Storage 更新
		metaClones = append(metaClones, meta.Clone())

		kicked++
	}

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	for _, metaClone := range metaClones {
		_ = h.queue.storage.UpdateJobMeta(context.Background(), metaClone)
	}

	needsNotify := kicked > 0
	return kicked, needsNotify, nil
}

// kickJob 踢出指定的埋葬任务
// 返回 (topicName string, error)
func (h *topicManager) kickJob(id uint64) (string, error) {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.findJob(id)
	if meta == nil {
		h.mu.Unlock()
		return "", ErrNotFound
	}

	if meta.State != StateBuried {
		h.mu.Unlock()
		return "", ErrNotBuried
	}

	topicName := topic.name

	// 使用原子操作
	now := time.Now()
	meta = topic.removeBuriedByIdAndPushReady(id, now)

	var metaClone *JobMeta
	if meta != nil {
		metaClone = meta.Clone()
	}

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	if metaClone != nil {
		_ = h.queue.storage.UpdateJobMeta(context.Background(), metaClone)
	}

	return topicName, nil
}

// touch 延长任务的 TTR
func (h *topicManager) touch(id uint64, config *Config, duration ...time.Duration) error {
	h.mu.Lock()

	// 查找任务
	meta, topic := h.findJob(id)
	if meta == nil {
		h.mu.Unlock()
		return ErrNotFound
	}

	if meta.State != StateReserved {
		h.mu.Unlock()
		return ErrNotReserved
	}

	// 检查 Touch 次数限制
	if config.MaxTouches > 0 && meta.Touches >= config.MaxTouches {
		h.mu.Unlock()
		return ErrTouchLimitExceeded
	}

	now := time.Now()

	// 检查最小 Touch 间隔
	if config.MinTouchInterval > 0 && meta.Touches > 0 {
		// 使用 LastTouchAt 检查间隔
		if now.Sub(meta.LastTouchAt) < config.MinTouchInterval {
			h.mu.Unlock()
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
			h.mu.Unlock()
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

	// 克隆用于 Storage 更新（避免锁外访问）
	metaClone := meta.Clone()

	h.mu.Unlock()

	// 更新到 Storage（移到锁外）
	_ = h.queue.storage.UpdateJobMeta(context.Background(), metaClone)

	return nil
}

// applyRecovery 应用恢复结果
func (h *topicManager) applyRecovery(result *RecoveryResult) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 按 Topic 恢复任务
	skipped := 0
	for topicName, jobs := range result.TopicJobs {
		// 确保 topic 存在
		t, err := h.getOrCreateTopic(topicName)
		if err != nil {
			return err
		}

		// 将任务加入对应队列
		for _, meta := range jobs {
			// 检查任务是否已存在（防止异步恢复导致的重复）
			if existingMeta, _ := h.findJob(meta.ID); existingMeta != nil {
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
		h.registerToTicker(topicName, t)
	}

	// 可选：记录跳过的任务数（用于调试）
	_ = skipped

	return nil
}
