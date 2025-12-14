package sdq

import (
	"context"
	"sync/atomic"
	"time"
)

// Inspector 提供队列统计和监控功能
type Inspector struct {
	queue *Queue
}

// NewInspector 创建新的 Inspector
func NewInspector(queue *Queue) *Inspector {
	return &Inspector{queue: queue}
}

// Stats 返回整体统计信息的快照
func (i *Inspector) Stats() *Stats {
	allStats := i.queue.topicMgr.allTopicStats()

	// 复制 stats
	stats := &Stats{
		Topics:   len(allStats),
		Puts:     atomic.LoadUint64(&i.queue.stats.Puts),
		Reserves: atomic.LoadUint64(&i.queue.stats.Reserves),
		Deletes:  atomic.LoadUint64(&i.queue.stats.Deletes),
		Releases: atomic.LoadUint64(&i.queue.stats.Releases),
		Buries:   atomic.LoadUint64(&i.queue.stats.Buries),
		Kicks:    atomic.LoadUint64(&i.queue.stats.Kicks),
		Timeouts: atomic.LoadUint64(&i.queue.stats.Timeouts),
		Touches:  atomic.LoadUint64(&i.queue.stats.Touches),
	}

	// 累计各 topic 的任务统计
	for _, topicStats := range allStats {
		stats.TotalJobs += topicStats.TotalJobs
		stats.ReadyJobs += topicStats.ReadyJobs
		stats.DelayedJobs += topicStats.DelayedJobs
		stats.ReservedJobs += topicStats.ReservedJobs
		stats.BuriedJobs += topicStats.BuriedJobs
	}

	return stats
}

// TopicStats 返回所有 Topic 的统计信息
func (i *Inspector) TopicStats() []*TopicStats {
	return i.queue.topicMgr.allTopicStats()
}

// WaitingStats 返回所有 topics 的等待队列统计
func (i *Inspector) WaitingStats() []WaitingStats {
	statsMap := i.queue.reserveMgr.stats()

	stats := make([]WaitingStats, 0, len(statsMap))
	for topic, count := range statsMap {
		stats = append(stats, WaitingStats{
			Topic:          topic,
			WaitingWorkers: count,
		})
	}

	return stats
}

// StartedAt 返回队列启动时间
func (i *Inspector) StartedAt() time.Time {
	return i.queue.startedAt
}

// Storage 返回存储后端
func (i *Inspector) Storage() Storage {
	return i.queue.storage
}

// StatsJob 返回指定任务的元数据
func (i *Inspector) StatsJob(id uint64) (*JobMeta, error) {
	meta := i.queue.topicMgr.findJobByID(id)
	if meta == nil {
		return nil, ErrNotFound
	}
	return meta, nil
}

// StatsTopic 返回 topic 统计信息
func (i *Inspector) StatsTopic(name string) (*TopicStats, error) {
	if err := ValidateTopicName(name); err != nil {
		return nil, err
	}

	stats := i.queue.topicMgr.topicStats(name)
	if stats == nil {
		return nil, ErrNotFound
	}

	return stats, nil
}

// ListTopics 返回所有 topic 列表
func (i *Inspector) ListTopics() []string {
	return i.queue.topicMgr.listTopics()
}

// StorageStats 返回存储统计信息
func (i *Inspector) StorageStats(ctx context.Context) (*StorageStats, error) {
	return i.queue.storage.Stats(ctx)
}

// TickerStats 返回定时器统计信息
func (i *Inspector) TickerStats() *TickerStats {
	return i.queue.ticker.Stats()
}
