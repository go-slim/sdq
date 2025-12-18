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

	// 安全读取操作统计
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

// TopicStats 返回所有 Topic 的统计信息（已排序）
// 注意：当 topic 数量较多时，建议使用 TopicStatsPage 进行分页查询
func (i *Inspector) TopicStats() []*TopicStats {
	return i.queue.topicMgr.allTopicStats()
}

// TopicStatsPage 分页返回 Topic 的统计信息（已排序）
// offset: 起始位置（从 0 开始）
// limit: 返回数量
// order: 排序方向（SortAsc 或 SortDesc）
// 返回: (统计信息列表, 总数)
func (i *Inspector) TopicStatsPage(offset, limit int, order SortOrder) ([]*TopicStats, int) {
	return i.queue.topicMgr.allTopicStatsPage(offset, limit, order)
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

// StorageStats 返回存储统计信息
func (i *Inspector) StorageStats(ctx context.Context) (*StorageStats, error) {
	return i.queue.storage.Stats(ctx)
}

// TickerStats 返回定时器统计信息
func (i *Inspector) TickerStats() *TickerStats {
	return i.queue.ticker.Stats()
}

// StartedAt 返回队列启动时间
func (i *Inspector) StartedAt() time.Time {
	return i.queue.startedAt
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

// ListTopics 返回所有 topic 列表（已排序）
// 注意：当 topic 数量较多时，建议使用 ListTopicsPage 进行分页查询
func (i *Inspector) ListTopics() []string {
	return i.queue.topicMgr.listTopics()
}

// ListTopicsPage 分页返回 topic 列表（已排序）
// offset: 起始位置（从 0 开始）
// limit: 返回数量
// order: 排序方向（SortAsc 或 SortDesc）
// 返回: (topic 名称列表, 总数)
func (i *Inspector) ListTopicsPage(offset, limit int, order SortOrder) ([]string, int) {
	return i.queue.topicMgr.listTopicsPage(offset, limit, order)
}

// KickJob 踢出指定的埋葬任务（使其重新进入 ready 队列）
func (i *Inspector) KickJob(id uint64) error {
	return i.queue.KickJob(id)
}

// DeleteJob 删除指定的任务（仅限已保留状态）
func (i *Inspector) DeleteJob(id uint64) error {
	return i.queue.Delete(id)
}

// ForceDeleteJob 强制删除指定的任务（支持任何状态：ready/delayed/reserved/buried）
func (i *Inspector) ForceDeleteJob(id uint64) error {
	return i.queue.ForceDelete(id)
}

// ListJobs 查询任务元数据列表（支持过滤和分页）
// 这是一个底层方法，用于实现各种查询场景
func (i *Inspector) ListJobs(ctx context.Context, filter *JobMetaFilter) (*JobMetaList, error) {
	return i.queue.storage.ScanJobMeta(ctx, filter)
}

// GetJobBody 获取任务 Body 内容
func (i *Inspector) GetJobBody(ctx context.Context, id uint64) ([]byte, error) {
	return i.queue.storage.GetJobBody(ctx, id)
}
