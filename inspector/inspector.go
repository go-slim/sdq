// Package inspector 提供任务查询能力，方便实时 UI 展示
package inspector

import (
	"context"
	"sort"
	"time"

	"go-slim.dev/sdq"
)

// Inspector 提供任务查询能力，方便实时 UI 展示
// 设计原则：
// - 只读操作，不修改任务状态
// - 支持分页和过滤
// - 提供聚合统计
// - 线程安全
type Inspector struct {
	queue *sdq.Queue
}

// New 创建 Inspector 实例
func New(q *sdq.Queue) *Inspector {
	return &Inspector{queue: q}
}

// ============================================================
// 查询结果数据结构
// ============================================================

// JobInfo 任务详情（用于 UI 展示）
type JobInfo struct {
	ID       uint64 `json:"id"`
	Topic    string `json:"topic"`
	Priority uint32 `json:"priority"`
	State    string `json:"state"`

	// 时间信息
	Delay     string    `json:"delay"` // 延迟时间
	TTR       string    `json:"ttr"`   // 执行超时
	CreatedAt time.Time `json:"created_at"`
	ReadyAt   time.Time `json:"ready_at,omitempty"`
	Age       string    `json:"age"` // 任务年龄

	// 状态相关时间
	ReservedAt time.Time `json:"reserved_at,omitempty"`
	BuriedAt   time.Time `json:"buried_at,omitempty"`

	// 统计信息
	Reserves int `json:"reserves"`
	Timeouts int `json:"timeouts"`
	Releases int `json:"releases"`
	Buries   int `json:"buries"`
	Kicks    int `json:"kicks"`
	Touches  int `json:"touches"`

	// 计算字段
	TimeUntilReady   string `json:"time_until_ready,omitempty"`   // 距离就绪的时间（延迟任务）
	TimeUntilTimeout string `json:"time_until_timeout,omitempty"` // 距离超时的时间（保留任务）
	BodySize         int    `json:"body_size,omitempty"`          // Body 大小（可选加载）
}

// JobListResult 任务列表查询结果
type JobListResult struct {
	Jobs       []*JobInfo `json:"jobs"`
	Total      int        `json:"total"`
	Page       int        `json:"page"`
	PageSize   int        `json:"page_size"`
	TotalPages int        `json:"total_pages"`
	HasMore    bool       `json:"has_more"`
}

// TopicInfo Topic 详情
type TopicInfo struct {
	Name           string    `json:"name"`
	ReadyJobs      int       `json:"ready_jobs"`
	DelayedJobs    int       `json:"delayed_jobs"`
	ReservedJobs   int       `json:"reserved_jobs"`
	BuriedJobs     int       `json:"buried_jobs"`
	TotalJobs      int       `json:"total_jobs"`
	WaitingWorkers int       `json:"waiting_workers"`
	CreatedAt      time.Time `json:"created_at,omitempty"` // Topic 创建时间（如果可追踪）
}

// QueueOverview 队列概览
type QueueOverview struct {
	// 基本统计
	TotalJobs    int `json:"total_jobs"`
	ReadyJobs    int `json:"ready_jobs"`
	DelayedJobs  int `json:"delayed_jobs"`
	ReservedJobs int `json:"reserved_jobs"`
	BuriedJobs   int `json:"buried_jobs"`

	// Topic 统计
	TotalTopics int `json:"total_topics"`

	// 等待统计
	TotalWaitingWorkers int `json:"total_waiting_workers"`

	// 吞吐量（需要外部追踪）
	Throughput *ThroughputStats `json:"throughput,omitempty"`

	// 系统信息
	Uptime    string    `json:"uptime"`
	StartedAt time.Time `json:"started_at"`
}

// ThroughputStats 吞吐量统计
type ThroughputStats struct {
	PutsPerSecond     float64 `json:"puts_per_second"`
	ReservesPerSecond float64 `json:"reserves_per_second"`
	DeletesPerSecond  float64 `json:"deletes_per_second"`
}

// JobQuery 任务查询条件
type JobQuery struct {
	Topic    string     `json:"topic,omitempty"`     // 按 Topic 过滤
	State    *sdq.State `json:"state,omitempty"`     // 按状态过滤
	Page     int        `json:"page,omitempty"`      // 页码（从 1 开始）
	PageSize int        `json:"page_size,omitempty"` // 每页大小（默认 20，最大 100）
	OrderBy  string     `json:"order_by,omitempty"`  // 排序字段: id, priority, created_at
	Order    string     `json:"order,omitempty"`     // 排序方向: asc, desc
	WithBody bool       `json:"with_body,omitempty"` // 是否加载 Body 大小
}

// ============================================================
// 查询方法
// ============================================================

// Overview 获取队列概览
func (i *Inspector) Overview() *QueueOverview {
	stats := i.queue.Stats()
	waitingStats := i.queue.StatsWaiting()

	totalWaiting := 0
	for _, ws := range waitingStats {
		totalWaiting += ws.WaitingWorkers
	}

	return &QueueOverview{
		TotalJobs:           stats.TotalJobs,
		ReadyJobs:           stats.ReadyJobs,
		DelayedJobs:         stats.DelayedJobs,
		ReservedJobs:        stats.ReservedJobs,
		BuriedJobs:          stats.BuriedJobs,
		TotalTopics:         stats.Topics,
		TotalWaitingWorkers: totalWaiting,
		StartedAt:           i.queue.StartedAt(),
		Uptime:              time.Since(i.queue.StartedAt()).Round(time.Second).String(),
	}
}

// ListTopics 获取所有 Topic 列表
func (i *Inspector) ListTopics() []*TopicInfo {
	topicStats := i.queue.AllTopicStats()
	waitingStats := i.queue.StatsWaiting()

	// 构建等待 worker 映射
	waitingMap := make(map[string]int)
	for _, ws := range waitingStats {
		waitingMap[ws.Topic] = ws.WaitingWorkers
	}

	topics := make([]*TopicInfo, 0, len(topicStats))
	for _, ts := range topicStats {
		topics = append(topics, &TopicInfo{
			Name:           ts.Name,
			ReadyJobs:      ts.ReadyJobs,
			DelayedJobs:    ts.DelayedJobs,
			ReservedJobs:   ts.ReservedJobs,
			BuriedJobs:     ts.BuriedJobs,
			TotalJobs:      ts.TotalJobs,
			WaitingWorkers: waitingMap[ts.Name],
		})
	}

	// 按名称排序
	sort.Slice(topics, func(a, b int) bool {
		return topics[a].Name < topics[b].Name
	})

	return topics
}

// GetTopic 获取单个 Topic 详情
func (i *Inspector) GetTopic(name string) (*TopicInfo, error) {
	ts, err := i.queue.StatsTopic(name)
	if err != nil {
		return nil, err
	}

	waitingStats := i.queue.StatsWaiting()

	// 查找等待 worker 数
	waitingWorkers := 0
	for _, ws := range waitingStats {
		if ws.Topic == name {
			waitingWorkers = ws.WaitingWorkers
			break
		}
	}

	return &TopicInfo{
		Name:           ts.Name,
		ReadyJobs:      ts.ReadyJobs,
		DelayedJobs:    ts.DelayedJobs,
		ReservedJobs:   ts.ReservedJobs,
		BuriedJobs:     ts.BuriedJobs,
		TotalJobs:      ts.TotalJobs,
		WaitingWorkers: waitingWorkers,
	}, nil
}

// ListJobs 查询任务列表
func (i *Inspector) ListJobs(ctx context.Context, query *JobQuery) (*JobListResult, error) {
	if query == nil {
		query = &JobQuery{}
	}

	// 默认值
	if query.Page < 1 {
		query.Page = 1
	}
	if query.PageSize < 1 {
		query.PageSize = 20
	}
	if query.PageSize > 100 {
		query.PageSize = 100
	}

	// 构建过滤条件
	filter := &sdq.JobMetaFilter{
		Topic:  query.Topic,
		State:  query.State,
		Limit:  query.PageSize + 1, // 多取一个用于判断 HasMore
		Offset: (query.Page - 1) * query.PageSize,
	}

	// 查询任务
	storage := i.queue.Storage()
	list, err := storage.ScanJobMeta(ctx, filter)
	if err != nil {
		return nil, err
	}

	// 判断是否有更多
	hasMore := len(list.Metas) > query.PageSize
	if hasMore {
		list.Metas = list.Metas[:query.PageSize]
	}

	// 转换为 JobInfo
	jobs := make([]*JobInfo, 0, len(list.Metas))
	now := time.Now()

	for _, meta := range list.Metas {
		info := i.metaToJobInfo(meta, now)

		// 可选：加载 Body 大小
		if query.WithBody {
			body, err := storage.GetJobBody(ctx, meta.ID)
			if err == nil {
				info.BodySize = len(body)
			}
		}

		jobs = append(jobs, info)
	}

	// 排序
	i.sortJobs(jobs, query.OrderBy, query.Order)

	// 计算总数和总页数
	total := list.Total
	if total == 0 {
		// 如果 Storage 不支持返回总数，尝试单独计数
		total, _ = storage.CountJobs(ctx, &sdq.JobMetaFilter{
			Topic: query.Topic,
			State: query.State,
		})
	}

	totalPages := (total + query.PageSize - 1) / query.PageSize
	if totalPages < 1 {
		totalPages = 1
	}

	return &JobListResult{
		Jobs:       jobs,
		Total:      total,
		Page:       query.Page,
		PageSize:   query.PageSize,
		TotalPages: totalPages,
		HasMore:    hasMore,
	}, nil
}

// GetJob 获取单个任务详情
func (i *Inspector) GetJob(ctx context.Context, id uint64, withBody bool) (*JobInfo, error) {
	meta, err := i.queue.StatsJob(id)
	if err != nil {
		return nil, err
	}

	info := i.metaToJobInfo(meta, time.Now())

	if withBody {
		body, err := i.queue.Storage().GetJobBody(ctx, id)
		if err == nil {
			info.BodySize = len(body)
		}
	}

	return info, nil
}

// GetJobBody 获取任务 Body 内容
func (i *Inspector) GetJobBody(ctx context.Context, id uint64) ([]byte, error) {
	return i.queue.Storage().GetJobBody(ctx, id)
}

// ListJobsByState 按状态查询任务
func (i *Inspector) ListJobsByState(ctx context.Context, state sdq.State, page, pageSize int) (*JobListResult, error) {
	return i.ListJobs(ctx, &JobQuery{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListJobsByTopic 按 Topic 查询任务
func (i *Inspector) ListJobsByTopic(ctx context.Context, topic string, page, pageSize int) (*JobListResult, error) {
	return i.ListJobs(ctx, &JobQuery{
		Topic:    topic,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListReadyJobs 查询就绪任务
func (i *Inspector) ListReadyJobs(ctx context.Context, page, pageSize int) (*JobListResult, error) {
	state := sdq.StateReady
	return i.ListJobs(ctx, &JobQuery{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListDelayedJobs 查询延迟任务
func (i *Inspector) ListDelayedJobs(ctx context.Context, page, pageSize int) (*JobListResult, error) {
	state := sdq.StateDelayed
	return i.ListJobs(ctx, &JobQuery{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListReservedJobs 查询保留中的任务
func (i *Inspector) ListReservedJobs(ctx context.Context, page, pageSize int) (*JobListResult, error) {
	state := sdq.StateReserved
	return i.ListJobs(ctx, &JobQuery{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListBuriedJobs 查询已埋葬的任务
func (i *Inspector) ListBuriedJobs(ctx context.Context, page, pageSize int) (*JobListResult, error) {
	state := sdq.StateBuried
	return i.ListJobs(ctx, &JobQuery{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ============================================================
// 批量操作（管理功能）
// ============================================================

// DeleteAllBuriedJobs 删除所有已埋葬的任务
// 注意：这会先 kick 任务到 ready 状态，再 reserve 并删除
func (i *Inspector) DeleteAllBuriedJobs(ctx context.Context, topic string) (int, error) {
	state := sdq.StateBuried
	filter := &sdq.JobMetaFilter{
		Topic: topic,
		State: &state,
		Limit: 100, // 批量处理
	}

	storage := i.queue.Storage()
	deleted := 0
	for {
		list, err := storage.ScanJobMeta(ctx, filter)
		if err != nil {
			return deleted, err
		}

		if len(list.Metas) == 0 {
			break
		}

		// 先 kick 所有任务到 ready 状态
		for _, meta := range list.Metas {
			_ = i.queue.KickJob(meta.ID)
		}

		// 然后 reserve 并删除
		for range list.Metas {
			job, err := i.queue.Reserve([]string{topic}, 100*time.Millisecond)
			if err != nil || job == nil {
				break
			}
			if err := job.Delete(); err == nil {
				deleted++
			}
		}
	}

	return deleted, nil
}

// KickAllBuriedJobs 踢出所有已埋葬的任务
func (i *Inspector) KickAllBuriedJobs(ctx context.Context, topic string) (int, error) {
	state := sdq.StateBuried
	filter := &sdq.JobMetaFilter{
		Topic: topic,
		State: &state,
		Limit: 100, // 批量处理
	}

	storage := i.queue.Storage()
	kicked := 0
	for {
		list, err := storage.ScanJobMeta(ctx, filter)
		if err != nil {
			return kicked, err
		}

		if len(list.Metas) == 0 {
			break
		}

		for _, meta := range list.Metas {
			if err := i.queue.KickJob(meta.ID); err == nil {
				kicked++
			}
		}
	}

	return kicked, nil
}

// KickJob 踢出单个已埋葬的任务
func (i *Inspector) KickJob(id uint64) error {
	return i.queue.KickJob(id)
}

// DeleteJob 删除单个任务
func (i *Inspector) DeleteJob(id uint64) error {
	return i.queue.Delete(id)
}

// ============================================================
// 实时监控（用于 WebSocket 推送）
// ============================================================

// Snapshot 获取当前队列快照（用于实时更新）
type Snapshot struct {
	Timestamp time.Time      `json:"timestamp"`
	Overview  *QueueOverview `json:"overview"`
	Topics    []*TopicInfo   `json:"topics"`
}

// TakeSnapshot 获取当前快照
func (i *Inspector) TakeSnapshot() *Snapshot {
	return &Snapshot{
		Timestamp: time.Now(),
		Overview:  i.Overview(),
		Topics:    i.ListTopics(),
	}
}

// WatchOptions 监控选项
type WatchOptions struct {
	Interval time.Duration // 更新间隔
}

// Watch 开始监控队列变化（返回 channel 用于接收快照）
func (i *Inspector) Watch(ctx context.Context, opts *WatchOptions) <-chan *Snapshot {
	if opts == nil {
		opts = &WatchOptions{}
	}
	if opts.Interval < 100*time.Millisecond {
		opts.Interval = 1 * time.Second
	}

	ch := make(chan *Snapshot, 1)

	go func() {
		defer close(ch)

		ticker := time.NewTicker(opts.Interval)
		defer ticker.Stop()

		// 立即发送第一个快照
		select {
		case ch <- i.TakeSnapshot():
		case <-ctx.Done():
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case ch <- i.TakeSnapshot():
				default:
					// channel 已满，跳过本次更新
				}
			}
		}
	}()

	return ch
}

// ============================================================
// 内部辅助方法
// ============================================================

// metaToJobInfo 将 JobMeta 转换为 JobInfo
func (i *Inspector) metaToJobInfo(meta *sdq.JobMeta, now time.Time) *JobInfo {
	info := &JobInfo{
		ID:         meta.ID,
		Topic:      meta.Topic,
		Priority:   meta.Priority,
		State:      meta.State.String(),
		Delay:      meta.Delay.String(),
		TTR:        meta.TTR.String(),
		CreatedAt:  meta.CreatedAt,
		ReadyAt:    meta.ReadyAt,
		Age:        now.Sub(meta.CreatedAt).Round(time.Second).String(),
		ReservedAt: meta.ReservedAt,
		BuriedAt:   meta.BuriedAt,
		Reserves:   meta.Reserves,
		Timeouts:   meta.Timeouts,
		Releases:   meta.Releases,
		Buries:     meta.Buries,
		Kicks:      meta.Kicks,
		Touches:    meta.Touches,
	}

	// 计算距离就绪的时间
	if meta.State == sdq.StateDelayed && !meta.ReadyAt.IsZero() && meta.ReadyAt.After(now) {
		info.TimeUntilReady = meta.ReadyAt.Sub(now).Round(time.Second).String()
	}

	// 计算距离超时的时间
	if meta.State == sdq.StateReserved && !meta.ReservedAt.IsZero() {
		deadline := meta.ReserveDeadline()
		if deadline.After(now) {
			info.TimeUntilTimeout = deadline.Sub(now).Round(time.Second).String()
		}
	}

	return info
}

// sortJobs 对任务列表排序
func (i *Inspector) sortJobs(jobs []*JobInfo, orderBy, order string) {
	if orderBy == "" {
		orderBy = "id"
	}
	if order == "" {
		order = "asc"
	}

	desc := order == "desc"

	sort.Slice(jobs, func(a, b int) bool {
		var less bool
		switch orderBy {
		case "priority":
			less = jobs[a].Priority < jobs[b].Priority
		case "created_at":
			less = jobs[a].CreatedAt.Before(jobs[b].CreatedAt)
		default: // id
			less = jobs[a].ID < jobs[b].ID
		}
		if desc {
			return !less
		}
		return less
	})
}
