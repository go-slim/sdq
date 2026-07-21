package webui

import (
	"context"
	"sort"
	"time"

	"go-slim.dev/sdq"
)

// Query 为 WebUI 提供查询、聚合统计和队列管理操作。
//
// List、Get、Overview 和 Storage 等查询不会修改任务；Kick、Delete 等方法会直接修改运行中
// Queue。各聚合结果由多个并发安全的读取组成，但不是事务快照，高并发下相邻字段可能来自
// 略有差异的时刻。
type Query struct {
	inspector *sdq.Inspector
	queue     *sdq.Queue
}

// NewQuery 创建 Query 实例
func NewQuery(q *sdq.Queue) *Query {
	return &Query{
		inspector: sdq.NewInspector(q),
		queue:     q,
	}
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

	// 操作计数
	Puts     uint64 `json:"puts"`
	Reserves uint64 `json:"reserves"`
	Deletes  uint64 `json:"deletes"`
	Releases uint64 `json:"releases"`
	Buries   uint64 `json:"buries"`
	Kicks    uint64 `json:"kicks"`
	Timeouts uint64 `json:"timeouts"`
	Touches  uint64 `json:"touches"`

	// 系统信息
	Uptime    string    `json:"uptime"`
	StartedAt time.Time `json:"started_at"`
}

// StorageInfo 是 Storage.Stats 返回的实际存储状态以及队列运行时间。
//
// Storage 接口不暴露文件路径、数据库连接数、备份状态等实现细节，因此这里不会推测或
// 填充这些信息。
type StorageInfo struct {
	Name           string    `json:"name"`
	TotalJobs      int64     `json:"total_jobs"`
	TotalTopics    int       `json:"total_topics"`
	MetaSize       int64     `json:"meta_size"`
	BodySize       int64     `json:"body_size"`
	TotalSize      int64     `json:"total_size"`
	AvgMetaSize    int64     `json:"avg_meta_size"`
	AvgBodySize    int64     `json:"avg_body_size"`
	LoadedMetaSize int64     `json:"loaded_meta_size"`
	LoadedBodySize int64     `json:"loaded_body_size"`
	StartedAt      time.Time `json:"started_at"`
	Uptime         string    `json:"uptime"`
}

// JobFilter 任务查询条件。
//
// Storage 接口只规定按 ID 升序分页，因此 OrderBy 和 Order 只对当前页生效，不是对完整结果集
// 排序后再分页。需要稳定的全局排序时，应扩展 Storage 查询契约，而不是依赖这两个字段。
type JobFilter struct {
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
func (q *Query) Overview() *QueueOverview {
	stats := q.inspector.Stats()
	waitingStats := q.inspector.WaitingStats()

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
		Puts:                stats.Puts,
		Reserves:            stats.Reserves,
		Deletes:             stats.Deletes,
		Releases:            stats.Releases,
		Buries:              stats.Buries,
		Kicks:               stats.Kicks,
		Timeouts:            stats.Timeouts,
		Touches:             stats.Touches,
		StartedAt:           q.inspector.StartedAt(),
		Uptime:              time.Since(q.inspector.StartedAt()).Round(time.Second).String(),
	}
}

// Storage 返回当前存储实现报告的真实统计信息。
func (q *Query) Storage(ctx context.Context) (*StorageInfo, error) {
	stats, err := q.inspector.StorageStats(ctx)
	if err != nil {
		return nil, err
	}

	startedAt := q.inspector.StartedAt()
	return &StorageInfo{
		Name:           stats.Name,
		TotalJobs:      stats.TotalJobs,
		TotalTopics:    stats.TotalTopics,
		MetaSize:       stats.MetaSize,
		BodySize:       stats.BodySize,
		TotalSize:      stats.TotalSize,
		AvgMetaSize:    stats.AvgMetaSize,
		AvgBodySize:    stats.AvgBodySize,
		LoadedMetaSize: stats.LoadedMetaSize,
		LoadedBodySize: stats.LoadedBodySize,
		StartedAt:      startedAt,
		Uptime:         time.Since(startedAt).Round(time.Second).String(),
	}, nil
}

// ListTopics 获取所有 Topic 列表
func (q *Query) ListTopics() []*TopicInfo {
	topicStats := q.inspector.TopicStats()
	waitingStats := q.inspector.WaitingStats()

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
func (q *Query) GetTopic(name string) (*TopicInfo, error) {
	ts, err := q.inspector.StatsTopic(name)
	if err != nil {
		return nil, err
	}

	waitingStats := q.inspector.WaitingStats()

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

// ListJobs 查询任务列表。
//
// 返回的 Total 来自一次独立计数查询，并非与列表读取组成事务快照；高并发状态变化下二者
// 可能存在短暂差异。OrderBy 和 Order 仅重排当前页。
func (q *Query) ListJobs(ctx context.Context, query *JobFilter) (*JobListResult, error) {
	if query == nil {
		query = &JobFilter{}
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
	list, err := q.inspector.ListJobs(ctx, filter)
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
		info := q.metaToJobInfo(meta, now)

		// 可选：加载 Body 大小
		if query.WithBody {
			body, err := q.inspector.GetJobBody(ctx, meta.ID)
			if err == nil {
				info.BodySize = len(body)
			}
		}

		jobs = append(jobs, info)
	}

	// 排序
	q.sortJobs(jobs, query.OrderBy, query.Order)

	// JobMetaList.Total 对 Storage 是可选的，且 0 无法区分“空集合”和“未提供”。
	// 展示分页需要精确总数，因此显式调用 CountJobs，避免把当前页大小当成总数。
	total, err := q.inspector.CountJobs(ctx, &sdq.JobMetaFilter{
		Topic: query.Topic,
		State: query.State,
	})
	if err != nil {
		return nil, err
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
func (q *Query) GetJob(ctx context.Context, id uint64, withBody bool) (*JobInfo, error) {
	meta, err := q.inspector.StatsJob(id)
	if err != nil {
		return nil, err
	}

	info := q.metaToJobInfo(meta, time.Now())

	if withBody {
		body, err := q.inspector.GetJobBody(ctx, id)
		if err == nil {
			info.BodySize = len(body)
		}
	}

	return info, nil
}

// GetJobBody 获取任务 Body 内容
func (q *Query) GetJobBody(ctx context.Context, id uint64) ([]byte, error) {
	return q.inspector.GetJobBody(ctx, id)
}

// ListJobsByState 按状态查询任务
func (q *Query) ListJobsByState(ctx context.Context, state sdq.State, page, pageSize int) (*JobListResult, error) {
	return q.ListJobs(ctx, &JobFilter{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListJobsByTopic 按 Topic 查询任务
func (q *Query) ListJobsByTopic(ctx context.Context, topic string, page, pageSize int) (*JobListResult, error) {
	return q.ListJobs(ctx, &JobFilter{
		Topic:    topic,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListReadyJobs 查询就绪任务
func (q *Query) ListReadyJobs(ctx context.Context, page, pageSize int) (*JobListResult, error) {
	state := sdq.StateReady
	return q.ListJobs(ctx, &JobFilter{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListDelayedJobs 查询延迟任务
func (q *Query) ListDelayedJobs(ctx context.Context, page, pageSize int) (*JobListResult, error) {
	state := sdq.StateDelayed
	return q.ListJobs(ctx, &JobFilter{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListReservedJobs 查询保留中的任务
func (q *Query) ListReservedJobs(ctx context.Context, page, pageSize int) (*JobListResult, error) {
	state := sdq.StateReserved
	return q.ListJobs(ctx, &JobFilter{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ListBuriedJobs 查询已埋葬的任务
func (q *Query) ListBuriedJobs(ctx context.Context, page, pageSize int) (*JobListResult, error) {
	state := sdq.StateBuried
	return q.ListJobs(ctx, &JobFilter{
		State:    &state,
		Page:     page,
		PageSize: pageSize,
	})
}

// ============================================================
// 批量操作（管理功能）
// ============================================================

// DeleteAllBuriedJobs 删除匹配 Topic 的全部已埋葬任务。
//
// 该操作不可逆，并且与队列的其他操作不构成事务。方法按扫描到的精确 ID 删除，
// 不会先将任务转为 Ready，因此不会误取同 Topic 中的其他就绪任务。任务在扫描后若被
// 其他 goroutine 改变状态，ForceDelete 仍会删除该 ID。中途失败时返回已删除数量和错误，
// 已完成的删除不会回滚。扫描没有固定快照，并发新增的埋葬任务也可能被纳入本次操作；
// 持续写入会延长完成时间。
func (q *Query) DeleteAllBuriedJobs(ctx context.Context, topic string) (int, error) {
	state := sdq.StateBuried
	filter := &sdq.JobMetaFilter{
		Topic: topic,
		State: &state,
		Limit: 100, // 批量处理
	}

	deleted := 0
	for {
		list, err := q.inspector.ListJobs(ctx, filter)
		if err != nil {
			return deleted, err
		}

		if len(list.Metas) == 0 {
			break
		}

		for _, meta := range list.Metas {
			if err := ctx.Err(); err != nil {
				return deleted, err
			}
			if err := q.inspector.ForceDeleteJob(meta.ID); err != nil {
				return deleted, err
			}
			deleted++
		}
	}

	return deleted, nil
}

// KickAllBuriedJobs 踢出匹配 Topic 的全部已埋葬任务。
//
// 该操作不是事务。中途失败时返回已踢出数量和首个错误，已完成的状态变化不会回滚。
// 扫描没有固定快照，并发新增的埋葬任务也可能被纳入本次操作；持续写入会延长完成时间。
func (q *Query) KickAllBuriedJobs(ctx context.Context, topic string) (int, error) {
	state := sdq.StateBuried
	filter := &sdq.JobMetaFilter{
		Topic: topic,
		State: &state,
		Limit: 100, // 批量处理
	}

	kicked := 0
	for {
		list, err := q.inspector.ListJobs(ctx, filter)
		if err != nil {
			return kicked, err
		}

		if len(list.Metas) == 0 {
			break
		}

		for _, meta := range list.Metas {
			if err := ctx.Err(); err != nil {
				return kicked, err
			}
			if err := q.inspector.KickJob(meta.ID); err != nil {
				return kicked, err
			}
			kicked++
		}
	}

	return kicked, nil
}

// KickJob 踢出单个已埋葬的任务
func (q *Query) KickJob(id uint64) error {
	return q.inspector.KickJob(id)
}

// DeleteJob 删除单个任务（仅限已保留状态）
func (q *Query) DeleteJob(id uint64) error {
	return q.inspector.DeleteJob(id)
}

// ForceDeleteJob 强制删除单个任务（支持任何状态）
func (q *Query) ForceDeleteJob(id uint64) error {
	return q.inspector.ForceDeleteJob(id)
}

// ============================================================
// 实时快照（供轮询或推送适配层使用）
// ============================================================

// Snapshot 获取当前队列快照（用于实时更新）
type Snapshot struct {
	Timestamp time.Time      `json:"timestamp"`
	Overview  *QueueOverview `json:"overview"`
	Topics    []*TopicInfo   `json:"topics"`
}

// TakeSnapshot 获取当前快照
func (q *Query) TakeSnapshot() *Snapshot {
	return &Snapshot{
		Timestamp: time.Now(),
		Overview:  q.Overview(),
		Topics:    q.ListTopics(),
	}
}

// WatchOptions 配置快照轮询。
type WatchOptions struct {
	// Interval 是快照间隔。小于 100ms 时使用 1s，避免高频聚合读取。
	Interval time.Duration
}

// Watch 按固定间隔产生队列快照。
//
// Watch 不是变更日志：channel 只缓冲一个快照，消费者处理不及时会丢弃中间更新，下一次
// 成功发送的快照仍反映当时的完整当前状态。取消 ctx 会停止 goroutine 并关闭 channel。
// ctx 不能为 nil；Watch 不会修改调用方传入的 WatchOptions。
func (q *Query) Watch(ctx context.Context, opts *WatchOptions) <-chan *Snapshot {
	if ctx == nil {
		panic("webui: nil context")
	}

	interval := time.Second
	if opts != nil && opts.Interval >= 100*time.Millisecond {
		interval = opts.Interval
	}

	ch := make(chan *Snapshot, 1)

	go func() {
		defer close(ch)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// 立即发送第一个快照
		select {
		case ch <- q.TakeSnapshot():
		case <-ctx.Done():
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case ch <- q.TakeSnapshot():
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
func (q *Query) metaToJobInfo(meta *sdq.JobMeta, now time.Time) *JobInfo {
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
func (q *Query) sortJobs(jobs []*JobInfo, orderBy, order string) {
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
