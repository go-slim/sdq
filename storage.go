package sdq

import (
	"context"
	"errors"
)

var (
	// ErrStorageClosed 存储已关闭
	ErrStorageClosed = errors.New("sdq: storage closed")
	// ErrJobExists 任务已存在
	ErrJobExists = errors.New("sdq: job already exists")
)

// JobMetaFilter 任务元数据过滤条件
type JobMetaFilter struct {
	Topic  string // 按 topic 过滤，空表示不过滤
	State  *State // 按状态过滤，nil 表示不过滤
	Limit  int    // 返回数量限制，0 表示无限制
	Offset int    // 偏移量，用于普通分页；不要与 Cursor 同时使用
	Cursor uint64 // 只返回 ID 大于 Cursor 的任务；0 表示从头开始
}

// JobMetaList 任务元数据列表结果
type JobMetaList struct {
	Metas      []*JobMeta // 任务元数据列表
	Total      int        // Offset/Limit 前的匹配总数；0 也可表示实现不提供该统计
	HasMore    bool       // 是否还有更多数据
	NextCursor uint64     // HasMore 为 true 时下一页游标，即本页最后一个任务 ID
}

// Storage 持久化存储接口
// 设计原则：元数据与 Body 分离存储
// - 元数据：轻量级（~200B），常驻内存，用于调度，会更新
// - Body：可能很大（KB~MB），按需加载，不可变
type Storage interface {
	// Name 返回存储名称
	Name() string

	// === 任务创建（原子操作） ===

	// SaveJob 保存完整任务（元数据 + Body）。
	//
	// 只在 Put 时调用，同时保存 meta 和 body。返回 nil 时，后续 GetJobMeta 和 GetJobBody
	// 必须已经能够读到该任务；仅把请求放入异步缓冲区不满足此契约。Body 不可变，一旦保存
	// 就不会修改。如果任务已存在则返回 ErrJobExists。
	SaveJob(ctx context.Context, meta *JobMeta, body []byte) error

	// === 元数据操作 ===

	// UpdateJobMeta 更新任务元数据。
	//
	// 只更新元数据（状态、统计等），不涉及 Body。实现可以在返回前完成持久化，
	// 也可以只接受到有界内部缓冲区，但 Close 必须排空已接受的更新。如果任务不存在则返回
	// ErrNotFound。
	UpdateJobMeta(ctx context.Context, meta *JobMeta) error

	// GetJobMeta 获取任务元数据
	// 如果任务不存在则返回 ErrNotFound
	GetJobMeta(ctx context.Context, id uint64) (*JobMeta, error)

	// ScanJobMeta 扫描任务元数据，不加载 Body。
	//
	// filter 为 nil 时返回全部元数据。使用 Cursor 分页时，实现必须只返回 ID > Cursor 的
	// 记录并按 ID 严格升序排列。HasMore 为 true 时，NextCursor 必须等于本页最后一条记录
	// 的 ID 且大于传入的 Cursor；否则 Queue 启动恢复可能重复读取同一页。
	//
	// Cursor 用于 Queue 的有界内存启动恢复，不应与 Offset 混用。Total 是可选信息，恢复
	// 流程不会依赖它计算进度。
	ScanJobMeta(ctx context.Context, filter *JobMetaFilter) (*JobMetaList, error)

	// === Body 操作 ===

	// GetJobBody 获取任务 Body
	// 如果任务不存在则返回 ErrNotFound
	// Reserve 时才调用，按需加载
	GetJobBody(ctx context.Context, id uint64) ([]byte, error)

	// === 任务删除 ===

	// DeleteJob 删除任务（元数据 + Body）
	// 如果任务不存在则返回 ErrNotFound
	DeleteJob(ctx context.Context, id uint64) error

	// === 统计查询 ===

	// CountJobs 统计任务数量。
	//
	// filter 为 nil 时统计所有任务。实现只应用 Topic 和 State，忽略 Limit、Offset 和 Cursor，
	// 以便调用方取得分页前的精确总数。
	CountJobs(ctx context.Context, filter *JobMetaFilter) (int, error)

	// GetMaxJobID 获取最大任务 ID
	// 用于快速启动时初始化 ID 生成器，避免扫描所有任务
	// 如果没有任务则返回 0
	GetMaxJobID(ctx context.Context) (uint64, error)

	// === 统计信息 ===

	// Stats 返回存储统计信息
	Stats(ctx context.Context) (*StorageStats, error)

	// === 资源管理 ===

	// Close 关闭存储并排空已经接受的后台写入。调用方必须先停止所有并发存储操作；Close
	// 不需要与 SaveJob、UpdateJobMeta 或查询方法并发安全。
	Close() error
}

// StorageStats 存储统计信息
type StorageStats struct {
	Name           string // 存储名称
	TotalJobs      int64  // 总任务数
	TotalTopics    int    // 总 topic 数
	MetaSize       int64  // 元数据存储大小（字节）
	BodySize       int64  // Body 存储大小（字节）
	TotalSize      int64  // 总存储大小（字节）
	LastSaveTime   int64  // 最后保存时间（Unix 时间戳）
	LastLoadTime   int64  // 最后加载时间（Unix 时间戳）
	AvgMetaSize    int64  // 平均元数据大小（字节）
	AvgBodySize    int64  // 平均 Body 大小（字节）
	LoadedMetaSize int64  // 已加载元数据大小（字节）
	LoadedBodySize int64  // 已加载 Body 大小（字节）
}
