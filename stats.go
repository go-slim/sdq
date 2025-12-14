package sdq

import "sync/atomic"

// Stats 队列统计信息
type Stats struct {
	// 队列状态
	TotalJobs    int // 总任务数
	ReadyJobs    int // 就绪任务数
	DelayedJobs  int // 延迟任务数
	ReservedJobs int // 保留任务数
	BuriedJobs   int // 埋葬任务数
	Topics       int // topic 数量

	// 操作计数
	Puts     uint64 // Put 操作次数
	Reserves uint64 // Reserve 操作次数
	Deletes  uint64 // Delete 操作次数
	Releases uint64 // Release 操作次数
	Buries   uint64 // Bury 操作次数
	Kicks    uint64 // Kick 操作次数
	Timeouts uint64 // 超时次数
	Touches  uint64 // Touch 操作次数
}

// recordPut 记录 Put 操作
func (s *Stats) recordPut() {
	atomic.AddUint64(&s.Puts, 1)
}

// recordReserve 记录 Reserve 操作
func (s *Stats) recordReserve() {
	atomic.AddUint64(&s.Reserves, 1)
}

// recordDelete 记录 Delete 操作
func (s *Stats) recordDelete() {
	atomic.AddUint64(&s.Deletes, 1)
}

// recordRelease 记录 Release 操作
func (s *Stats) recordRelease() {
	atomic.AddUint64(&s.Releases, 1)
}

// recordBury 记录 Bury 操作
func (s *Stats) recordBury() {
	atomic.AddUint64(&s.Buries, 1)
}

// recordKick 记录 Kick 操作（可批量）
func (s *Stats) recordKick(count int) {
	atomic.AddUint64(&s.Kicks, uint64(count))
}

// recordTimeout 记录超时
func (s *Stats) recordTimeout() {
	atomic.AddUint64(&s.Timeouts, 1)
}

// recordTouch 记录 Touch 操作
func (s *Stats) recordTouch() {
	atomic.AddUint64(&s.Touches, 1)
}

// TopicStats Topic 统计信息
type TopicStats struct {
	Name         string
	ReadyJobs    int
	DelayedJobs  int
	ReservedJobs int
	BuriedJobs   int
	TotalJobs    int
}

// WaitingStats 等待队列统计信息
type WaitingStats struct {
	Topic          string // Topic 名称
	WaitingWorkers int    // 等待的 worker 数量
}
