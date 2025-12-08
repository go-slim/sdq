package sdq

import "sync/atomic"

// QueueStats 队列操作统计（线程安全）
// 用于 Prometheus metrics 等监控场景
type QueueStats struct {
	// 操作计数（累计值，适合 Prometheus Counter）
	puts     atomic.Uint64 // Put 操作次数
	reserves atomic.Uint64 // Reserve 操作次数
	deletes  atomic.Uint64 // Delete 操作次数
	releases atomic.Uint64 // Release 操作次数
	buries   atomic.Uint64 // Bury 操作次数
	kicks    atomic.Uint64 // Kick 操作次数
	timeouts atomic.Uint64 // 超时次数（TTR 超时自动释放）
	touches  atomic.Uint64 // Touch 操作次数
}

// newQueueStats 创建新的统计实例
func newQueueStats() *QueueStats {
	return &QueueStats{}
}

// RecordPut 记录 Put 操作
func (s *QueueStats) RecordPut() {
	s.puts.Add(1)
}

// RecordReserve 记录 Reserve 操作
func (s *QueueStats) RecordReserve() {
	s.reserves.Add(1)
}

// RecordDelete 记录 Delete 操作
func (s *QueueStats) RecordDelete() {
	s.deletes.Add(1)
}

// RecordRelease 记录 Release 操作
func (s *QueueStats) RecordRelease() {
	s.releases.Add(1)
}

// RecordBury 记录 Bury 操作
func (s *QueueStats) RecordBury() {
	s.buries.Add(1)
}

// RecordKick 记录 Kick 操作（可批量）
func (s *QueueStats) RecordKick(count int) {
	s.kicks.Add(uint64(count))
}

// RecordTimeout 记录超时
func (s *QueueStats) RecordTimeout() {
	s.timeouts.Add(1)
}

// RecordTouch 记录 Touch 操作
func (s *QueueStats) RecordTouch() {
	s.touches.Add(1)
}

// Snapshot 返回当前统计快照
type StatsSnapshot struct {
	Puts     uint64 `json:"puts"`
	Reserves uint64 `json:"reserves"`
	Deletes  uint64 `json:"deletes"`
	Releases uint64 `json:"releases"`
	Buries   uint64 `json:"buries"`
	Kicks    uint64 `json:"kicks"`
	Timeouts uint64 `json:"timeouts"`
	Touches  uint64 `json:"touches"`
}

// Snapshot 获取当前统计快照
func (s *QueueStats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		Puts:     s.puts.Load(),
		Reserves: s.reserves.Load(),
		Deletes:  s.deletes.Load(),
		Releases: s.releases.Load(),
		Buries:   s.buries.Load(),
		Kicks:    s.kicks.Load(),
		Timeouts: s.timeouts.Load(),
		Touches:  s.touches.Load(),
	}
}

// Puts 返回 Put 操作次数
func (s *QueueStats) Puts() uint64 {
	return s.puts.Load()
}

// Reserves 返回 Reserve 操作次数
func (s *QueueStats) Reserves() uint64 {
	return s.reserves.Load()
}

// Deletes 返回 Delete 操作次数
func (s *QueueStats) Deletes() uint64 {
	return s.deletes.Load()
}

// Releases 返回 Release 操作次数
func (s *QueueStats) Releases() uint64 {
	return s.releases.Load()
}

// Buries 返回 Bury 操作次数
func (s *QueueStats) Buries() uint64 {
	return s.buries.Load()
}

// Kicks 返回 Kick 操作次数
func (s *QueueStats) Kicks() uint64 {
	return s.kicks.Load()
}

// Timeouts 返回超时次数
func (s *QueueStats) Timeouts() uint64 {
	return s.timeouts.Load()
}

// Touches 返回 Touch 操作次数
func (s *QueueStats) Touches() uint64 {
	return s.touches.Load()
}
