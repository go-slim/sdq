package inspector

import (
	"sync"
	"time"
)

// ThroughputTracker 吞吐量追踪器
type ThroughputTracker struct {
	mu sync.Mutex

	// 计数器
	puts     uint64
	reserves uint64
	deletes  uint64

	// 上次统计时间
	lastTime time.Time
	lastPuts uint64
	lastRes  uint64
	lastDel  uint64

	// 计算的吞吐量
	stats *ThroughputStats
}

// NewThroughputTracker 创建吞吐量追踪器
func NewThroughputTracker() *ThroughputTracker {
	return &ThroughputTracker{
		lastTime: time.Now(),
		stats:    &ThroughputStats{},
	}
}

// RecordPut 记录 Put 操作
func (t *ThroughputTracker) RecordPut() {
	t.mu.Lock()
	t.puts++
	t.mu.Unlock()
}

// RecordReserve 记录 Reserve 操作
func (t *ThroughputTracker) RecordReserve() {
	t.mu.Lock()
	t.reserves++
	t.mu.Unlock()
}

// RecordDelete 记录 Delete 操作
func (t *ThroughputTracker) RecordDelete() {
	t.mu.Lock()
	t.deletes++
	t.mu.Unlock()
}

// Calculate 计算吞吐量（应定期调用）
func (t *ThroughputTracker) Calculate() *ThroughputStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(t.lastTime).Seconds()
	if elapsed < 0.001 {
		return t.stats
	}

	t.stats = &ThroughputStats{
		PutsPerSecond:     float64(t.puts-t.lastPuts) / elapsed,
		ReservesPerSecond: float64(t.reserves-t.lastRes) / elapsed,
		DeletesPerSecond:  float64(t.deletes-t.lastDel) / elapsed,
	}

	t.lastTime = now
	t.lastPuts = t.puts
	t.lastRes = t.reserves
	t.lastDel = t.deletes

	return t.stats
}

// Stats 获取当前吞吐量统计
func (t *ThroughputTracker) Stats() *ThroughputStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stats
}
