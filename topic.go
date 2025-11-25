package sdq

import (
	"container/heap"
	"sync"
	"time"
)

// topic 表示一个命名队列
// 只存储 JobMeta，不存储 Body
// 注意：topic 的所有方法都不是线程安全的，调用者必须持有外部锁（TopicHub.mu）
type topic struct {
	name     string
	ready    *jobMetaHeap        // 就绪任务优先级队列（按 Priority 排序）
	delayed  *delayedJobHeap     // 延迟任务优先级队列（按 ReadyAt 排序）
	reserved map[uint64]*JobMeta // 保留任务映射（ID -> Meta）
	buried   *jobMetaHeap        // 埋葬任务优先级队列（按 Priority 排序）
}

// topicWrapper wraps a topic and provides thread-safe access
type topicWrapper struct {
	topic *topic
	mu    sync.RWMutex // 独立的锁，不共享 TopicHub.mu
}

// ProcessTick implements Tickable interface with locking
func (w *topicWrapper) ProcessTick(now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.topic.ProcessTick(now)
}

// NextTickTime implements Tickable interface with locking
func (w *topicWrapper) NextTickTime() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.topic.NextTickTime()
}

// NeedsTick implements Tickable interface with locking
func (w *topicWrapper) NeedsTick() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.topic.NeedsTick()
}

// newTopic 创建新的 topic
func newTopic(name string) *topic {
	return &topic{
		name:     name,
		ready:    newJobMetaHeap(),
		delayed:  newDelayedJobHeap(),
		reserved: make(map[uint64]*JobMeta),
		buried:   newJobMetaHeap(),
	}
}

// === Ready 队列操作 ===
// 注意：调用者必须持有 Queue.mu

// pushReady 添加就绪任务
func (t *topic) pushReady(meta *JobMeta) {
	heap.Push(t.ready, meta)
}

// popReady 获取并移除优先级最高的就绪任务
func (t *topic) popReady() *JobMeta {
	if t.ready.Len() == 0 {
		return nil
	}
	return heap.Pop(t.ready).(*JobMeta)
}

// peekReady 查看但不移除优先级最高的就绪任务
func (t *topic) peekReady() *JobMeta {
	if t.ready.Len() == 0 {
		return nil
	}
	return t.ready.items[0]
}

// removeReady 从就绪队列移除任务
func (t *topic) removeReady(id uint64) bool {
	return t.ready.remove(id)
}

// === Delayed 队列操作 ===
// 注意：调用者必须持有 Queue.mu

// pushDelayed 添加延迟任务
func (t *topic) pushDelayed(meta *JobMeta) {
	heap.Push(t.delayed, meta)
}

// popDelayed 获取并移除最早到期的延迟任务
func (t *topic) popDelayed() *JobMeta {
	if t.delayed.Len() == 0 {
		return nil
	}
	return heap.Pop(t.delayed).(*JobMeta)
}

// peekDelayed 查看但不移除最早到期的延迟任务
func (t *topic) peekDelayed() *JobMeta {
	if t.delayed.Len() == 0 {
		return nil
	}
	return t.delayed.items[0]
}

// removeDelayed 从延迟队列移除任务
func (t *topic) removeDelayed(id uint64) bool {
	return t.delayed.remove(id)
}

// === Reserved 队列操作 ===
// 注意：调用者必须持有 Queue.mu

// addReserved 添加保留任务
func (t *topic) addReserved(meta *JobMeta) {
	t.reserved[meta.ID] = meta
}

// removeReserved 移除保留任务
func (t *topic) removeReserved(id uint64) *JobMeta {
	meta := t.reserved[id]
	delete(t.reserved, id)
	return meta
}

// getReserved 获取保留任务
func (t *topic) getReserved(id uint64) *JobMeta {
	return t.reserved[id]
}

// === Buried 队列操作 ===
// 注意：调用者必须持有 Queue.mu

// pushBuried 添加埋葬任务
func (t *topic) pushBuried(meta *JobMeta) {
	heap.Push(t.buried, meta)
}

// popBuried 获取并移除优先级最高的埋葬任务
func (t *topic) popBuried() *JobMeta {
	if t.buried.Len() == 0 {
		return nil
	}
	return heap.Pop(t.buried).(*JobMeta)
}

// peekBuried 查看但不移除优先级最高的埋葬任务
func (t *topic) peekBuried() *JobMeta {
	if t.buried.Len() == 0 {
		return nil
	}
	return t.buried.items[0]
}

// removeBuried 从埋葬队列移除任务
func (t *topic) removeBuried(id uint64) bool {
	return t.buried.remove(id)
}

// === 统计信息 ===

// stats 返回统计信息
// 注意：调用者必须持有 Queue.mu
func (t *topic) stats() *TopicStats {
	return &TopicStats{
		Name:         t.name,
		ReadyJobs:    t.ready.Len(),
		DelayedJobs:  t.delayed.Len(),
		ReservedJobs: len(t.reserved),
		BuriedJobs:   t.buried.Len(),
		TotalJobs:    t.ready.Len() + t.delayed.Len() + len(t.reserved) + t.buried.Len(),
	}
}

// === 实现 Tickable 接口 ===

// ProcessTick 处理 tick 通知
func (t *topic) ProcessTick(now time.Time) {
	// 处理 Delayed → Ready
	t.processDelayed(now)

	// 处理 Reserved 超时 → Ready
	t.processReservedTimeout(now)
}

// NextTickTime 返回下一个需要 tick 的时间
// 注意：调用者必须持有 Queue.mu（通过 topicWrapper）
func (t *topic) NextTickTime() time.Time {
	var nextTime time.Time
	hasNext := false

	// 检查 Delayed 队列
	if delayedMeta := t.peekDelayed(); delayedMeta != nil {
		nextTime = delayedMeta.ReadyAt
		hasNext = true
	}

	// 检查 Reserved 队列
	for _, meta := range t.reserved {
		deadline := meta.ReserveDeadline()
		if !deadline.IsZero() {
			if !hasNext || deadline.Before(nextTime) {
				nextTime = deadline
				hasNext = true
			}
		}
	}

	if !hasNext {
		return time.Time{} // zero time 表示不需要 tick
	}

	return nextTime
}

// NeedsTick 是否需要 tick
// 注意：调用者必须持有 Queue.mu（通过 topicWrapper）
func (t *topic) NeedsTick() bool {
	return t.delayed.Len() > 0 || len(t.reserved) > 0
}

// processDelayed 处理延迟任务到期
// 注意：调用者必须持有 Queue.mu
func (t *topic) processDelayed(now time.Time) {
	for {
		meta := t.peekDelayed()
		if meta == nil {
			break
		}

		if !meta.ShouldBeReady(now) {
			break
		}

		// 从 Delayed 移到 Ready
		t.popDelayed()
		meta.State = StateReady
		t.pushReady(meta)
	}
}

// processReservedTimeout 处理保留任务超时
// 注意：调用者必须持有 Queue.mu
func (t *topic) processReservedTimeout(now time.Time) {
	// 收集超时的任务
	timeoutIDs := make([]uint64, 0)
	for id, meta := range t.reserved {
		if meta.ShouldTimeout(now) {
			timeoutIDs = append(timeoutIDs, id)
		}
	}

	// 处理超时任务
	for _, id := range timeoutIDs {
		meta := t.removeReserved(id)
		if meta != nil {
			meta.State = StateReady
			meta.Timeouts++
			meta.Releases++
			meta.ReservedAt = time.Time{}
			meta.ReadyAt = now
			t.pushReady(meta)
		}
	}
}

// === jobMetaHeap 实现任务元数据优先级堆（最小堆，按 Priority 排序） ===

type jobMetaHeap struct {
	items []*JobMeta
	index map[uint64]int // job ID -> index
}

func newJobMetaHeap() *jobMetaHeap {
	h := &jobMetaHeap{
		items: make([]*JobMeta, 0),
		index: make(map[uint64]int),
	}
	heap.Init(h)
	return h
}

// Len 实现 heap.Interface
func (h *jobMetaHeap) Len() int {
	return len(h.items)
}

// Less 实现 heap.Interface
// 优先级数字越小优先级越高，相同优先级按 ID 排序（FIFO）
func (h *jobMetaHeap) Less(i, j int) bool {
	if h.items[i].Priority != h.items[j].Priority {
		return h.items[i].Priority < h.items[j].Priority
	}
	return h.items[i].ID < h.items[j].ID
}

// Swap 实现 heap.Interface
func (h *jobMetaHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.index[h.items[i].ID] = i
	h.index[h.items[j].ID] = j
}

// Push 实现 heap.Interface
func (h *jobMetaHeap) Push(x any) {
	meta := x.(*JobMeta)
	h.index[meta.ID] = len(h.items)
	h.items = append(h.items, meta)
}

// Pop 实现 heap.Interface
func (h *jobMetaHeap) Pop() any {
	old := h.items
	n := len(old)
	meta := old[n-1]
	old[n-1] = nil
	h.items = old[0 : n-1]
	delete(h.index, meta.ID)
	return meta
}

// remove 移除指定任务
func (h *jobMetaHeap) remove(id uint64) bool {
	idx, ok := h.index[id]
	if !ok {
		return false
	}
	heap.Remove(h, idx)
	return true
}

// find 查找指定任务
func (h *jobMetaHeap) find(id uint64) *JobMeta {
	idx, ok := h.index[id]
	if !ok {
		return nil
	}
	return h.items[idx]
}

// === delayedJobHeap 延迟任务堆（按 ReadyAt 排序） ===

type delayedJobHeap struct {
	items []*JobMeta
	index map[uint64]int // job ID -> index
}

func newDelayedJobHeap() *delayedJobHeap {
	h := &delayedJobHeap{
		items: make([]*JobMeta, 0),
		index: make(map[uint64]int),
	}
	heap.Init(h)
	return h
}

// Len 实现 heap.Interface
func (h *delayedJobHeap) Len() int {
	return len(h.items)
}

// Less 实现 heap.Interface
// 按就绪时间排序，时间越早优先级越高
func (h *delayedJobHeap) Less(i, j int) bool {
	return h.items[i].ReadyAt.Before(h.items[j].ReadyAt)
}

// Swap 实现 heap.Interface
func (h *delayedJobHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.index[h.items[i].ID] = i
	h.index[h.items[j].ID] = j
}

// Push 实现 heap.Interface
func (h *delayedJobHeap) Push(x any) {
	meta := x.(*JobMeta)
	h.index[meta.ID] = len(h.items)
	h.items = append(h.items, meta)
}

// Pop 实现 heap.Interface
func (h *delayedJobHeap) Pop() any {
	old := h.items
	n := len(old)
	meta := old[n-1]
	old[n-1] = nil
	h.items = old[0 : n-1]
	delete(h.index, meta.ID)
	return meta
}

// remove 移除指定任务
func (h *delayedJobHeap) remove(id uint64) bool {
	idx, ok := h.index[id]
	if !ok {
		return false
	}
	heap.Remove(h, idx)
	return true
}

// find 查找指定任务
func (h *delayedJobHeap) find(id uint64) *JobMeta {
	idx, ok := h.index[id]
	if !ok {
		return nil
	}
	return h.items[idx]
}
