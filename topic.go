package sdq

import (
	"container/heap"
	"sync"
	"time"
)

// topic 表示一个命名队列
// 只存储 JobMeta，不存储 Body
// topic 是线程安全的，内部使用 RWMutex 保护
type topic struct {
	mu       sync.RWMutex
	name     string
	ready    *jobMetaHeap        // 就绪任务优先级队列（按 Priority 排序）
	delayed  *delayedJobHeap     // 延迟任务优先级队列（按 ReadyAt 排序）
	reserved map[uint64]*JobMeta // 保留任务映射（ID -> Meta）
	buried   *jobMetaHeap        // 埋葬任务优先级队列（按 Priority 排序）
}

// topicWrapper wraps a topic and provides Tickable interface
// 注意：topicWrapper 不再需要独立的锁，因为 topic 本身是线程安全的
type topicWrapper struct {
	topic *topic
}

// ProcessTick implements Tickable interface
func (w *topicWrapper) ProcessTick(now time.Time) {
	w.topic.ProcessTick(now)
}

// NextTickTime implements Tickable interface
func (w *topicWrapper) NextTickTime() time.Time {
	return w.topic.NextTickTime()
}

// NeedsTick implements Tickable interface
func (w *topicWrapper) NeedsTick() bool {
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

// pushReady 添加就绪任务
func (t *topic) pushReady(meta *JobMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()
	heap.Push(t.ready, meta)
}

// popReady 获取并移除优先级最高的就绪任务
func (t *topic) popReady() *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ready.Len() == 0 {
		return nil
	}
	return heap.Pop(t.ready).(*JobMeta)
}

// peekReady 查看但不移除优先级最高的就绪任务
func (t *topic) peekReady() *JobMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.ready.Len() == 0 {
		return nil
	}
	return t.ready.items[0]
}

// removeReady 从就绪队列移除任务
func (t *topic) removeReady(id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ready.remove(id)
}

// === Delayed 队列操作 ===

// pushDelayed 添加延迟任务
func (t *topic) pushDelayed(meta *JobMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()
	heap.Push(t.delayed, meta)
}

// popDelayed 获取并移除最早到期的延迟任务
func (t *topic) popDelayed() *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.delayed.Len() == 0 {
		return nil
	}
	return heap.Pop(t.delayed).(*JobMeta)
}

// peekDelayed 查看但不移除最早到期的延迟任务
func (t *topic) peekDelayed() *JobMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.delayed.Len() == 0 {
		return nil
	}
	return t.delayed.items[0]
}

// removeDelayed 从延迟队列移除任务
func (t *topic) removeDelayed(id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.delayed.remove(id)
}

// === Reserved 队列操作 ===

// addReserved 添加保留任务
func (t *topic) addReserved(meta *JobMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reserved[meta.ID] = meta
}

// removeReserved 移除保留任务
func (t *topic) removeReserved(id uint64) *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	meta := t.reserved[id]
	delete(t.reserved, id)
	return meta
}

// getReserved 获取保留任务
func (t *topic) getReserved(id uint64) *JobMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.reserved[id]
}

// === Buried 队列操作 ===

// pushBuried 添加埋葬任务
func (t *topic) pushBuried(meta *JobMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()
	heap.Push(t.buried, meta)
}

// popBuried 获取并移除优先级最高的埋葬任务
func (t *topic) popBuried() *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.buried.Len() == 0 {
		return nil
	}
	return heap.Pop(t.buried).(*JobMeta)
}

// peekBuried 查看但不移除优先级最高的埋葬任务
func (t *topic) peekBuried() *JobMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.buried.Len() == 0 {
		return nil
	}
	return t.buried.items[0]
}

// removeBuried 从埋葬队列移除任务
func (t *topic) removeBuried(id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buried.remove(id)
}

// === 统计信息 ===

// stats 返回统计信息
func (t *topic) stats() *TopicStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
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
func (t *topic) NextTickTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var nextTime time.Time
	hasNext := false

	// 检查 Delayed 队列
	if t.delayed.Len() > 0 {
		delayedMeta := t.delayed.items[0]
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
func (t *topic) NeedsTick() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.delayed.Len() > 0 || len(t.reserved) > 0
}

// processDelayed 处理延迟任务到期
func (t *topic) processDelayed(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for t.delayed.Len() > 0 {
		meta := t.delayed.items[0]

		if !meta.ShouldBeReady(now) {
			break
		}

		// 从 Delayed 移到 Ready
		heap.Pop(t.delayed)
		meta.State = StateReady
		heap.Push(t.ready, meta)
	}
}

// processReservedTimeout 处理保留任务超时
func (t *topic) processReservedTimeout(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 收集超时的任务
	timeoutIDs := make([]uint64, 0)
	for id, meta := range t.reserved {
		if meta.ShouldTimeout(now) {
			timeoutIDs = append(timeoutIDs, id)
		}
	}

	// 处理超时任务
	for _, id := range timeoutIDs {
		meta := t.reserved[id]
		delete(t.reserved, id)
		if meta != nil {
			meta.State = StateReady
			meta.Timeouts++
			meta.Releases++
			meta.ReservedAt = time.Time{}
			meta.ReadyAt = now
			heap.Push(t.ready, meta)
		}
	}
}

// === 批量操作（用于 TopicHub 内部，需要原子操作多个队列） ===

// popReadyAndAddReserved 原子地从 Ready 弹出并加入 Reserved
func (t *topic) popReadyAndAddReserved(now time.Time) *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.ready.Len() == 0 {
		return nil
	}

	meta := heap.Pop(t.ready).(*JobMeta)
	meta.State = StateReserved
	meta.ReservedAt = now
	meta.Reserves++
	t.reserved[meta.ID] = meta

	return meta
}

// removeReservedAndPushReady 原子地从 Reserved 移除并加入 Ready（用于 Release）
func (t *topic) removeReservedAndPushReady(id uint64, priority uint32, now time.Time) *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()

	meta := t.reserved[id]
	if meta == nil {
		return nil
	}
	delete(t.reserved, id)

	meta.Priority = priority
	meta.Releases++
	meta.ReservedAt = time.Time{}
	meta.State = StateReady
	meta.ReadyAt = now
	heap.Push(t.ready, meta)

	return meta
}

// removeReservedAndPushDelayed 原子地从 Reserved 移除并加入 Delayed（用于 Release with delay）
func (t *topic) removeReservedAndPushDelayed(id uint64, priority uint32, readyAt time.Time) *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()

	meta := t.reserved[id]
	if meta == nil {
		return nil
	}
	delete(t.reserved, id)

	meta.Priority = priority
	meta.Releases++
	meta.ReservedAt = time.Time{}
	meta.State = StateDelayed
	meta.ReadyAt = readyAt
	heap.Push(t.delayed, meta)

	return meta
}

// removeReservedAndPushBuried 原子地从 Reserved 移除并加入 Buried（用于 Bury）
func (t *topic) removeReservedAndPushBuried(id uint64, priority uint32, now time.Time) *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()

	meta := t.reserved[id]
	if meta == nil {
		return nil
	}
	delete(t.reserved, id)

	meta.State = StateBuried
	meta.Priority = priority
	meta.BuriedAt = now
	meta.Buries++
	meta.ReservedAt = time.Time{}
	heap.Push(t.buried, meta)

	return meta
}

// popBuriedAndPushReady 原子地从 Buried 弹出并加入 Ready（用于 Kick）
func (t *topic) popBuriedAndPushReady(now time.Time) *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.buried.Len() == 0 {
		return nil
	}

	meta := heap.Pop(t.buried).(*JobMeta)
	meta.State = StateReady
	meta.BuriedAt = time.Time{}
	meta.Kicks++
	meta.ReadyAt = now
	heap.Push(t.ready, meta)

	return meta
}

// removeBuriedByIdAndPushReady 原子地从 Buried 移除指定任务并加入 Ready（用于 KickJob）
func (t *topic) removeBuriedByIdAndPushReady(id uint64, now time.Time) *JobMeta {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 使用 removeAndReturn 来获取被移除的 meta
	meta := t.buried.removeAndReturn(id)
	if meta == nil {
		return nil
	}

	meta.State = StateReady
	meta.BuriedAt = time.Time{}
	meta.Kicks++
	meta.ReadyAt = now
	heap.Push(t.ready, meta)

	return meta
}

// findJob 在所有队列中查找任务
func (t *topic) findJob(id uint64) *JobMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// 检查 Ready
	if meta := t.ready.find(id); meta != nil {
		return meta
	}
	// 检查 Delayed
	if meta := t.delayed.find(id); meta != nil {
		return meta
	}
	// 检查 Reserved
	if meta := t.reserved[id]; meta != nil {
		return meta
	}
	// 检查 Buried
	if meta := t.buried.find(id); meta != nil {
		return meta
	}
	return nil
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

// removeAndReturn 移除并返回指定任务
func (h *jobMetaHeap) removeAndReturn(id uint64) *JobMeta {
	idx, ok := h.index[id]
	if !ok {
		return nil
	}
	return heap.Remove(h, idx).(*JobMeta)
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
