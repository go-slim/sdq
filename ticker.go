package sdq

import (
	"time"
)

// Tickable 可被 Ticker 调度，Topic 实现此接口以接收时间推进通知。
//
// Ticker 可能在 Register、Wakeup、Stats 和调度循环中读取下一次时间，因此实现必须允许
// NextTickTime 与自身的状态更新并发执行，并应快速返回。为兼容不同 Ticker 实现，三个方法
// 都不应回调注册自己的同一个 Ticker；部分实现会在调度锁内读取下一次时间。
//
// Register 或 Unregister 会阻止尚未选中的后续调度，但一个已经选中的 ProcessTick 可能与
// 注册表变更并发完成。Tickable 必须自行保证该情况安全。
type Tickable interface {
	// ProcessTick 处理 tick 通知
	ProcessTick(now time.Time)

	// NextTickTime 返回下一个需要 tick 的时间
	// 返回 zero time 表示不需要 tick
	NextTickTime() time.Time

	// NeedsTick 是否需要 tick
	NeedsTick() bool
}

// Ticker 定时器接口，负责定时触发已注册对象的 ProcessTick 方法。
//
// Queue 对一个 Ticker 只调用一次 Start 和一次 Stop；自定义实现可以依赖这一生命周期，
// 无需支持停止后重新启动。Queue 允许在 Start 前 Put，因此 Ticker 必须允许在 Start 前调用
// Register 和 Wakeup；此时只需记录调度状态，不能要求后台循环已经运行。
type Ticker interface {
	// Name 返回定时器名称
	Name() string

	// Start 启动定时器
	Start()

	// Stop 停止定时器
	Stop()

	// Register 注册需要 tick 的对象。同名注册必须替换旧对象，并按新对象的
	// NextTickTime 重新调度。
	Register(name string, tickable Tickable)

	// Unregister 取消注册
	Unregister(name string)

	// Wakeup 通知定时器已注册对象的 NextTickTime 可能发生变化，并要求尽快重新计算调度。
	Wakeup()

	// Stats 返回统计信息
	Stats() *TickerStats
}

// TickerStats 定时器统计信息
type TickerStats struct {
	Name            string        // 定时器名称
	RegisteredCount int           // 注册的对象数量
	NextTickTime    time.Time     // 下一个 tick 时间
	TimeUntilTick   time.Duration // 距离下一个 tick 的时间
}
