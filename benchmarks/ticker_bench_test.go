package benchmarks

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go-slim.dev/sdq/x/dynsleep"
	"go-slim.dev/sdq/x/timewheel"
)

// mockTickable 用于基准测试的 mock Tickable
type mockTickable struct {
	nextTime time.Time
	mu       sync.RWMutex
}

func newMockTickable(next time.Time) *mockTickable {
	return &mockTickable{nextTime: next}
}

func (m *mockTickable) ProcessTick(now time.Time) {
	m.mu.Lock()
	m.nextTime = now.Add(100 * time.Millisecond)
	m.mu.Unlock()
}

func (m *mockTickable) NextTickTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nextTime
}

func (m *mockTickable) NeedsTick() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return time.Now().After(m.nextTime)
}

// BenchmarkTimeWheelTicker_Register 测试 TimeWheelTicker 注册性能
func BenchmarkTimeWheelTicker_Register(b *testing.B) {
	ticker := timewheel.New(10*time.Millisecond, 100)
	ticker.Start()
	defer ticker.Stop()

	for i := 0; b.Loop(); i++ {
		name := fmt.Sprintf("obj-%d", i)
		mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
		ticker.Register(name, mock)
	}
}

// BenchmarkTimeWheelTicker_Unregister 测试 TimeWheelTicker 取消注册性能
func BenchmarkTimeWheelTicker_Unregister(b *testing.B) {
	ticker := timewheel.New(10*time.Millisecond, 100)
	ticker.Start()
	defer ticker.Stop()

	// 预先注册
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("obj-%d", i)
		mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
		ticker.Register(name, mock)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("obj-%d", i)
		ticker.Unregister(name)
	}
}

// BenchmarkTimeWheelTicker_Wakeup 测试 TimeWheelTicker 唤醒性能
func BenchmarkTimeWheelTicker_Wakeup(b *testing.B) {
	ticker := timewheel.New(10*time.Millisecond, 100)
	ticker.Start()
	defer ticker.Stop()

	// 注册一些对象
	for i := range 100 {
		name := fmt.Sprintf("obj-%d", i)
		mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
		ticker.Register(name, mock)
	}

	for b.Loop() {
		ticker.Wakeup()
	}
}

// BenchmarkTimeWheelTicker_Stats 测试 TimeWheelTicker 统计性能
func BenchmarkTimeWheelTicker_Stats(b *testing.B) {
	ticker := timewheel.New(10*time.Millisecond, 100)
	ticker.Start()
	defer ticker.Stop()

	// 注册一些对象
	for i := range 100 {
		name := fmt.Sprintf("obj-%d", i)
		mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
		ticker.Register(name, mock)
	}

	for b.Loop() {
		_ = ticker.Stats()
	}
}

// BenchmarkDynamicSleepTicker_Register 测试 DynamicSleepTicker 注册性能
func BenchmarkDynamicSleepTicker_Register(b *testing.B) {
	ticker := dynsleep.New(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	for i := 0; b.Loop(); i++ {
		name := fmt.Sprintf("obj-%d", i)
		mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
		ticker.Register(name, mock)
	}
}

// BenchmarkDynamicSleepTicker_Unregister 测试 DynamicSleepTicker 取消注册性能
func BenchmarkDynamicSleepTicker_Unregister(b *testing.B) {
	ticker := dynsleep.New(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// 预先注册
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("obj-%d", i)
		mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
		ticker.Register(name, mock)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("obj-%d", i)
		ticker.Unregister(name)
	}
}

// BenchmarkDynamicSleepTicker_Wakeup 测试 DynamicSleepTicker 唤醒性能
func BenchmarkDynamicSleepTicker_Wakeup(b *testing.B) {
	ticker := dynsleep.New(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// 注册一些对象
	for i := range 100 {
		name := fmt.Sprintf("obj-%d", i)
		mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
		ticker.Register(name, mock)
	}

	for b.Loop() {
		ticker.Wakeup()
	}
}

// BenchmarkDynamicSleepTicker_Stats 测试 DynamicSleepTicker 统计性能
func BenchmarkDynamicSleepTicker_Stats(b *testing.B) {
	ticker := dynsleep.New(10*time.Millisecond, 1*time.Second)
	ticker.Start()
	defer ticker.Stop()

	// 注册一些对象
	for i := range 100 {
		name := fmt.Sprintf("obj-%d", i)
		mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
		ticker.Register(name, mock)
	}

	for b.Loop() {
		_ = ticker.Stats()
	}
}

// BenchmarkTicker_Comparison 比较两种 Ticker 实现
func BenchmarkTicker_Comparison(b *testing.B) {
	registeredCounts := []int{10, 100, 1000}

	for _, count := range registeredCounts {
		b.Run(fmt.Sprintf("TimeWheel_Register_%d", count), func(b *testing.B) {
			ticker := timewheel.New(10*time.Millisecond, 100)
			ticker.Start()
			defer ticker.Stop()

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				for j := range count {
					name := fmt.Sprintf("obj-%d-%d", i, j)
					mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
					ticker.Register(name, mock)
				}
			}
		})

		b.Run(fmt.Sprintf("Dynamic_Register_%d", count), func(b *testing.B) {
			ticker := dynsleep.New(10*time.Millisecond, 1*time.Second)
			ticker.Start()
			defer ticker.Stop()

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				for j := range count {
					name := fmt.Sprintf("obj-%d-%d", i, j)
					mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
					ticker.Register(name, mock)
				}
			}
		})
	}
}

// BenchmarkTicker_ConcurrentRegister 测试并发注册性能
func BenchmarkTicker_ConcurrentRegister(b *testing.B) {
	b.Run("TimeWheel", func(b *testing.B) {
		ticker := timewheel.New(10*time.Millisecond, 100)
		ticker.Start()
		defer ticker.Stop()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				name := fmt.Sprintf("obj-%d", i)
				mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
				ticker.Register(name, mock)
				i++
			}
		})
	})

	b.Run("Dynamic", func(b *testing.B) {
		ticker := dynsleep.New(10*time.Millisecond, 1*time.Second)
		ticker.Start()
		defer ticker.Stop()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				name := fmt.Sprintf("obj-%d", i)
				mock := newMockTickable(time.Now().Add(100 * time.Millisecond))
				ticker.Register(name, mock)
				i++
			}
		})
	})
}

// BenchmarkTicker_WheelSlots 测试不同时间轮槽数的性能影响
func BenchmarkTicker_WheelSlots(b *testing.B) {
	slotCounts := []int{60, 100, 360, 1000}

	for _, slots := range slotCounts {
		b.Run(fmt.Sprintf("Slots_%d", slots), func(b *testing.B) {
			ticker := timewheel.New(10*time.Millisecond, slots)
			ticker.Start()
			defer ticker.Stop()

			// 注册对象
			for i := range 100 {
				name := fmt.Sprintf("obj-%d", i)
				mock := newMockTickable(time.Now().Add(time.Duration(i*10) * time.Millisecond))
				ticker.Register(name, mock)
			}

			b.ResetTimer()
			for b.Loop() {
				ticker.Wakeup()
			}
		})
	}
}
