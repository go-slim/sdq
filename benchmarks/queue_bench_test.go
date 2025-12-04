package benchmarks

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go-slim.dev/sdq"

	_ "github.com/mattn/go-sqlite3"
)

// noOpTicker 是一个无操作的 Ticker 实现，用于基准测试
type noOpTicker struct{}

func newNoOpTicker() *noOpTicker                                  { return &noOpTicker{} }
func (t *noOpTicker) Start()                                      {}
func (t *noOpTicker) Stop()                                       {}
func (t *noOpTicker) Register(name string, tickable sdq.Tickable) {}
func (t *noOpTicker) Unregister(name string)                      {}
func (t *noOpTicker) Wakeup()                                     {}
func (t *noOpTicker) Stats() *sdq.TickerStats {
	return &sdq.TickerStats{Mode: "noop"}
}

// BenchmarkQueue_Put 测试 Put 操作性能
func BenchmarkQueue_Put(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return sdq.NewMemoryStorage() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sdq.NewSQLiteStorage(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			config := sdq.DefaultConfig()
			config.Storage = bm.storage()
			config.Ticker = newNoOpTicker()

			q, err := sdq.New(config)
			if err != nil {
				b.Fatal(err)
			}
			if err := q.Start(); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = q.Stop() }()

			body := []byte("benchmark test body")

			b.ResetTimer()
			for b.Loop() {
				_, err := q.Put("bench-topic", body, 1, 0, 30*time.Second)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkQueue_Put_Parallel 测试并发 Put 操作性能
func BenchmarkQueue_Put_Parallel(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return sdq.NewMemoryStorage() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sdq.NewSQLiteStorage(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			config := sdq.DefaultConfig()
			config.Storage = bm.storage()
			config.Ticker = newNoOpTicker()

			q, err := sdq.New(config)
			if err != nil {
				b.Fatal(err)
			}
			if err := q.Start(); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = q.Stop() }()

			body := []byte("benchmark test body")

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := q.Put("bench-topic", body, 1, 0, 30*time.Second)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkQueue_Reserve 测试 Reserve 操作性能
func BenchmarkQueue_Reserve(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return sdq.NewMemoryStorage() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sdq.NewSQLiteStorage(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			config := sdq.DefaultConfig()
			config.Storage = bm.storage()
			config.Ticker = newNoOpTicker()

			q, err := sdq.New(config)
			if err != nil {
				b.Fatal(err)
			}
			if err := q.Start(); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = q.Stop() }()

			// 预先放入足够多的任务
			body := []byte("benchmark test body")
			for i := 0; i < b.N; i++ {
				_, _ = q.Put("bench-topic", body, 1, 0, 30*time.Second)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				job, err := q.Reserve([]string{"bench-topic"}, 1*time.Second)
				if err != nil {
					b.Fatal(err)
				}
				_ = job.Delete()
			}
		})
	}
}

// BenchmarkQueue_PutReserveDelete 测试完整工作流性能
func BenchmarkQueue_PutReserveDelete(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return sdq.NewMemoryStorage() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sdq.NewSQLiteStorage(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			config := sdq.DefaultConfig()
			config.Storage = bm.storage()
			config.Ticker = newNoOpTicker()

			q, err := sdq.New(config)
			if err != nil {
				b.Fatal(err)
			}
			if err := q.Start(); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = q.Stop() }()

			body := []byte("benchmark test body")

			b.ResetTimer()
			for b.Loop() {
				_, err := q.Put("bench-topic", body, 1, 0, 30*time.Second)
				if err != nil {
					b.Fatal(err)
				}

				job, err := q.Reserve([]string{"bench-topic"}, 1*time.Second)
				if err != nil {
					b.Fatal(err)
				}

				if err := job.Delete(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkQueue_MultiTopic 测试多 Topic 场景性能
func BenchmarkQueue_MultiTopic(b *testing.B) {
	topicCounts := []int{1, 10, 100}

	for _, numTopics := range topicCounts {
		b.Run(fmt.Sprintf("Topics_%d", numTopics), func(b *testing.B) {
			config := sdq.DefaultConfig()
			config.Storage = sdq.NewMemoryStorage()
			config.Ticker = newNoOpTicker()

			q, err := sdq.New(config)
			if err != nil {
				b.Fatal(err)
			}
			if err := q.Start(); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = q.Stop() }()

			body := []byte("benchmark test body")
			topics := make([]string, numTopics)
			for i := range topics {
				topics[i] = fmt.Sprintf("topic-%d", i)
			}

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				topic := topics[i%numTopics]
				_, err := q.Put(topic, body, 1, 0, 30*time.Second)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkQueue_Priority 测试优先级排序性能
func BenchmarkQueue_Priority(b *testing.B) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = newNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		b.Fatal(err)
	}
	if err := q.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	body := []byte("benchmark test body")

	// 预先放入不同优先级的任务
	for i := 0; i < b.N; i++ {
		priority := uint32(i % 1000) // 0-999 的优先级
		_, _ = q.Put("bench-topic", body, priority, 0, 30*time.Second)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job, err := q.Reserve([]string{"bench-topic"}, 1*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		_ = job.Delete()
	}
}

// BenchmarkQueue_BodySize 测试不同 Body 大小的性能影响
func BenchmarkQueue_BodySize(b *testing.B) {
	sizes := []int{100, 1024, 10 * 1024, 64 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%dB", size), func(b *testing.B) {
			config := sdq.DefaultConfig()
			config.Storage = sdq.NewMemoryStorage()
			config.Ticker = newNoOpTicker()

			q, err := sdq.New(config)
			if err != nil {
				b.Fatal(err)
			}
			if err := q.Start(); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = q.Stop() }()

			body := make([]byte, size)
			for i := range body {
				body[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.SetBytes(int64(size))
			for b.Loop() {
				_, err := q.Put("bench-topic", body, 1, 0, 30*time.Second)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkQueue_ConcurrentProducerConsumer 测试生产者-消费者并发模式
func BenchmarkQueue_ConcurrentProducerConsumer(b *testing.B) {
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()
	config.Ticker = newNoOpTicker()

	q, err := sdq.New(config)
	if err != nil {
		b.Fatal(err)
	}
	if err := q.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	body := []byte("benchmark test body")
	var produced, consumed int64
	var wg sync.WaitGroup

	b.ResetTimer()

	// 生产者
	wg.Go(func() {
		for b.Loop() {
			_, err := q.Put("bench-topic", body, 1, 0, 30*time.Second)
			if err == nil {
				produced++
			}
		}
	})

	// 消费者
	wg.Go(func() {
		for consumed < int64(b.N) {
			job, err := q.Reserve([]string{"bench-topic"}, 100*time.Millisecond)
			if err == nil {
				_ = job.Delete()
				consumed++
			}
		}
	})

	wg.Wait()
}
