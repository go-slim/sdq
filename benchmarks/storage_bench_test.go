package benchmarks

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go-slim.dev/sdq"
	"go-slim.dev/sdq/x/memory"
	"go-slim.dev/sdq/x/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// BenchmarkStorage_SaveJob 测试 SaveJob 性能
func BenchmarkStorage_SaveJob(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return memory.New() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sqlite.New(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			storage := bm.storage()
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := []byte("benchmark test body")

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				meta := sdq.NewJobMeta(uint64(i+1), "bench-topic", 1, 0, 30*time.Second)
				err := storage.SaveJob(ctx, meta, body)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStorage_SaveJob_Parallel 测试并发 SaveJob 性能
func BenchmarkStorage_SaveJob_Parallel(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return memory.New() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sqlite.New(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			storage := bm.storage()
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := []byte("benchmark test body")
			var id atomic.Uint64

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					localID := id.Add(1)
					meta := sdq.NewJobMeta(localID, "bench-topic", 1, 0, 30*time.Second)
					err := storage.SaveJob(ctx, meta, body)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkStorage_GetJobMeta 测试 GetJobMeta 性能
func BenchmarkStorage_GetJobMeta(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return memory.New() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sqlite.New(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			storage := bm.storage()
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := []byte("benchmark test body")

			// 预先保存任务
			numJobs := 10000
			for i := 1; i <= numJobs; i++ {
				meta := sdq.NewJobMeta(uint64(i), "bench-topic", 1, 0, 30*time.Second)
				_ = storage.SaveJob(ctx, meta, body)
			}

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				id := uint64((i % numJobs) + 1)
				_, err := storage.GetJobMeta(ctx, id)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStorage_GetJobBody 测试 GetJobBody 性能
func BenchmarkStorage_GetJobBody(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return memory.New() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sqlite.New(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			storage := bm.storage()
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := []byte("benchmark test body with some content")

			// 预先保存任务
			numJobs := 10000
			for i := 1; i <= numJobs; i++ {
				meta := sdq.NewJobMeta(uint64(i), "bench-topic", 1, 0, 30*time.Second)
				_ = storage.SaveJob(ctx, meta, body)
			}

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				id := uint64((i % numJobs) + 1)
				_, err := storage.GetJobBody(ctx, id)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStorage_UpdateJobMeta 测试 UpdateJobMeta 性能 (Memory only)
func BenchmarkStorage_UpdateJobMeta(b *testing.B) {
	storage := memory.New()
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	body := []byte("benchmark test body")

	// 预先保存任务
	numJobs := 1000
	metas := make([]*sdq.JobMeta, numJobs)
	for i := 1; i <= numJobs; i++ {
		meta := sdq.NewJobMeta(uint64(i), "bench-topic", 1, 0, 30*time.Second)
		_ = storage.SaveJob(ctx, meta, body)
		metas[i-1] = meta
	}

	for i := 0; b.Loop(); i++ {
		meta := metas[i%numJobs]
		meta.Reserves++
		err := storage.UpdateJobMeta(ctx, meta)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStorage_DeleteJob 测试 DeleteJob 性能
func BenchmarkStorage_DeleteJob(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return memory.New() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sqlite.New(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			storage := bm.storage()
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := []byte("benchmark test body")

			// 预先保存足够多的任务
			for i := 1; i <= b.N; i++ {
				meta := sdq.NewJobMeta(uint64(i), "bench-topic", 1, 0, 30*time.Second)
				_ = storage.SaveJob(ctx, meta, body)
			}

			b.ResetTimer()
			for i := 1; i <= b.N; i++ {
				err := storage.DeleteJob(ctx, uint64(i))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStorage_ScanJobMeta 测试 ScanJobMeta 性能
func BenchmarkStorage_ScanJobMeta(b *testing.B) {
	benchmarks := []struct {
		name    string
		storage func() sdq.Storage
	}{
		{"Memory", func() sdq.Storage { return memory.New() }},
		{"SQLite", func() sdq.Storage {
			s, _ := sqlite.New(b.TempDir() + "/bench.db")
			return s
		}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			storage := bm.storage()
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := []byte("benchmark test body")

			// 预先保存任务
			numJobs := 10000
			for i := 1; i <= numJobs; i++ {
				meta := sdq.NewJobMeta(uint64(i), "bench-topic", 1, 0, 30*time.Second)
				_ = storage.SaveJob(ctx, meta, body)
			}

			filter := &sdq.JobMetaFilter{
				Topic: "bench-topic",
				Limit: 100,
			}

			b.ResetTimer()
			for b.Loop() {
				_, err := storage.ScanJobMeta(ctx, filter)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStorage_BodySize 测试不同 Body 大小的 Storage 性能
func BenchmarkStorage_BodySize(b *testing.B) {
	sizes := []int{100, 1024, 10 * 1024, 64 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%dB", size), func(b *testing.B) {
			storage := memory.New()
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := make([]byte, size)
			for i := range body {
				body[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.SetBytes(int64(size))
			for i := 0; b.Loop(); i++ {
				meta := sdq.NewJobMeta(uint64(i+1), "bench-topic", 1, 0, 30*time.Second)
				err := storage.SaveJob(ctx, meta, body)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSQLiteStorage_BatchSize 测试不同批处理大小的性能
func BenchmarkSQLiteStorage_BatchSize(b *testing.B) {
	batchSizes := []int{100, 500, 1000, 2000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			storage, err := sqlite.New(
				b.TempDir()+"/bench.db",
				sqlite.WithMaxBatchSize(batchSize),
			)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := []byte("benchmark test body")

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				meta := sdq.NewJobMeta(uint64(i+1), "bench-topic", 1, 0, 30*time.Second)
				err := storage.SaveJob(ctx, meta, body)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
