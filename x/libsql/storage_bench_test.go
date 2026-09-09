package libsql

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go-slim.dev/sdq"
)

func BenchmarkStorageGetJobBodySize(b *testing.B) {
	for _, size := range []int{100, 1024, 10 * 1024, 64 * 1024} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			storage, err := New(b.TempDir() + "/bench.db")
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			meta := sdq.NewJobMeta(1, "body", 1, 0, time.Minute)
			if err := storage.SaveJob(ctx, meta, make([]byte, size)); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				if _, err := storage.GetJobBody(ctx, meta.ID); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStorageSaveJobParallelConnections(b *testing.B) {
	for _, connections := range []int{1, 2, 4, 10} {
		b.Run(fmt.Sprintf("%d", connections), func(b *testing.B) {
			storage, err := New(
				b.TempDir()+"/bench.db",
				WithMaxOpenConns(connections),
				WithMaxIdleConns(connections),
			)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := []byte("benchmark test body")
			var id atomic.Uint64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(parallel *testing.PB) {
				for parallel.Next() {
					jobID := id.Add(1)
					meta := sdq.NewJobMeta(jobID, "parallel", 1, 0, time.Minute)
					if err := storage.SaveJob(ctx, meta, body); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkStorageGetJobBodyParallelConnections(b *testing.B) {
	for _, connections := range []int{1, 2, 4, 10} {
		b.Run(fmt.Sprintf("%d", connections), func(b *testing.B) {
			storage, err := New(
				b.TempDir()+"/bench.db",
				WithMaxOpenConns(connections),
				WithMaxIdleConns(connections),
			)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			body := make([]byte, 1024)
			const jobCount = 1000
			for id := 1; id <= jobCount; id++ {
				meta := sdq.NewJobMeta(uint64(id), "parallel", 1, 0, time.Minute)
				if err := storage.SaveJob(ctx, meta, body); err != nil {
					b.Fatal(err)
				}
			}

			var sequence atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(parallel *testing.PB) {
				for parallel.Next() {
					id := sequence.Add(1)%jobCount + 1
					if _, err := storage.GetJobBody(ctx, id); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkStorageCombinedIndex(b *testing.B) {
	for _, keepCombinedIndex := range []bool{false, true} {
		name := "Separate"
		if keepCombinedIndex {
			name = "Combined"
		}
		b.Run(name, func(b *testing.B) {
			storage, err := New(b.TempDir() + "/bench.db")
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			const jobCount = 10_000
			for id := 1; id <= jobCount; id++ {
				meta := sdq.NewJobMeta(
					uint64(id),
					fmt.Sprintf("topic-%d", id%100),
					1,
					0,
					time.Minute,
				)
				meta.State = sdq.State(id % 5)
				if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
					b.Fatal(err)
				}
			}
			if keepCombinedIndex {
				if _, err := storage.db.ExecContext(
					ctx,
					"CREATE INDEX idx_job_meta_topic_state_id ON job_meta(topic, state, id)",
				); err != nil {
					b.Fatal(err)
				}
			}

			state := sdq.State(2)
			filter := &sdq.JobMetaFilter{
				Topic: "topic-42",
				State: &state,
				Limit: 100,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := storage.ScanJobMeta(ctx, filter); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStorageUpdateCombinedIndex(b *testing.B) {
	for _, keepCombinedIndex := range []bool{false, true} {
		name := "Separate"
		if keepCombinedIndex {
			name = "Combined"
		}
		b.Run(name, func(b *testing.B) {
			storage, err := New(b.TempDir() + "/bench.db")
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			const jobCount = 1000
			metas := make([]*sdq.JobMeta, jobCount)
			for id := 1; id <= jobCount; id++ {
				meta := sdq.NewJobMeta(
					uint64(id),
					fmt.Sprintf("topic-%d", id%100),
					1,
					0,
					time.Minute,
				)
				if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
					b.Fatal(err)
				}
				metas[id-1] = meta
			}
			if keepCombinedIndex {
				if _, err := storage.db.ExecContext(
					ctx,
					"CREATE INDEX idx_job_meta_topic_state_id ON job_meta(topic, state, id)",
				); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for sequence := 0; b.Loop(); sequence++ {
				meta := metas[sequence%jobCount].Clone()
				meta.Reserves = sequence + 1
				if err := storage.UpdateJobMeta(ctx, meta); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStorageUpdateMVCCThreshold(b *testing.B) {
	for _, threshold := range []int{1000, 5000, 10_000, -1} {
		b.Run(fmt.Sprintf("%d", threshold), func(b *testing.B) {
			storage, err := New(
				b.TempDir()+"/bench.db",
				WithMVCCCheckpointThreshold(threshold),
				WithMVCCGCThreshold(threshold),
			)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = storage.Close() }()

			ctx := context.Background()
			const jobCount = 1000
			metas := make([]*sdq.JobMeta, jobCount)
			for id := 1; id <= jobCount; id++ {
				meta := sdq.NewJobMeta(uint64(id), "threshold", 1, 0, time.Minute)
				if err := storage.SaveJob(ctx, meta, []byte("body")); err != nil {
					b.Fatal(err)
				}
				metas[id-1] = meta
			}

			b.ReportAllocs()
			b.ResetTimer()
			for sequence := 0; b.Loop(); sequence++ {
				meta := metas[sequence%jobCount].Clone()
				meta.Reserves = sequence + 1
				if err := storage.UpdateJobMeta(ctx, meta); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
