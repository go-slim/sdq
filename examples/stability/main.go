// stability 是一个带 Inspector 监控的稳定性测试程序
//
// 使用方式:
//
//	go run ./examples/stability
//	go run ./examples/stability -duration 1h
//	go run ./examples/stability -duration 24h -addr :9090
//	go run ./examples/stability -storage sqlite -db ./test.db
//	go run ./examples/stability -ticker timewheel
//
// 然后在浏览器中访问 http://localhost:8686 查看监控仪表盘
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go-slim.dev/sdq"
	"go-slim.dev/sdq/inspector"
)

// JobPayload 模拟任务数据
type JobPayload struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
	Data      string    `json:"data"`
}

func main() {
	var (
		duration  = flag.Duration("duration", 5*time.Minute, "test duration")
		addr      = flag.String("addr", ":8686", "inspector HTTP address")
		producers = flag.Int("producers", 4, "number of producers")
		consumers = flag.Int("consumers", 4, "number of consumers")
		topics    = flag.Int("topics", 10, "number of topics")
		storage   = flag.String("storage", "sqlite", "storage type: memory, sqlite")
		dbPath    = flag.String("db", "./stability.db", "sqlite database path (when storage=sqlite)")
		ticker    = flag.String("ticker", "timewheel", "ticker type: dynamic, timewheel")
	)
	flag.Parse()

	slog.Info("starting stability test",
		slog.Duration("duration", *duration),
		slog.String("inspector", "http://localhost"+*addr),
		slog.String("storage", *storage),
		slog.String("ticker", *ticker),
	)

	// 创建队列配置
	config := sdq.DefaultConfig()

	// 配置 Storage
	switch *storage {
	case "memory":
		config.Storage = sdq.NewMemoryStorage()
		slog.Info("using memory storage")
	case "sqlite":
		sqliteStorage, err := sdq.NewSQLiteStorage(*dbPath)
		if err != nil {
			slog.Error("failed to create sqlite storage", slog.Any("error", err))
			os.Exit(1)
		}
		config.Storage = sqliteStorage
		slog.Info("using sqlite storage", slog.String("path", *dbPath))
	default:
		slog.Error("unknown storage type", slog.String("storage", *storage))
		os.Exit(1)
	}

	// 配置 Ticker
	switch *ticker {
	case "dynamic":
		config.Ticker = sdq.NewDynamicSleepTicker(100*time.Millisecond, 5*time.Second)
		slog.Info("using dynamic ticker", slog.Duration("min", 100*time.Millisecond), slog.Duration("max", 5*time.Second))
	case "timewheel":
		config.Ticker = sdq.NewTimeWheelTicker(100*time.Millisecond, 3600)
		slog.Info("using timewheel ticker", slog.Duration("interval", 100*time.Millisecond), slog.Int("slots", 3600))
	default:
		slog.Error("unknown ticker type", slog.String("ticker", *ticker))
		os.Exit(1)
	}

	q, err := sdq.New(config)
	if err != nil {
		slog.Error("failed to create queue", slog.Any("error", err))
		os.Exit(1)
	}
	if err := q.Start(); err != nil {
		slog.Error("failed to start queue", slog.Any("error", err))
		os.Exit(1)
	}

	// 启动 Inspector HTTP 服务器
	insp := inspector.New(q)
	handler := inspector.NewHandler(insp)
	server := &http.Server{
		Addr:    *addr,
		Handler: handler,
	}
	go func() {
		slog.Info("inspector dashboard available", slog.String("url", "http://localhost"+*addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("inspector server error", slog.Any("error", err))
		}
	}()

	// 设置上下文
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// 处理信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("received shutdown signal")
		cancel()
	}()

	// 统计
	var (
		putCount      atomic.Uint64
		delayedCount  atomic.Uint64
		reserveCount  atomic.Uint64
		deleteCount   atomic.Uint64
		errorCount    atomic.Uint64
		longTaskCount atomic.Uint64
	)

	// 背压控制
	const maxPendingJobs = 10000

	// 启动生产者和消费者
	var wg sync.WaitGroup

	// 普通任务生产者
	for i := 0; i < *producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			topic := fmt.Sprintf("topic-%d", id%*topics)

			for {
				select {
				case <-ctx.Done():
					return
				default:
					pending := putCount.Load() - deleteCount.Load()
					if pending > maxPendingJobs {
						time.Sleep(10 * time.Millisecond)
						continue
					}

					payload := JobPayload{
						ID:        fmt.Sprintf("job-%d-%d", id, putCount.Load()),
						Type:      "normal",
						CreatedAt: time.Now(),
						Data:      fmt.Sprintf("Normal task from producer %d", id),
					}
					body, _ := json.Marshal(payload)

					_, err := q.Put(topic, body, 1, 0, 30*time.Second)
					if err != nil {
						errorCount.Add(1)
					} else {
						putCount.Add(1)
					}
				}
			}
		}(i)
	}

	// 延迟任务生产者
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// 随机延迟 1-30 秒
				delay := time.Duration(1+rand.Intn(30)) * time.Second
				topic := fmt.Sprintf("delayed-topic-%d", rand.Intn(3))

				payload := JobPayload{
					ID:        fmt.Sprintf("delayed-%d", delayedCount.Load()),
					Type:      "delayed",
					CreatedAt: time.Now(),
					Data:      fmt.Sprintf("Delayed task, will be ready in %v", delay),
				}
				body, _ := json.Marshal(payload)

				_, err := q.Put(topic, body, 1, delay, 60*time.Second)
				if err != nil {
					errorCount.Add(1)
				} else {
					delayedCount.Add(1)
					putCount.Add(1)
				}
			}
		}
	}()

	// 长时间运行任务生产者（模拟需要较长 TTR 的任务）
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				topic := "long-running"

				payload := JobPayload{
					ID:        fmt.Sprintf("long-%d", longTaskCount.Load()),
					Type:      "long-running",
					CreatedAt: time.Now(),
					Data:      "This task simulates a long-running job that takes 5-15 seconds to process",
				}
				body, _ := json.Marshal(payload)

				// 长 TTR（2 分钟）
				_, err := q.Put(topic, body, 2, 0, 2*time.Minute)
				if err != nil {
					errorCount.Add(1)
				} else {
					longTaskCount.Add(1)
					putCount.Add(1)
				}
			}
		}
	}()

	// 普通消费者
	topicList := make([]string, *topics)
	for i := range topicList {
		topicList[i] = fmt.Sprintf("topic-%d", i)
	}

	for i := 0; i < *consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					job, err := q.Reserve(topicList, 100*time.Millisecond)
					if err == nil {
						reserveCount.Add(1)
						if err := job.Delete(); err == nil {
							deleteCount.Add(1)
						}
					}
				}
			}
		}()
	}

	// 延迟任务消费者
	delayedTopics := []string{"delayed-topic-0", "delayed-topic-1", "delayed-topic-2"}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				job, err := q.Reserve(delayedTopics, 100*time.Millisecond)
				if err == nil {
					reserveCount.Add(1)
					// 模拟处理延迟任务
					time.Sleep(50 * time.Millisecond)
					if err := job.Delete(); err == nil {
						deleteCount.Add(1)
					}
				}
			}
		}
	}()

	// 长时间运行任务消费者
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				job, err := q.Reserve([]string{"long-running"}, 100*time.Millisecond)
				if err == nil {
					reserveCount.Add(1)

					// 模拟长时间处理（5-15秒）
					processingTime := time.Duration(5+rand.Intn(10)) * time.Second

					// 在处理过程中定期 touch 以延长 TTR
					done := make(chan struct{})
					go func() {
						touchTicker := time.NewTicker(30 * time.Second)
						defer touchTicker.Stop()
						for {
							select {
							case <-done:
								return
							case <-touchTicker.C:
								_ = job.Touch()
							}
						}
					}()

					// 模拟处理
					select {
					case <-ctx.Done():
						close(done)
						return
					case <-time.After(processingTime):
					}
					close(done)

					if err := job.Delete(); err == nil {
						deleteCount.Add(1)
					}
				}
			}
		}
	}()

	// 启动状态报告
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		start := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(start)
				puts := putCount.Load()
				delayed := delayedCount.Load()
				longTasks := longTaskCount.Load()
				reserves := reserveCount.Load()
				deletes := deleteCount.Load()
				errors := errorCount.Load()
				pending := puts - deletes

				slog.Info("progress",
					slog.Duration("elapsed", elapsed.Round(time.Second)),
					slog.Uint64("puts", puts),
					slog.Uint64("delayed", delayed),
					slog.Uint64("long_tasks", longTasks),
					slog.Uint64("reserves", reserves),
					slog.Uint64("deletes", deletes),
					slog.Uint64("pending", pending),
					slog.Uint64("errors", errors),
					slog.Float64("puts/s", float64(puts)/elapsed.Seconds()),
				)
			}
		}
	}()

	// 等待完成
	wg.Wait()

	// 最终统计
	slog.Info("test completed",
		slog.Uint64("total_puts", putCount.Load()),
		slog.Uint64("total_delayed", delayedCount.Load()),
		slog.Uint64("total_long_tasks", longTaskCount.Load()),
		slog.Uint64("total_reserves", reserveCount.Load()),
		slog.Uint64("total_deletes", deleteCount.Load()),
		slog.Uint64("total_errors", errorCount.Load()),
	)

	// 关闭
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	_ = q.Stop()
}
