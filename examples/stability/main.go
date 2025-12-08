// stability 是一个带 Inspector 监控的稳定性测试程序
//
// 使用方式:
//
//	go run ./examples/stability
//	go run ./examples/stability -duration 1h
//	go run ./examples/stability -duration 24h -addr :9090
//
// 然后在浏览器中访问 http://localhost:8686 查看监控仪表盘
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
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

func main() {
	var (
		duration  = flag.Duration("duration", 5*time.Minute, "test duration")
		addr      = flag.String("addr", ":8686", "inspector HTTP address")
		producers = flag.Int("producers", 4, "number of producers")
		consumers = flag.Int("consumers", 4, "number of consumers")
		topics    = flag.Int("topics", 10, "number of topics")
	)
	flag.Parse()

	slog.Info("starting stability test",
		slog.Duration("duration", *duration),
		slog.String("inspector", "http://localhost"+*addr),
	)

	// 创建队列
	config := sdq.DefaultConfig()
	config.Storage = sdq.NewMemoryStorage()

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
		putCount     atomic.Uint64
		reserveCount atomic.Uint64
		deleteCount  atomic.Uint64
		errorCount   atomic.Uint64
	)

	// 背压控制
	const maxPendingJobs = 10000

	// 启动生产者和消费者
	var wg sync.WaitGroup

	// 生产者
	for i := 0; i < *producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			topic := fmt.Sprintf("topic-%d", id%*topics)
			body := []byte("stability test payload")

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

	// 消费者
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
				reserves := reserveCount.Load()
				deletes := deleteCount.Load()
				errors := errorCount.Load()
				pending := puts - deletes

				slog.Info("progress",
					slog.Duration("elapsed", elapsed.Round(time.Second)),
					slog.Uint64("puts", puts),
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
