// Package main 演示如何使用 webui 包提供的 HTTP API 和嵌入式前端界面
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-slim.dev/sdq"
	"go-slim.dev/sdq/x/dynsleep"
	"go-slim.dev/sdq/x/memory"
	"go-slim.dev/sdq/x/webui"
)

func main() {
	// 创建队列配置
	config := sdq.DefaultConfig()
	config.Storage = memory.New()
	config.Ticker = dynsleep.New(
		50*time.Millisecond, // 最小检查间隔
		5*time.Minute,       // 最大休眠时间
	)

	// 创建队列
	q, err := sdq.New(config)
	if err != nil {
		log.Fatal("failed to create queue:", err)
	}

	// 启动队列
	if err := q.Start(); err != nil {
		log.Fatal("failed to start queue:", err)
	}
	defer func() { _ = q.Stop() }()

	log.Println("Queue started successfully")

	// 添加一些示例任务
	if err := addSampleJobs(q); err != nil {
		log.Fatal("failed to add sample jobs:", err)
	}

	// 创建 WebUI Query 和 Handler
	query := webui.NewQuery(q)
	handler := webui.NewHandlerWithBasePath(query, "/sdq/")

	// 设置 HTTP 路由（包含 API 和嵌入的前端静态文件）
	// 方式1：使用路由前缀 /sdq
	http.Handle("/sdq/", http.StripPrefix("/sdq", handler))

	// 方式2：直接挂载到根路径（取消注释以使用）
	// http.Handle("/", handler)

	// 添加根路径的重定向到 WebUI
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/sdq/", http.StatusFound)
	})

	// 启动 HTTP 服务器
	server := &http.Server{
		Addr:         ":8081",
		Handler:      nil,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 优雅关闭
	go func() {
		log.Printf("WebUI server listening on http://localhost:8081")
		log.Printf("WebUI Dashboard: http://localhost:8081/sdq/")
		log.Printf("\nAPI Endpoints:")
		log.Printf("  - http://localhost:8081/sdq/api/overview")
		log.Printf("  - http://localhost:8081/sdq/api/topics")
		log.Printf("  - http://localhost:8081/sdq/api/topics/email/jobs")
		log.Println()

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error:", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// 优雅关闭服务器
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("server forced to shutdown:", err)
	}

	log.Println("Server stopped")
}

// addSampleJobs 添加一些示例任务用于演示
func addSampleJobs(q *sdq.Queue) error {
	log.Println("Adding sample jobs...")

	// 添加一些就绪任务
	for i := 0; i < 5; i++ {
		_, err := q.Put("email", []byte("Send email to user"), 1, 0, 30*time.Second)
		if err != nil {
			return err
		}
	}

	// 添加一些延迟任务
	for i := 0; i < 3; i++ {
		_, err := q.Put("email", []byte("Scheduled email"), 1, 10*time.Minute, 30*time.Second)
		if err != nil {
			return err
		}
	}

	// 添加 SMS 任务
	for i := 0; i < 2; i++ {
		_, err := q.Put("sms", []byte("Send SMS notification"), 2, 0, 30*time.Second)
		if err != nil {
			return err
		}
	}

	// 添加推送通知任务
	_, err := q.Put("push", []byte("Push notification"), 3, 5*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}

	// Reserve 并 bury 一个任务用于演示
	job, err := q.Reserve([]string{"email"}, 1*time.Second)
	if err == nil && job != nil {
		_ = job.Bury(1)
	}

	log.Printf("Added sample jobs successfully")
	return nil
}
