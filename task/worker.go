package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go-slim.dev/sdq"
)

// Worker 任务工作器，用于处理任务
type Worker struct {
	queue    *sdq.Queue
	topics   []string
	handlers map[string]*Task
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewWorker 创建任务工作器
func NewWorker(q *sdq.Queue, taskNames ...string) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	handlers := make(map[string]*Task)
	for _, name := range taskNames {
		if t, ok := Get(name); ok {
			handlers[name] = t
			t.SetQueue(q)
		}
	}

	return &Worker{
		queue:    q,
		topics:   taskNames,
		handlers: handlers,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start 启动工作器
func (w *Worker) Start(workers int) error {
	if workers <= 0 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		w.wg.Add(1)
		go w.worker()
	}

	return nil
}

// Stop 停止工作器
func (w *Worker) Stop() {
	w.cancel()
	w.wg.Wait()
}

// worker 工作协程
func (w *Worker) worker() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		// Reserve 任务
		job, err := w.queue.Reserve(w.topics, 5*time.Second)
		if err != nil {
			if err == sdq.ErrTimeout {
				continue
			}
			// TODO: 记录错误
			time.Sleep(time.Second)
			continue
		}

		// 处理任务
		if err := w.handleJob(job); err != nil {
			// TODO: 重试逻辑
			_ = job.Bury(job.Meta.Priority)
		} else {
			_ = job.Delete()
		}
	}
}

// handleJob 处理单个任务
func (w *Worker) handleJob(job *sdq.Job) error {
	t, ok := w.handlers[job.Meta.Topic]
	if !ok {
		return fmt.Errorf("no handler for task %q", job.Meta.Topic)
	}

	// 获取任务数据
	body, err := job.GetBody()
	if err != nil {
		return fmt.Errorf("failed to get job body: %w", err)
	}

	// 调用处理函数
	return t.handler.Call(w.ctx, body)
}
