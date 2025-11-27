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

		// --- 任务处理 ---
		task, ok := w.handlers[job.Meta.Topic]
		if !ok {
			// 没有找到处理器，直接埋葬
			// TODO: Log this critical error
			_ = job.Bury(job.Meta.Priority)
			continue
		}

		// 执行任务处理器
		if err := w.handleJob(job, task); err != nil {
			// --- 失败处理逻辑 ---
			// TODO: Log the handler error

			// 检查是否还有重试次数
			releases := job.Meta.Releases
			maxRetries := task.config.MaxRetries

			if releases < maxRetries {
				// 还有重试机会，Release 任务并设置指数退避延迟
				const BASE_RETRY_DELAY = 5 * time.Second
				// 计算延迟: 5s, 10s, 20s, 40s, ...
				delay := BASE_RETRY_DELAY * time.Duration(1<<uint(releases))
				_ = job.Release(job.Meta.Priority, delay)
			} else {
				// 达到最大重试次数，埋葬任务
				_ = job.Bury(job.Meta.Priority)
			}
		} else {
			// --- 成功处理逻辑 ---
			_ = job.Delete()
		}
	}
}

// handleJob 处理单个任务
func (w *Worker) handleJob(job *sdq.Job, t *Task) (err error) {
	// Panic 恢复（双重保护）
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("job processing panic: %v", r)
		}
	}()

	// 获取任务数据
	body, e := job.GetBody()
	if e != nil {
		return fmt.Errorf("failed to get job body: %w", e)
	}

	// --- TTR 控制 ---
	// 创建一个比服务器 TTR 略短的上下文，以确保 Worker 能抢先处理超时
	// 默认 1 秒的安全边际
	const safetyMargin = 1 * time.Second
	timeout := job.Meta.TTR - safetyMargin
	if timeout <= 0 {
		// 如果 TTR 太短，至少给一点点执行时间
		timeout = job.Meta.TTR
	}

	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()

	// 调用处理函数
	return t.handler.Call(ctx, body)
}
