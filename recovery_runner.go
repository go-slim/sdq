package sdq

import (
	"context"
	"time"
)

// recoveryRunner 负责从 Storage 恢复任务
type recoveryRunner struct {
	storage Storage
	ctx     context.Context
}

// newRecoveryRunner 创建新的 recoveryRunner
func newRecoveryRunner(ctx context.Context, storage Storage) *recoveryRunner {
	return &recoveryRunner{
		storage: storage,
		ctx:     ctx,
	}
}

// RecoveryResult 恢复结果
type RecoveryResult struct {
	MaxID      uint64                // 最大任务 ID
	TopicJobs  map[string][]*JobMeta // topic -> jobs
	TotalJobs  int                   // 总任务数
	FailedJobs int                   // 失败的任务数
}

// GetMaxID 快速获取最大任务 ID（用于快速启动）
// 只查询 MAX(id)，不加载任务数据，毫秒级返回
func (rr *recoveryRunner) GetMaxID() (uint64, error) {
	return rr.storage.GetMaxJobID(rr.ctx)
}

// Recover 从 Storage 恢复所有任务
func (rr *recoveryRunner) Recover() (*RecoveryResult, error) {
	// 扫描所有任务
	result, err := rr.storage.ScanJobMeta(rr.ctx, nil)
	if err != nil {
		return nil, err
	}

	recovery := &RecoveryResult{
		TopicJobs:  make(map[string][]*JobMeta),
		TotalJobs:  len(result.Metas),
		FailedJobs: 0,
	}

	// 查找最大 ID
	maxID := uint64(0)
	for _, meta := range result.Metas {
		if meta.ID > maxID {
			maxID = meta.ID
		}
	}
	recovery.MaxID = maxID

	// 按 Topic 分组
	for _, meta := range result.Metas {
		// 预处理任务状态
		processedMeta := rr.preprocessJobMeta(meta)
		if processedMeta == nil {
			recovery.FailedJobs++
			continue
		}

		recovery.TopicJobs[meta.Topic] = append(recovery.TopicJobs[meta.Topic], processedMeta)
	}

	return recovery, nil
}

// RecoverAsync 异步恢复任务
// 返回一个 channel，调用者可以从中接收恢复进度
func (rr *recoveryRunner) RecoverAsync() <-chan *RecoveryProgress {
	progressCh := make(chan *RecoveryProgress, 10)

	go func() {
		defer close(progressCh)

		// 发送开始事件
		progressCh <- &RecoveryProgress{
			Phase: RecoveryPhaseStart,
		}

		// 执行恢复
		result, err := rr.Recover()
		if err != nil {
			progressCh <- &RecoveryProgress{
				Phase: RecoveryPhaseError,
				Error: err,
			}
			return
		}

		// 发送完成事件
		progressCh <- &RecoveryProgress{
			Phase:      RecoveryPhaseComplete,
			Result:     result,
			TotalJobs:  result.TotalJobs,
			FailedJobs: result.FailedJobs,
		}
	}()

	return progressCh
}

// RecoveryPhase 恢复阶段
type RecoveryPhase int

const (
	RecoveryPhaseStart    RecoveryPhase = iota // 开始恢复
	RecoveryPhaseScanning                      // 扫描中
	RecoveryPhaseLoading                       // 加载中
	RecoveryPhaseComplete                      // 完成
	RecoveryPhaseError                         // 错误
)

// RecoveryProgress 恢复进度
type RecoveryProgress struct {
	Phase      RecoveryPhase   // 当前阶段
	Result     *RecoveryResult // 恢复结果（仅在 Complete 阶段有值）
	TotalJobs  int             // 总任务数
	LoadedJobs int             // 已加载任务数
	FailedJobs int             // 失败任务数
	Error      error           // 错误信息（仅在 Error 阶段有值）
}

// preprocessJobMeta 预处理任务元数据
// 根据状态做必要的转换和修正
func (rr *recoveryRunner) preprocessJobMeta(meta *JobMeta) *JobMeta {
	switch meta.State {
	case StateEnqueued:
		// Enqueued 是临时状态，说明上次崩溃时任务刚创建还未完全加载
		return rr.handleEnqueuedJob(meta)

	case StateReserved:
		// 崩溃前正在处理的任务，转为 Ready 重新分配
		return rr.handleReservedJob(meta)

	case StateReady, StateDelayed, StateBuried:
		// 这些状态直接恢复
		return meta

	default:
		// 未知状态，跳过
		return nil
	}
}

// handleEnqueuedJob 处理 Enqueued 状态的任务
func (rr *recoveryRunner) handleEnqueuedJob(meta *JobMeta) *JobMeta {
	if meta.Delay > 0 {
		meta.State = StateDelayed
	} else {
		meta.State = StateReady
	}

	// 更新状态到 Storage
	if rr.storage != nil {
		_ = rr.storage.UpdateJobMeta(rr.ctx, meta)
	}

	return meta
}

// handleReservedJob 处理 Reserved 状态的任务
// 崩溃前正在处理的任务，转为 Ready 重新分配
func (rr *recoveryRunner) handleReservedJob(meta *JobMeta) *JobMeta {
	meta.State = StateReady
	meta.ReservedAt = time.Time{}
	meta.ReadyAt = time.Now()

	// 增加 Timeouts 计数（崩溃导致的隐式超时）
	meta.Timeouts++

	// 更新状态到 Storage
	if rr.storage != nil {
		_ = rr.storage.UpdateJobMeta(rr.ctx, meta)
	}

	return meta
}
