package sdq

import (
	"context"
	"time"
)

// recoveryManager 负责从 Storage 恢复任务
type recoveryManager struct {
	storage Storage
	ctx     context.Context
}

// newRecoveryManager 创建新的 recoveryManager
func newRecoveryManager(ctx context.Context, storage Storage) *recoveryManager {
	return &recoveryManager{
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
func (rm *recoveryManager) GetMaxID() (uint64, error) {
	return rm.storage.GetMaxJobID(rm.ctx)
}

// Recover 恢复任务
// 使用分页模式加载数据，避免一次性加载所有任务到内存
// 通过 callback 报告恢复进度（Start → Recovering → Complete/Error）
func (rm *recoveryManager) Recover(callback RecoveryCallback) {
	// 通过回调报告开始事件
	callback(&RecoveryProgress{
		Phase: RecoveryPhaseStart,
	})

	// 初始化恢复结果
	recovery := &RecoveryResult{
		TopicJobs:  make(map[string][]*JobMeta),
		TotalJobs:  0,
		FailedJobs: 0,
	}

	// 分页参数
	var cursor uint64 = 0
	maxID := uint64(0)
	loadedJobs := 0
	isFirstBatch := true

	for {
		// 分页扫描任务
		filter := &JobMetaFilter{
			Limit:  1000,
			Cursor: cursor,
		}
		scanResult, err := rm.storage.ScanJobMeta(rm.ctx, filter)
		if err != nil {
			callback(&RecoveryProgress{
				Phase: RecoveryPhaseError,
				Error: err,
			})
			return
		}

		// 第一批有数据时，通过回调报告恢复中事件
		if isFirstBatch && len(scanResult.Metas) > 0 {
			isFirstBatch = false
			// 获取总数（如果 Storage 支持）
			totalJobs := scanResult.Total
			if totalJobs == 0 {
				totalJobs = len(scanResult.Metas)
				if scanResult.HasMore {
					// 估算总数，实际可能更多
					totalJobs = -1 // 表示未知
				}
			}
			callback(&RecoveryProgress{
				Phase:     RecoveryPhaseRecovering,
				TotalJobs: totalJobs,
			})
		}

		// 没有更多数据，退出循环
		if len(scanResult.Metas) == 0 {
			break
		}

		// 处理当前批次
		for _, meta := range scanResult.Metas {
			if meta.ID > maxID {
				maxID = meta.ID
			}

			// 预处理任务状态
			processedMeta := rm.preprocessJobMeta(meta)
			if processedMeta == nil {
				recovery.FailedJobs++
				continue
			}

			recovery.TopicJobs[meta.Topic] = append(recovery.TopicJobs[meta.Topic], processedMeta)
			loadedJobs++
		}

		recovery.TotalJobs += len(scanResult.Metas)

		// 没有更多数据，退出循环
		if !scanResult.HasMore {
			break
		}

		// 更新游标，继续下一页
		cursor = scanResult.NextCursor
	}

	recovery.MaxID = maxID

	// 通过回调报告完成
	callback(&RecoveryProgress{
		Phase:      RecoveryPhaseComplete,
		Result:     recovery,
		TotalJobs:  recovery.TotalJobs,
		LoadedJobs: loadedJobs,
		FailedJobs: recovery.FailedJobs,
	})
}

// RecoveryPhase 恢复阶段
type RecoveryPhase int

const (
	RecoveryPhaseStart      RecoveryPhase = iota // 开始恢复
	RecoveryPhaseRecovering                      // 恢复中
	RecoveryPhaseComplete                        // 完成
	RecoveryPhaseError                           // 错误
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

// RecoveryCallback 恢复进度回调函数
// 如果提供了回调，恢复过程中会调用它报告进度
type RecoveryCallback func(progress *RecoveryProgress)

// preprocessJobMeta 预处理任务元数据
// 根据状态做必要的转换和修正
func (rm *recoveryManager) preprocessJobMeta(meta *JobMeta) *JobMeta {
	switch meta.State {
	case StateEnqueued:
		// Enqueued 是临时状态，说明上次崩溃时任务刚创建还未完全加载
		return rm.handleEnqueuedJob(meta)

	case StateReserved:
		// 崩溃前正在处理的任务，转为 Ready 重新分配
		return rm.handleReservedJob(meta)

	case StateReady, StateDelayed, StateBuried:
		// 这些状态直接恢复
		return meta

	default:
		// 未知状态，跳过
		return nil
	}
}

// handleEnqueuedJob 处理 Enqueued 状态的任务
func (rm *recoveryManager) handleEnqueuedJob(meta *JobMeta) *JobMeta {
	if meta.Delay > 0 {
		meta.State = StateDelayed
	} else {
		meta.State = StateReady
	}

	// 更新状态到 Storage
	if rm.storage != nil {
		_ = rm.storage.UpdateJobMeta(rm.ctx, meta)
	}

	return meta
}

// handleReservedJob 处理 Reserved 状态的任务
// 崩溃前正在处理的任务，转为 Ready 重新分配
func (rm *recoveryManager) handleReservedJob(meta *JobMeta) *JobMeta {
	meta.State = StateReady
	meta.ReservedAt = time.Time{}
	meta.ReadyAt = time.Now()

	// 增加 Timeouts 计数（崩溃导致的隐式超时）
	meta.Timeouts++

	// 更新状态到 Storage
	if rm.storage != nil {
		_ = rm.storage.UpdateJobMeta(rm.ctx, meta)
	}

	return meta
}
