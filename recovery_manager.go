package sdq

import (
	"context"
	"fmt"
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

// RecoveryResult 表示一个恢复批次或完成阶段的恢复摘要。
//
// 传给恢复消费函数时，TopicJobs 只包含当前页的数据；Complete 阶段的 Result 只保留计数
// 和 MaxID，不保留 TopicJobs，以免恢复期间额外持有全部任务。
type RecoveryResult struct {
	MaxID      uint64                // 当前已扫描到的最大任务 ID
	TopicJobs  map[string][]*JobMeta // 当前批次按 topic 分组的任务
	TotalJobs  int                   // 当前批次扫描的任务数，完成摘要中为累计数
	FailedJobs int                   // 当前批次失败数，完成摘要中为累计数
}

// GetMaxID 快速获取最大任务 ID（用于快速启动）
// 只查询 MAX(id)，不加载任务数据，毫秒级返回
func (rm *recoveryManager) GetMaxID() (uint64, error) {
	return rm.storage.GetMaxJobID(rm.ctx)
}

// Recover 恢复 snapshotMaxID 以内的任务。
//
// 每一页经过预处理后立即交给 consume，避免在恢复阶段额外保留一份完整任务集合。
// snapshotMaxID 是 Queue 启动时取得的最大任务 ID；恢复过程不会读入启动后新提交的任务。
// callback 用于报告恢复进度（Start → Recovering → Complete/Error）。
func (rm *recoveryManager) Recover(
	snapshotMaxID uint64,
	callback RecoveryCallback,
	consume func(*RecoveryResult) error,
) {
	// 通过回调报告开始事件
	callback(&RecoveryProgress{
		Phase: RecoveryPhaseStart,
	})

	// 分页参数
	var cursor uint64 = 0
	maxID := uint64(0)
	loadedJobs := 0
	failedJobs := 0
	totalJobs := 0

	for snapshotMaxID != 0 {
		// 分页扫描任务
		filter := &JobMetaFilter{
			Limit:  1000,
			Cursor: cursor,
		}
		scanResult, err := rm.storage.ScanJobMeta(rm.ctx, filter)
		if err != nil {
			callback(&RecoveryProgress{
				Phase:      RecoveryPhaseError,
				TotalJobs:  totalJobs,
				LoadedJobs: loadedJobs,
				FailedJobs: failedJobs,
				Error:      err,
			})
			return
		}

		// 没有更多数据，退出循环
		if len(scanResult.Metas) == 0 {
			break
		}

		batch := &RecoveryResult{
			TopicJobs: make(map[string][]*JobMeta),
		}

		// 处理当前批次。ScanJobMeta 按 ID 升序返回，因此遇到快照上限后的任务即可结束。
		reachedSnapshotEnd := false
		batchLoadedJobs := 0
		for _, meta := range scanResult.Metas {
			if meta.ID > snapshotMaxID {
				reachedSnapshotEnd = true
				break
			}
			batch.TotalJobs++
			if meta.ID > maxID {
				maxID = meta.ID
			}

			// 预处理任务状态
			processedMeta, err := rm.preprocessJobMeta(meta)
			if err != nil {
				failedJobs++
				callback(&RecoveryProgress{
					Phase:      RecoveryPhaseError,
					TotalJobs:  totalJobs + batch.TotalJobs,
					LoadedJobs: loadedJobs,
					FailedJobs: failedJobs,
					Error:      err,
				})
				return
			}
			if processedMeta == nil {
				batch.FailedJobs++
				failedJobs++
				continue
			}

			batch.TopicJobs[meta.Topic] = append(batch.TopicJobs[meta.Topic], processedMeta)
			batchLoadedJobs++
		}

		batch.MaxID = maxID
		totalJobs += batch.TotalJobs
		if batch.TotalJobs > 0 && consume != nil {
			if err := consume(batch); err != nil {
				callback(&RecoveryProgress{
					Phase:      RecoveryPhaseError,
					TotalJobs:  totalJobs,
					LoadedJobs: loadedJobs,
					FailedJobs: failedJobs,
					Error:      err,
				})
				return
			}
			loadedJobs += batchLoadedJobs
		}
		if batch.TotalJobs > 0 {
			callback(&RecoveryProgress{
				Phase:      RecoveryPhaseRecovering,
				TotalJobs:  totalJobs,
				LoadedJobs: loadedJobs,
				FailedJobs: failedJobs,
			})
		}

		// 没有更多数据，退出循环
		if reachedSnapshotEnd || !scanResult.HasMore {
			break
		}

		// 防御不符合 Storage 游标契约的实现，避免后台恢复永久读取同一页。
		if scanResult.NextCursor <= cursor {
			err := fmt.Errorf(
				"sdq: recovery cursor did not advance: current=%d next=%d",
				cursor,
				scanResult.NextCursor,
			)
			callback(&RecoveryProgress{
				Phase:      RecoveryPhaseError,
				TotalJobs:  totalJobs,
				LoadedJobs: loadedJobs,
				FailedJobs: failedJobs,
				Error:      err,
			})
			return
		}

		// 更新游标，继续下一页。
		cursor = scanResult.NextCursor
	}

	// 通过回调报告完成
	callback(&RecoveryProgress{
		Phase: RecoveryPhaseComplete,
		Result: &RecoveryResult{
			MaxID:      maxID,
			TotalJobs:  totalJobs,
			FailedJobs: failedJobs,
		},
		TotalJobs:  totalJobs,
		LoadedJobs: loadedJobs,
		FailedJobs: failedJobs,
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

// RecoveryProgress 是后台恢复的一个进度快照。
//
// TotalJobs、LoadedJobs 和 FailedJobs 在 Recovering、Complete、Error 阶段都是截至当前的
// 累计值。LoadedJobs 表示已成功交给 Queue 应用的任务数。Result 仅在 Complete 阶段
// 有值，并且只包含摘要，不包含所有已恢复任务。
type RecoveryProgress struct {
	Phase      RecoveryPhase   // 当前阶段
	Result     *RecoveryResult // 恢复结果（仅在 Complete 阶段有值）
	TotalJobs  int             // 总任务数
	LoadedJobs int             // 已加载任务数
	FailedJobs int             // 失败任务数
	Error      error           // 错误信息（仅在 Error 阶段有值）
}

// RecoveryCallback 接收后台恢复进度。
//
// Queue 会在单个恢复 goroutine 中串行调用回调，并且第一次调用可能早于启动方法返回。
// 回调应尽快返回，不应调用同一 Queue 的生命周期或运行期方法，也不应 panic。
type RecoveryCallback func(progress *RecoveryProgress)

// preprocessJobMeta 预处理任务元数据
// 根据状态做必要的转换和修正
func (rm *recoveryManager) preprocessJobMeta(meta *JobMeta) (*JobMeta, error) {
	switch meta.State {
	case StateEnqueued:
		// Enqueued 是临时状态，说明上次崩溃时任务刚创建还未完全加载
		return rm.handleEnqueuedJob(meta)

	case StateReserved:
		// 崩溃前正在处理的任务，转为 Ready 重新分配
		return rm.handleReservedJob(meta)

	case StateReady, StateDelayed, StateBuried:
		// 这些状态直接恢复
		return meta, nil

	default:
		// 未知状态，跳过
		return nil, nil
	}
}

// handleEnqueuedJob 处理 Enqueued 状态的任务
func (rm *recoveryManager) handleEnqueuedJob(meta *JobMeta) (*JobMeta, error) {
	if meta.Delay > 0 {
		meta.State = StateDelayed
	} else {
		meta.State = StateReady
	}

	// 先让 Storage 接受状态更新，再把任务交给内存调度。某些 Storage 会在
	// UpdateJobMeta 内部异步批处理，nil 只代表更新已被接受，最终落盘由 Storage 保证。
	if err := rm.storage.UpdateJobMeta(rm.ctx, meta); err != nil {
		return nil, err
	}

	return meta, nil
}

// handleReservedJob 处理 Reserved 状态的任务
// 崩溃前正在处理的任务，转为 Ready 重新分配
func (rm *recoveryManager) handleReservedJob(meta *JobMeta) (*JobMeta, error) {
	meta.State = StateReady
	meta.ReservedAt = time.Time{}
	meta.ReadyAt = time.Now()

	// 增加 Timeouts 计数（崩溃导致的隐式超时）
	meta.Timeouts++

	if err := rm.storage.UpdateJobMeta(rm.ctx, meta); err != nil {
		return nil, err
	}

	return meta, nil
}
