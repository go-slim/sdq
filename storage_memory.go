package sdq

import (
	"context"
	"sync"
)

// MemoryStorage 内存存储实现
// 用于测试和不需要持久化的场景
type MemoryStorage struct {
	mu     sync.RWMutex
	metas  map[uint64]*JobMeta // 任务元数据
	bodies map[uint64][]byte   // 任务 Body
}

// NewMemoryStorage 创建内存存储
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		metas:  make(map[uint64]*JobMeta),
		bodies: make(map[uint64][]byte),
	}
}

// SaveJob 保存任务（元数据 + Body）
func (s *MemoryStorage) SaveJob(ctx context.Context, meta *JobMeta, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metas[meta.ID]; exists {
		return ErrJobExists
	}

	// 保存元数据（克隆）
	s.metas[meta.ID] = meta.Clone()

	// 保存 Body（复制）
	if len(body) > 0 {
		bodyCopy := make([]byte, len(body))
		copy(bodyCopy, body)
		s.bodies[meta.ID] = bodyCopy
	}

	return nil
}

// UpdateJobMeta 更新任务元数据
func (s *MemoryStorage) UpdateJobMeta(ctx context.Context, meta *JobMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metas[meta.ID]; !exists {
		return ErrNotFound
	}

	// 更新元数据（克隆）
	s.metas[meta.ID] = meta.Clone()

	return nil
}

// GetJobMeta 获取任务元数据
func (s *MemoryStorage) GetJobMeta(ctx context.Context, id uint64) (*JobMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, ok := s.metas[id]
	if !ok {
		return nil, ErrNotFound
	}

	return meta.Clone(), nil
}

// GetJobBody 获取任务 Body
func (s *MemoryStorage) GetJobBody(ctx context.Context, id uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	body, ok := s.bodies[id]
	if !ok {
		return nil, ErrNotFound
	}

	// 返回副本
	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	return bodyCopy, nil
}

// DeleteJob 删除任务（元数据 + Body）
func (s *MemoryStorage) DeleteJob(ctx context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metas[id]; !exists {
		return ErrNotFound
	}

	delete(s.metas, id)
	delete(s.bodies, id)

	return nil
}

// ScanJobMeta 扫描任务元数据
func (s *MemoryStorage) ScanJobMeta(ctx context.Context, filter *JobMetaFilter) (*JobMetaList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 收集所有符合条件的元数据
	var allMetas []*JobMeta

	for _, meta := range s.metas {
		// 过滤 Topic
		if filter != nil && filter.Topic != "" && meta.Topic != filter.Topic {
			continue
		}

		// 过滤 State
		if filter != nil && filter.State != nil && meta.State != *filter.State {
			continue
		}

		allMetas = append(allMetas, meta.Clone())
	}

	total := len(allMetas)

	// 应用 Offset 和 Limit
	var resultMetas []*JobMeta
	hasMore := false
	nextCursor := uint64(0)

	if filter != nil {
		// Offset 分页
		if filter.Offset > 0 {
			if filter.Offset >= len(allMetas) {
				allMetas = nil
			} else {
				allMetas = allMetas[filter.Offset:]
			}
		}

		// Limit
		if filter.Limit > 0 && len(allMetas) > filter.Limit {
			resultMetas = allMetas[:filter.Limit]
			hasMore = true
			if len(resultMetas) > 0 {
				nextCursor = resultMetas[len(resultMetas)-1].ID
			}
		} else {
			resultMetas = allMetas
		}
	} else {
		resultMetas = allMetas
	}

	return &JobMetaList{
		Metas:      resultMetas,
		Total:      total,
		HasMore:    hasMore,
		NextCursor: nextCursor,
	}, nil
}

// CountJobs 统计任务数量
func (s *MemoryStorage) CountJobs(ctx context.Context, filter *JobMetaFilter) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0

	for _, meta := range s.metas {
		// 过滤 Topic
		if filter != nil && filter.Topic != "" && meta.Topic != filter.Topic {
			continue
		}

		// 过滤 State
		if filter != nil && filter.State != nil && meta.State != *filter.State {
			continue
		}

		count++
	}

	return count, nil
}

// GetMaxJobID 获取最大任务 ID
func (s *MemoryStorage) GetMaxJobID(ctx context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	maxID := uint64(0)
	for id := range s.metas {
		if id > maxID {
			maxID = id
		}
	}

	return maxID, nil
}

// Close 关闭存储
func (s *MemoryStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 清空数据
	s.metas = nil
	s.bodies = nil

	return nil
}

// Stats 返回存储统计信息
func (s *MemoryStorage) Stats(ctx context.Context) (*StorageStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &StorageStats{
		TotalJobs: int64(len(s.metas)),
	}

	// 统计不同 topic 数量
	topics := make(map[string]bool)
	var metaSize int64
	var bodySize int64

	for id, meta := range s.metas {
		topics[meta.Topic] = true
		metaSize += 200 // 估算每个 JobMeta 约 200 字节

		if body, ok := s.bodies[id]; ok {
			bodySize += int64(len(body))
		}
	}

	stats.TotalTopics = len(topics)
	stats.MetaSize = metaSize
	stats.BodySize = bodySize
	stats.TotalSize = metaSize + bodySize

	if stats.TotalJobs > 0 {
		stats.AvgMetaSize = metaSize / stats.TotalJobs
		stats.AvgBodySize = bodySize / stats.TotalJobs
	}

	stats.LoadedMetaSize = metaSize
	stats.LoadedBodySize = bodySize

	return stats, nil
}
