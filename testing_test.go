package sdq

import (
	"context"
	"sync"
)

// noOpTicker 是一个无操作的 Ticker 实现，用于测试
type noOpTicker struct{}

func (t *noOpTicker) Name() string                     { return "noop" }
func (t *noOpTicker) Start()                           {}
func (t *noOpTicker) Stop()                            {}
func (t *noOpTicker) Register(name string, _ Tickable) {}
func (t *noOpTicker) Unregister(name string)           {}
func (t *noOpTicker) Wakeup()                          {}
func (t *noOpTicker) Stats() *TickerStats {
	return &TickerStats{Name: "noop"}
}

// memoryStorage 是一个简单的内存存储实现，用于测试
type memoryStorage struct {
	mu     sync.RWMutex
	metas  map[uint64]*JobMeta
	bodies map[uint64][]byte
	maxID  uint64
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{
		metas:  make(map[uint64]*JobMeta),
		bodies: make(map[uint64][]byte),
	}
}

func (s *memoryStorage) SaveJob(ctx context.Context, meta *JobMeta, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.metas[meta.ID]; exists {
		return ErrJobExists
	}
	s.metas[meta.ID] = meta.Clone()
	s.bodies[meta.ID] = append([]byte(nil), body...)
	if meta.ID > s.maxID {
		s.maxID = meta.ID
	}
	return nil
}

func (s *memoryStorage) UpdateJobMeta(ctx context.Context, meta *JobMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.metas[meta.ID]; !exists {
		return ErrNotFound
	}
	s.metas[meta.ID] = meta.Clone()
	return nil
}

func (s *memoryStorage) GetJobMeta(ctx context.Context, id uint64) (*JobMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, exists := s.metas[id]
	if !exists {
		return nil, ErrNotFound
	}
	return meta.Clone(), nil
}

func (s *memoryStorage) GetJobBody(ctx context.Context, id uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	body, exists := s.bodies[id]
	if !exists {
		return nil, ErrNotFound
	}
	return append([]byte(nil), body...), nil
}

func (s *memoryStorage) DeleteJob(ctx context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.metas, id)
	delete(s.bodies, id)
	return nil
}

func (s *memoryStorage) ScanJobMeta(ctx context.Context, filter *JobMetaFilter) (*JobMetaList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := &JobMetaList{
		Metas: make([]*JobMeta, 0, len(s.metas)),
	}
	for _, meta := range s.metas {
		if filter != nil {
			if filter.Topic != "" && meta.Topic != filter.Topic {
				continue
			}
			if filter.State != nil && meta.State != *filter.State {
				continue
			}
		}
		result.Metas = append(result.Metas, meta.Clone())
	}
	return result, nil
}

func (s *memoryStorage) CountJobs(ctx context.Context, filter *JobMetaFilter) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, meta := range s.metas {
		if filter != nil {
			if filter.Topic != "" && meta.Topic != filter.Topic {
				continue
			}
			if filter.State != nil && meta.State != *filter.State {
				continue
			}
		}
		count++
	}
	return count, nil
}

func (s *memoryStorage) GetMaxJobID(ctx context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxID, nil
}

func (s *memoryStorage) Name() string {
	return "memory"
}

func (s *memoryStorage) Stats(ctx context.Context) (*StorageStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &StorageStats{
		Name:      s.Name(),
		TotalJobs: int64(len(s.metas)),
	}, nil
}

func (s *memoryStorage) Close() error {
	return nil
}
