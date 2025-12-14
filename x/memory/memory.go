// Package memory provides an in-memory storage implementation.
// It is suitable for testing and scenarios that don't require persistence.
package memory

import (
	"context"
	"sync"

	"go-slim.dev/sdq"
)

// Compile-time interface check
var _ sdq.Storage = (*Storage)(nil)

// Storage is an in-memory storage implementation.
// It is suitable for testing and scenarios that don't require persistence.
type Storage struct {
	mu     sync.RWMutex
	metas  map[uint64]*sdq.JobMeta // Job metadata
	bodies map[uint64][]byte       // Job bodies
}

// Name returns the storage name.
func (s *Storage) Name() string {
	return "memory"
}

// New creates a new in-memory storage.
func New() *Storage {
	return &Storage{
		metas:  make(map[uint64]*sdq.JobMeta),
		bodies: make(map[uint64][]byte),
	}
}

// SaveJob saves a job (metadata + body).
func (s *Storage) SaveJob(ctx context.Context, meta *sdq.JobMeta, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metas[meta.ID]; exists {
		return sdq.ErrJobExists
	}

	// Save metadata (clone)
	s.metas[meta.ID] = meta.Clone()

	// Save body (copy)
	if len(body) > 0 {
		bodyCopy := make([]byte, len(body))
		copy(bodyCopy, body)
		s.bodies[meta.ID] = bodyCopy
	}

	return nil
}

// UpdateJobMeta updates job metadata.
func (s *Storage) UpdateJobMeta(ctx context.Context, meta *sdq.JobMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metas[meta.ID]; !exists {
		return sdq.ErrNotFound
	}

	// Update metadata (clone)
	s.metas[meta.ID] = meta.Clone()

	return nil
}

// GetJobMeta retrieves job metadata.
func (s *Storage) GetJobMeta(ctx context.Context, id uint64) (*sdq.JobMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, ok := s.metas[id]
	if !ok {
		return nil, sdq.ErrNotFound
	}

	return meta.Clone(), nil
}

// GetJobBody retrieves job body.
func (s *Storage) GetJobBody(ctx context.Context, id uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	body, ok := s.bodies[id]
	if !ok {
		return nil, sdq.ErrNotFound
	}

	// Return a copy
	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	return bodyCopy, nil
}

// DeleteJob deletes a job (metadata + body).
func (s *Storage) DeleteJob(ctx context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metas[id]; !exists {
		return sdq.ErrNotFound
	}

	delete(s.metas, id)
	delete(s.bodies, id)

	return nil
}

// ScanJobMeta scans job metadata.
func (s *Storage) ScanJobMeta(ctx context.Context, filter *sdq.JobMetaFilter) (*sdq.JobMetaList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all matching metadata
	var allMetas []*sdq.JobMeta

	for _, meta := range s.metas {
		// Filter by topic
		if filter != nil && filter.Topic != "" && meta.Topic != filter.Topic {
			continue
		}

		// Filter by state
		if filter != nil && filter.State != nil && meta.State != *filter.State {
			continue
		}

		allMetas = append(allMetas, meta.Clone())
	}

	total := len(allMetas)

	// Apply offset and limit
	var resultMetas []*sdq.JobMeta
	hasMore := false
	nextCursor := uint64(0)

	if filter != nil {
		// Offset pagination
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

	return &sdq.JobMetaList{
		Metas:      resultMetas,
		Total:      total,
		HasMore:    hasMore,
		NextCursor: nextCursor,
	}, nil
}

// CountJobs counts jobs.
func (s *Storage) CountJobs(ctx context.Context, filter *sdq.JobMetaFilter) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0

	for _, meta := range s.metas {
		// Filter by topic
		if filter != nil && filter.Topic != "" && meta.Topic != filter.Topic {
			continue
		}

		// Filter by state
		if filter != nil && filter.State != nil && meta.State != *filter.State {
			continue
		}

		count++
	}

	return count, nil
}

// GetMaxJobID returns the maximum job ID.
func (s *Storage) GetMaxJobID(ctx context.Context) (uint64, error) {
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

// Close closes the storage.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear data
	s.metas = nil
	s.bodies = nil

	return nil
}

// Stats returns storage statistics.
func (s *Storage) Stats(ctx context.Context) (*sdq.StorageStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &sdq.StorageStats{
		Name:      s.Name(),
		TotalJobs: int64(len(s.metas)),
	}

	// Count unique topics
	topics := make(map[string]bool)
	var metaSize int64
	var bodySize int64

	for id, meta := range s.metas {
		topics[meta.Topic] = true
		metaSize += 200 // Estimate each JobMeta is about 200 bytes

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
