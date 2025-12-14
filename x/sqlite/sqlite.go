// Package sqlite provides a SQLite storage implementation.
// It includes built-in batch update and batch save buffering mechanisms
// that automatically merge concurrent calls.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go-slim.dev/sdq"
)

// Compile-time interface check
var _ sdq.Storage = (*Storage)(nil)

// saveJobRequest represents a SaveJob request.
type saveJobRequest struct {
	meta *sdq.JobMeta
	body []byte
	done chan error // Channel to notify caller of completion
}

// Storage is a SQLite storage implementation.
// It includes built-in batch update and batch save buffering mechanisms
// that automatically merge concurrent calls.
type Storage struct {
	db     *sql.DB
	dbPath string
	mu     sync.RWMutex

	// Batch update buffering mechanism (for UpdateJobMeta)
	updateBuffer   map[uint64]*sdq.JobMeta // Buffer for pending job metadata updates
	updateBufferMu sync.Mutex
	updateChan     chan *sdq.JobMeta // Update channel

	// Batch save buffering mechanism (for SaveJob)
	// SaveJob is synchronous, but internally merges concurrent requests
	saveChan chan *saveJobRequest // Save request channel

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closed  bool       // Whether storage is closed
	closeMu sync.Mutex // Protects closed field

	// Batch operation configuration
	maxBatchSize  int // Maximum batch operation count, default 1000
	maxBatchBytes int // Maximum batch save bytes, default 16MB
}

// Option is a configuration option for SQLite storage.
type Option func(*Storage)

// WithMaxBatchSize sets the maximum batch operation count.
func WithMaxBatchSize(size int) Option {
	return func(s *Storage) {
		if size > 0 {
			s.maxBatchSize = size
		}
	}
}

// WithMaxBatchBytes sets the maximum batch save bytes.
func WithMaxBatchBytes(bytes int) Option {
	return func(s *Storage) {
		if bytes > 0 {
			s.maxBatchBytes = bytes
		}
	}
}

// Name returns the storage name.
func (s *Storage) Name() string {
	return "sqlite"
}

// New creates a new SQLite storage.
func New(dbPath string, opts ...Option) (*Storage, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	storage := &Storage{
		db:            db,
		dbPath:        dbPath,
		updateBuffer:  make(map[uint64]*sdq.JobMeta),
		updateChan:    make(chan *sdq.JobMeta, 1000),    // Buffer 1000 update requests
		saveChan:      make(chan *saveJobRequest, 1000), // Buffer 1000 save requests
		ctx:           ctx,
		cancel:        cancel,
		maxBatchSize:  1000,             // Default 1000
		maxBatchBytes: 16 * 1024 * 1024, // Default 16MB
	}

	// Apply options
	for _, opt := range opts {
		opt(storage)
	}

	// Initialize database tables
	if err := storage.initTables(); err != nil {
		_ = db.Close()
		cancel()
		return nil, fmt.Errorf("init tables: %w", err)
	}

	// Enable foreign key constraints (SQLite doesn't enable by default)
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		_ = db.Close()
		cancel()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	// Enable WAL mode (Write-Ahead Logging)
	// WAL mode allows concurrent read/write, significantly improving concurrency performance
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		cancel()
		return nil, fmt.Errorf("enable WAL mode: %w", err)
	}

	// Lower synchronization level for better write performance
	// NORMAL: Sync at critical moments, balancing performance and safety
	if _, err := db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		_ = db.Close()
		cancel()
		return nil, fmt.Errorf("set synchronous mode: %w", err)
	}

	// WAL mode allows more concurrent connections
	db.SetMaxOpenConns(10) // Allow multiple read connections
	db.SetMaxIdleConns(2)

	// Start batch update background task
	storage.wg.Add(1)
	go storage.batchUpdateLoop()

	// Start batch save background task
	storage.wg.Add(1)
	go storage.batchSaveLoop()

	return storage, nil
}

// initTables initializes database tables.
func (s *Storage) initTables() error {
	// Create job_meta table
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS job_meta (
			id INTEGER PRIMARY KEY,
			topic TEXT NOT NULL,
			priority INTEGER NOT NULL,
			state INTEGER NOT NULL,
			delay INTEGER NOT NULL,
			ttr INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			ready_at INTEGER NOT NULL,
			reserved_at INTEGER,
			buried_at INTEGER,
			deleted_at INTEGER,
			reserves INTEGER DEFAULT 0,
			timeouts INTEGER DEFAULT 0,
			releases INTEGER DEFAULT 0,
			buries INTEGER DEFAULT 0,
			kicks INTEGER DEFAULT 0,
			touches INTEGER DEFAULT 0
		)
	`)
	if err != nil {
		return fmt.Errorf("create job_meta table: %w", err)
	}

	// Create indexes
	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_job_meta_topic ON job_meta(topic)`)
	if err != nil {
		return fmt.Errorf("create topic index: %w", err)
	}

	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_job_meta_state ON job_meta(state)`)
	if err != nil {
		return fmt.Errorf("create state index: %w", err)
	}

	// Create job_body table
	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS job_body (
			id INTEGER PRIMARY KEY,
			body BLOB NOT NULL,
			FOREIGN KEY (id) REFERENCES job_meta(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return fmt.Errorf("create job_body table: %w", err)
	}

	return nil
}

// SaveJob saves a job (metadata + body).
// Internally merges concurrent requests for better batch write performance.
func (s *Storage) SaveJob(ctx context.Context, meta *sdq.JobMeta, body []byte) error {
	// Check if closed
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return sdq.ErrStorageClosed
	}
	s.closeMu.Unlock()

	// Clone metadata and body to avoid external modification
	metaCopy := meta.Clone()
	var bodyCopy []byte
	if len(body) > 0 {
		bodyCopy = make([]byte, len(body))
		copy(bodyCopy, body)
	}

	// Create request
	req := &saveJobRequest{
		meta: metaCopy,
		body: bodyCopy,
		done: make(chan error, 1),
	}

	// Send to saveChan, processed by batchSaveLoop
	select {
	case s.saveChan <- req:
		return <-req.done // Wait for result
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// batchSaveJobsInternal batch saves jobs (in a single transaction).
// Internal method, called by SaveJob.
func (s *Storage) batchSaveJobsInternal(requests []*saveJobRequest) error {
	if len(requests) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		// Notify all requests of failure
		for _, req := range requests {
			req.done <- fmt.Errorf("begin transaction: %w", err)
		}
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Prepare insert metadata statement
	metaStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO job_meta (
			id, topic, priority, state, delay, ttr,
			created_at, ready_at, reserved_at, buried_at, deleted_at,
			reserves, timeouts, releases, buries, kicks, touches
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		for _, req := range requests {
			req.done <- fmt.Errorf("prepare meta statement: %w", err)
		}
		return fmt.Errorf("prepare meta statement: %w", err)
	}
	defer func() { _ = metaStmt.Close() }()

	// Prepare insert body statement
	bodyStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO job_body (id, body) VALUES (?, ?)
	`)
	if err != nil {
		for _, req := range requests {
			req.done <- fmt.Errorf("prepare body statement: %w", err)
		}
		return fmt.Errorf("prepare body statement: %w", err)
	}
	defer func() { _ = bodyStmt.Close() }()

	// Batch execute inserts
	var successReqs []*saveJobRequest
	for _, req := range requests {
		meta := req.meta
		_, err := metaStmt.ExecContext(ctx,
			meta.ID,
			meta.Topic,
			meta.Priority,
			meta.State,
			int64(meta.Delay),
			int64(meta.TTR),
			meta.CreatedAt.Unix(),
			meta.ReadyAt.Unix(),
			nullableTime(meta.ReservedAt),
			nullableTime(meta.BuriedAt),
			nullableTime(meta.DeletedAt),
			meta.Reserves,
			meta.Timeouts,
			meta.Releases,
			meta.Buries,
			meta.Kicks,
			meta.Touches,
		)
		if err != nil {
			// Check if it's a duplicate key error
			if err.Error() == "UNIQUE constraint failed: job_meta.id" {
				req.done <- sdq.ErrJobExists
			} else {
				req.done <- err
			}
			continue
		}

		// Insert body
		if len(req.body) > 0 {
			_, err = bodyStmt.ExecContext(ctx, meta.ID, req.body)
			if err != nil {
				req.done <- err
				continue
			}
		}

		// Mark as successful (waiting for commit)
		successReqs = append(successReqs, req)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		// Commit failed, notify all successful requests
		commitErr := fmt.Errorf("commit transaction: %w", err)
		for _, req := range successReqs {
			req.done <- commitErr
		}
		return commitErr
	}

	// Commit succeeded, notify all successful requests
	for _, req := range successReqs {
		req.done <- nil
	}

	return nil
}

// UpdateJobMeta updates job metadata (async batch buffering).
// Automatically buffers high-frequency updates and batch writes, greatly reducing I/O pressure.
func (s *Storage) UpdateJobMeta(ctx context.Context, meta *sdq.JobMeta) error {
	// Clone metadata to avoid external modification
	metaCopy := meta.Clone()

	// Send to update channel (non-blocking)
	select {
	case s.updateChan <- metaCopy:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Channel full, flush immediately and retry
		s.flushUpdates()
		select {
		case s.updateChan <- metaCopy:
			return nil
		default:
			return fmt.Errorf("update channel full")
		}
	}
}

// UpdateJobMetaSync synchronously updates job metadata (immediate write, no buffering).
// Used for critical operations that require immediate persistence (e.g., final update before delete).
func (s *Storage) UpdateJobMetaSync(ctx context.Context, meta *sdq.JobMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.db.ExecContext(ctx, `
		UPDATE job_meta SET
			topic = ?, priority = ?, state = ?,
			delay = ?, ttr = ?,
			created_at = ?, ready_at = ?,
			reserved_at = ?, buried_at = ?, deleted_at = ?,
			reserves = ?, timeouts = ?, releases = ?,
			buries = ?, kicks = ?, touches = ?
		WHERE id = ?
	`,
		meta.Topic, meta.Priority, meta.State,
		int64(meta.Delay), int64(meta.TTR),
		meta.CreatedAt.Unix(), meta.ReadyAt.Unix(),
		nullableTime(meta.ReservedAt),
		nullableTime(meta.BuriedAt),
		nullableTime(meta.DeletedAt),
		meta.Reserves, meta.Timeouts, meta.Releases,
		meta.Buries, meta.Kicks, meta.Touches,
		meta.ID,
	)
	if err != nil {
		return fmt.Errorf("update job_meta: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sdq.ErrNotFound
	}

	return nil
}

// GetJobMeta retrieves job metadata.
func (s *Storage) GetJobMeta(ctx context.Context, id uint64) (*sdq.JobMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var meta sdq.JobMeta
	var delay, ttr, createdAt, readyAt int64
	var reservedAt, buriedAt, deletedAt sql.NullInt64

	err := s.db.QueryRowContext(ctx, `
		SELECT
			id, topic, priority, state, delay, ttr,
			created_at, ready_at, reserved_at, buried_at, deleted_at,
			reserves, timeouts, releases, buries, kicks, touches
		FROM job_meta WHERE id = ?
	`, id).Scan(
		&meta.ID,
		&meta.Topic,
		&meta.Priority,
		&meta.State,
		&delay,
		&ttr,
		&createdAt,
		&readyAt,
		&reservedAt,
		&buriedAt,
		&deletedAt,
		&meta.Reserves,
		&meta.Timeouts,
		&meta.Releases,
		&meta.Buries,
		&meta.Kicks,
		&meta.Touches,
	)
	if err == sql.ErrNoRows {
		return nil, sdq.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query job_meta: %w", err)
	}

	// Convert times
	meta.Delay = time.Duration(delay)
	meta.TTR = time.Duration(ttr)
	meta.CreatedAt = time.Unix(createdAt, 0)
	meta.ReadyAt = time.Unix(readyAt, 0)
	if reservedAt.Valid {
		meta.ReservedAt = time.Unix(reservedAt.Int64, 0)
	}
	if buriedAt.Valid {
		meta.BuriedAt = time.Unix(buriedAt.Int64, 0)
	}
	if deletedAt.Valid {
		meta.DeletedAt = time.Unix(deletedAt.Int64, 0)
	}

	return &meta, nil
}

// GetJobBody retrieves job body.
func (s *Storage) GetJobBody(ctx context.Context, id uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var body []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT body FROM job_body WHERE id = ?
	`, id).Scan(&body)
	if err == sql.ErrNoRows {
		return nil, sdq.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query job_body: %w", err)
	}

	return body, nil
}

// DeleteJob deletes a job (metadata + body).
func (s *Storage) DeleteJob(ctx context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Delete metadata
	result, err := tx.ExecContext(ctx, "DELETE FROM job_meta WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete job_meta: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sdq.ErrNotFound
	}

	// Explicitly delete body (for compatibility when foreign key constraint is not effective)
	if _, err := tx.ExecContext(ctx, "DELETE FROM job_body WHERE id = ?", id); err != nil {
		return fmt.Errorf("delete job_body: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// ScanJobMeta scans job metadata.
func (s *Storage) ScanJobMeta(ctx context.Context, filter *sdq.JobMetaFilter) (*sdq.JobMetaList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Build query
	query := "SELECT id, topic, priority, state, delay, ttr, created_at, ready_at, reserved_at, buried_at, deleted_at, reserves, timeouts, releases, buries, kicks, touches FROM job_meta WHERE 1=1"
	args := make([]any, 0)

	if filter != nil {
		if filter.Topic != "" {
			query += " AND topic = ?"
			args = append(args, filter.Topic)
		}
		if filter.State != nil {
			query += " AND state = ?"
			args = append(args, *filter.State)
		}
	}

	// Apply offset and limit
	if filter != nil && filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit+1) // Query one more to check HasMore
		if filter.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, filter.Offset)
		}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query job_meta: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var metas []*sdq.JobMeta
	for rows.Next() {
		var meta sdq.JobMeta
		var delay, ttr, createdAt, readyAt int64
		var reservedAt, buriedAt, deletedAt sql.NullInt64

		err := rows.Scan(
			&meta.ID,
			&meta.Topic,
			&meta.Priority,
			&meta.State,
			&delay,
			&ttr,
			&createdAt,
			&readyAt,
			&reservedAt,
			&buriedAt,
			&deletedAt,
			&meta.Reserves,
			&meta.Timeouts,
			&meta.Releases,
			&meta.Buries,
			&meta.Kicks,
			&meta.Touches,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		// Convert times
		meta.Delay = time.Duration(delay)
		meta.TTR = time.Duration(ttr)
		meta.CreatedAt = time.Unix(createdAt, 0)
		meta.ReadyAt = time.Unix(readyAt, 0)
		if reservedAt.Valid {
			meta.ReservedAt = time.Unix(reservedAt.Int64, 0)
		}
		if buriedAt.Valid {
			meta.BuriedAt = time.Unix(buriedAt.Int64, 0)
		}
		if deletedAt.Valid {
			meta.DeletedAt = time.Unix(deletedAt.Int64, 0)
		}

		metas = append(metas, &meta)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	// Check HasMore
	hasMore := false
	var nextCursor uint64
	if filter != nil && filter.Limit > 0 && len(metas) > filter.Limit {
		hasMore = true
		nextCursor = metas[filter.Limit-1].ID
		metas = metas[:filter.Limit]
	}

	// Count total (optional)
	total := len(metas)

	return &sdq.JobMetaList{
		Metas:      metas,
		Total:      total,
		HasMore:    hasMore,
		NextCursor: nextCursor,
	}, nil
}

// CountJobs counts jobs.
func (s *Storage) CountJobs(ctx context.Context, filter *sdq.JobMetaFilter) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := "SELECT COUNT(*) FROM job_meta WHERE 1=1"
	args := make([]any, 0)

	if filter != nil {
		if filter.Topic != "" {
			query += " AND topic = ?"
			args = append(args, filter.Topic)
		}
		if filter.State != nil {
			query += " AND state = ?"
			args = append(args, *filter.State)
		}
	}

	var count int
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count jobs: %w", err)
	}

	return count, nil
}

// GetMaxJobID returns the maximum job ID (fast startup optimization).
func (s *Storage) GetMaxJobID(ctx context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var maxID sql.NullInt64
	err := s.db.QueryRowContext(ctx, "SELECT MAX(id) FROM job_meta").Scan(&maxID)
	if err != nil {
		return 0, fmt.Errorf("get max job id: %w", err)
	}

	if !maxID.Valid {
		return 0, nil
	}

	return uint64(maxID.Int64), nil
}

// Close closes the storage.
func (s *Storage) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil // Already closed, return directly
	}
	s.closed = true
	s.closeMu.Unlock()

	// Stop batch loops
	s.cancel()

	// Close channels to let goroutines exit
	close(s.updateChan)
	close(s.saveChan)

	// Wait for goroutines to complete
	s.wg.Wait()

	// Final flush of update buffer (batchSaveLoop has processed all save requests)
	s.flushUpdates()

	return s.db.Close()
}

// batchUpdateLoop is the batch update background loop.
// Optimization strategy: After receiving a request, quickly collect all available updates in the channel, then flush immediately.
func (s *Storage) batchUpdateLoop() {
	defer s.wg.Done()

	for {
		// Wait for first update request
		meta, ok := <-s.updateChan
		if !ok {
			return
		}

		// Check if context is cancelled
		select {
		case <-s.ctx.Done():
			// Process this request then exit
			s.updateBufferMu.Lock()
			s.updateBuffer[meta.ID] = meta
			s.updateBufferMu.Unlock()
			s.flushUpdates()
			return
		default:
		}

		// Add to buffer
		s.updateBufferMu.Lock()
		s.updateBuffer[meta.ID] = meta
		s.updateBufferMu.Unlock()

		// Quickly collect all available updates in the channel
		numAvailable := len(s.updateChan)
		if numAvailable > 0 {
			limit := numAvailable
			if len(s.updateBuffer)+numAvailable > s.maxBatchSize {
				limit = s.maxBatchSize - len(s.updateBuffer)
			}

			s.updateBufferMu.Lock()
			for range limit {
				meta2, ok := <-s.updateChan
				if !ok {
					break
				}
				s.updateBuffer[meta2.ID] = meta2
			}
			s.updateBufferMu.Unlock()
		}

		// Non-blocking continue collecting (capture requests arriving after len() call)
	collectLoop:
		for {
			s.updateBufferMu.Lock()
			bufferSize := len(s.updateBuffer)
			s.updateBufferMu.Unlock()

			// Check if limit reached
			if bufferSize >= s.maxBatchSize {
				break collectLoop
			}

			select {
			case meta2, ok := <-s.updateChan:
				if !ok {
					break collectLoop
				}
				s.updateBufferMu.Lock()
				s.updateBuffer[meta2.ID] = meta2
				s.updateBufferMu.Unlock()

			case <-s.ctx.Done():
				break collectLoop

			default:
				// Channel empty, flush immediately
				break collectLoop
			}
		}

		// Immediately flush all collected updates
		s.flushUpdates()
	}
}

// batchSaveLoop is the batch save background loop.
// Optimization strategy: After receiving first request, use non-blocking to quickly collect all requests in the channel, then batch save immediately.
// This achieves true batching under high concurrency while avoiding latency under low concurrency.
func (s *Storage) batchSaveLoop() {
	defer s.wg.Done()

	for {
		// Wait for first request (blocking)
		req, ok := <-s.saveChan
		if !ok {
			return
		}

		// Check if context is cancelled
		select {
		case <-s.ctx.Done():
			// Process this request then exit
			_ = s.batchSaveJobsInternal([]*saveJobRequest{req})
			return
		default:
		}

		// Collect all available requests in the channel (non-blocking)
		buffer := []*saveJobRequest{req}

		// Strategy: Collect all currently available requests in the channel
		// But limit count and total size to avoid:
		// 1. Single transaction too large, causing long table lock
		// 2. Buffer using too much memory
		// 3. Database write timeout

		// Calculate current buffer total size
		totalBytes := len(req.body)

		// First use len() to quickly read known requests
		numAvailable := len(s.saveChan)
		if numAvailable > 0 {
			for range numAvailable {
				// Check if limit reached
				if len(buffer) >= s.maxBatchSize {
					break // Count limit reached
				}
				if totalBytes >= s.maxBatchBytes {
					break // Size limit reached
				}

				req2, ok := <-s.saveChan
				if !ok {
					break
				}

				totalBytes += len(req2.body)
				buffer = append(buffer, req2)
			}
		}

		// Use non-blocking to continue collecting (capture requests arriving after len() call)
	collectLoop:
		for {
			// Check if limit reached
			if len(buffer) >= s.maxBatchSize || totalBytes >= s.maxBatchBytes {
				break collectLoop
			}

			select {
			case req2, ok := <-s.saveChan:
				if !ok {
					// Channel closed
					break collectLoop
				}
				totalBytes += len(req2.body)
				buffer = append(buffer, req2)

			case <-s.ctx.Done():
				// Context cancelled, save immediately
				break collectLoop

			default:
				// Channel empty, save current batch immediately
				break collectLoop
			}
		}

		// Batch save all collected requests
		_ = s.batchSaveJobsInternal(buffer)
	}
}

// flushUpdates flushes buffered updates to database.
func (s *Storage) flushUpdates() {
	// Get pending updates
	s.updateBufferMu.Lock()
	if len(s.updateBuffer) == 0 {
		s.updateBufferMu.Unlock()
		return
	}

	// Copy and clear buffer
	updates := make([]*sdq.JobMeta, 0, len(s.updateBuffer))
	for _, meta := range s.updateBuffer {
		updates = append(updates, meta)
	}
	s.updateBuffer = make(map[uint64]*sdq.JobMeta)
	s.updateBufferMu.Unlock()

	// Batch write to database
	if err := s.batchUpdateJobMeta(updates); err != nil {
		// Log error but don't interrupt (can add logging)
		_ = err
	}
}

// batchUpdateJobMeta batch updates job metadata to database.
func (s *Storage) batchUpdateJobMeta(metas []*sdq.JobMeta) error {
	if len(metas) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Prepare batch update statement
	stmt, err := tx.Prepare(`
		UPDATE job_meta SET
			topic = ?, priority = ?, state = ?,
			delay = ?, ttr = ?,
			created_at = ?, ready_at = ?,
			reserved_at = ?, buried_at = ?, deleted_at = ?,
			reserves = ?, timeouts = ?, releases = ?,
			buries = ?, kicks = ?, touches = ?
		WHERE id = ?
	`)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	// Batch execute
	for _, meta := range metas {
		_, err := stmt.Exec(
			meta.Topic, meta.Priority, meta.State,
			int64(meta.Delay), int64(meta.TTR),
			meta.CreatedAt.Unix(), meta.ReadyAt.Unix(),
			nullableTime(meta.ReservedAt),
			nullableTime(meta.BuriedAt),
			nullableTime(meta.DeletedAt),
			meta.Reserves, meta.Timeouts, meta.Releases,
			meta.Buries, meta.Kicks, meta.Touches,
			meta.ID,
		)
		if err != nil {
			// Single update failure doesn't affect others, continue execution
			_ = err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// Stats returns storage statistics.
func (s *Storage) Stats(ctx context.Context) (*sdq.StorageStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &sdq.StorageStats{Name: s.Name()}

	// Count jobs
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_meta").Scan(&stats.TotalJobs)
	if err != nil {
		return nil, fmt.Errorf("count jobs: %w", err)
	}

	// Count topics
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(DISTINCT topic) FROM job_meta").Scan(&stats.TotalTopics)
	if err != nil {
		return nil, fmt.Errorf("count topics: %w", err)
	}

	// Estimate storage size
	stats.MetaSize = stats.TotalJobs * 200 // Each meta is about 200 bytes

	var totalBodySize sql.NullInt64
	err = s.db.QueryRowContext(ctx, "SELECT SUM(LENGTH(body)) FROM job_body").Scan(&totalBodySize)
	if err != nil {
		return nil, fmt.Errorf("sum body size: %w", err)
	}
	if totalBodySize.Valid {
		stats.BodySize = totalBodySize.Int64
	}

	stats.TotalSize = stats.MetaSize + stats.BodySize

	if stats.TotalJobs > 0 {
		stats.AvgMetaSize = stats.MetaSize / stats.TotalJobs
		stats.AvgBodySize = stats.BodySize / stats.TotalJobs
	}

	// Currently loaded data size (SQLite is not in memory)
	stats.LoadedMetaSize = 0
	stats.LoadedBodySize = 0

	return stats, nil
}

// nullableTime converts time.Time to sql.NullInt64.
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.Unix()
}

// Vacuum optimizes the database (periodic maintenance).
func (s *Storage) Vacuum() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("VACUUM")
	return err
}

// ExportJSON exports data as JSON (for backup/debugging).
func (s *Storage) ExportJSON(ctx context.Context) ([]byte, error) {
	list, err := s.ScanJobMeta(ctx, nil)
	if err != nil {
		return nil, err
	}

	return json.MarshalIndent(list.Metas, "", "  ")
}
