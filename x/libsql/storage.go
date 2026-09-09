package libsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	turso "turso.tech/database/tursogo"

	"go-slim.dev/sdq"
)

const (
	defaultBusyTimeout             = 5 * time.Second
	defaultMaxOpenConns            = 10
	defaultMaxIdleConns            = 2
	defaultMaxBatchSize            = 128
	defaultMaxBatchBytes           = 4 * 1024 * 1024
	defaultWriteBuffer             = 1024
	defaultMVCCCheckpointThreshold = 10_000
	defaultMVCCGCThreshold         = 10_000
	estimatedMetaSize              = 200
)

var (
	_ sdq.Storage = (*Storage)(nil)

	// ErrNilJobMeta indicates that a nil metadata value was passed to a write method.
	ErrNilJobMeta = errors.New("libsql: nil job metadata")
)

const (
	createJobMetaTableSQL = `
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
			last_touch_at INTEGER,
			buried_at INTEGER,
			deleted_at INTEGER,
			reserves INTEGER NOT NULL DEFAULT 0,
			timeouts INTEGER NOT NULL DEFAULT 0,
			releases INTEGER NOT NULL DEFAULT 0,
			buries INTEGER NOT NULL DEFAULT 0,
			kicks INTEGER NOT NULL DEFAULT 0,
			touches INTEGER NOT NULL DEFAULT 0,
			total_touch_time INTEGER NOT NULL DEFAULT 0
		)`
	createJobBodyTableSQL = `
		CREATE TABLE IF NOT EXISTS job_body (
			id INTEGER PRIMARY KEY,
			body BLOB NOT NULL,
			FOREIGN KEY (id) REFERENCES job_meta(id) ON DELETE CASCADE
		)`
	insertJobMetaSQL = `
		INSERT INTO job_meta (
			id, topic, priority, state, delay, ttr,
			created_at, ready_at, reserved_at, last_touch_at, buried_at, deleted_at,
			reserves, timeouts, releases, buries, kicks, touches, total_touch_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO NOTHING`
	updateJobMetaSQL = `
		UPDATE job_meta SET
			topic = ?, priority = ?, state = ?, delay = ?, ttr = ?,
			created_at = ?, ready_at = ?, reserved_at = ?, last_touch_at = ?,
			buried_at = ?, deleted_at = ?, reserves = ?, timeouts = ?,
			releases = ?, buries = ?, kicks = ?, touches = ?, total_touch_time = ?
		WHERE id = ?`
	selectJobMetaSQL = `
		SELECT id, topic, priority, state, delay, ttr,
			created_at, ready_at, reserved_at, last_touch_at, buried_at, deleted_at,
			reserves, timeouts, releases, buries, kicks, touches, total_touch_time
		FROM job_meta`
)

// Option configures a Storage.
type Option func(*options)

type options struct {
	busyTimeout             time.Duration
	maxOpenConns            int
	maxIdleConns            int
	maxBatchSize            int
	maxBatchBytes           int
	mvccCheckpointThreshold int
	mvccGCThreshold         int
}

// WithBusyTimeout sets how long libSQL waits for a busy database. A zero timeout
// disables the busy handler. Negative values are ignored.
func WithBusyTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		if timeout >= 0 {
			opts.busyTimeout = timeout
		}
	}
}

// WithMaxOpenConns sets the database/sql maximum number of open connections.
// Non-positive values are ignored.
func WithMaxOpenConns(count int) Option {
	return func(opts *options) {
		if count > 0 {
			opts.maxOpenConns = count
		}
	}
}

// WithMaxIdleConns sets the database/sql maximum number of idle connections.
// Negative values are ignored; zero disables idle connections.
func WithMaxIdleConns(count int) Option {
	return func(opts *options) {
		if count >= 0 {
			opts.maxIdleConns = count
		}
	}
}

// WithMaxBatchSize sets the maximum number of concurrent SaveJob requests
// committed in one transaction. Non-positive values are ignored.
func WithMaxBatchSize(count int) Option {
	return func(opts *options) {
		if count > 0 {
			opts.maxBatchSize = count
		}
	}
}

// WithMaxBatchBytes sets the approximate body-byte limit for one write batch.
// A single request larger than the limit is still written. Non-positive values
// are ignored.
func WithMaxBatchBytes(size int) Option {
	return func(opts *options) {
		if size > 0 {
			opts.maxBatchBytes = size
		}
	}
}

// WithMVCCCheckpointThreshold sets the committed-work threshold that triggers
// an MVCC checkpoint. Use -1 to disable automatic checkpointing. Zero and
// values below -1 are ignored.
func WithMVCCCheckpointThreshold(threshold int) Option {
	return func(opts *options) {
		if threshold > 0 || threshold == -1 {
			opts.mvccCheckpointThreshold = threshold
		}
	}
}

// WithMVCCGCThreshold sets how aggressively obsolete MVCC row versions are
// collected. Use -1 to disable automatic collection. Zero and values below -1
// are ignored.
func WithMVCCGCThreshold(threshold int) Option {
	return func(opts *options) {
		if threshold > 0 || threshold == -1 {
			opts.mvccGCThreshold = threshold
		}
	}
}

type writeRequest struct {
	meta *sdq.JobMeta
	body []byte
	done chan error
}

// Storage is a Turso/libSQL-backed sdq storage.
//
// SaveJob and UpdateJobMeta are synchronous. Do not call Close concurrently with
// other methods; operations attempted after Close return sdq.ErrStorageClosed.
type Storage struct {
	db *sql.DB

	mu                      sync.RWMutex
	closed                  bool
	writeMu                 sync.Mutex
	writeChan               chan *writeRequest
	writeWG                 sync.WaitGroup
	maxBatchSize            int
	maxBatchBytes           int
	mvccCheckpointThreshold int
	mvccGCThreshold         int

	lastSaveTime atomic.Int64
	lastLoadTime atomic.Int64
}

// New opens or creates a local Turso/libSQL database at dbPath.
func New(dbPath string, opts ...Option) (*Storage, error) {
	config := options{
		busyTimeout:             defaultBusyTimeout,
		maxOpenConns:            defaultMaxOpenConns,
		maxIdleConns:            defaultMaxIdleConns,
		maxBatchSize:            defaultMaxBatchSize,
		maxBatchBytes:           defaultMaxBatchBytes,
		mvccCheckpointThreshold: defaultMVCCCheckpointThreshold,
		mvccGCThreshold:         defaultMVCCGCThreshold,
	}
	for _, apply := range opts {
		if apply != nil {
			apply(&config)
		}
	}
	// Each connection to :memory: owns a separate database. Keep one connection
	// alive so schema and data remain visible for the lifetime of the storage.
	if dbPath == ":memory:" {
		config.maxOpenConns = 1
		config.maxIdleConns = 1
	}

	base, err := turso.NewConnector(
		dbPath,
		turso.WithBusyTimeout(durationMillis(config.busyTimeout)),
	)
	if err != nil {
		return nil, fmt.Errorf("libsql: create connector: %w", err)
	}

	db := sql.OpenDB(&connector{base: base})
	db.SetMaxOpenConns(config.maxOpenConns)
	db.SetMaxIdleConns(min(config.maxIdleConns, config.maxOpenConns))

	storage := &Storage{
		db:                      db,
		writeChan:               make(chan *writeRequest, defaultWriteBuffer),
		maxBatchSize:            config.maxBatchSize,
		maxBatchBytes:           config.maxBatchBytes,
		mvccCheckpointThreshold: config.mvccCheckpointThreshold,
		mvccGCThreshold:         config.mvccGCThreshold,
	}
	if err := storage.initialize(context.Background()); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	storage.writeWG.Go(storage.writeLoop)

	return storage, nil
}

// Name returns the storage name.
func (s *Storage) Name() string {
	return "libsql"
}

// SaveJob atomically persists metadata and body. It returns sdq.ErrJobExists
// when a job with the same ID already exists. Once a request has entered the
// write queue, SaveJob waits for its transaction to finish even if ctx is later
// canceled.
func (s *Storage) SaveJob(ctx context.Context, meta *sdq.JobMeta, body []byte) error {
	if meta == nil {
		return ErrNilJobMeta
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return sdq.ErrStorageClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if body == nil {
		body = []byte{}
	}
	request := &writeRequest{
		meta: meta.Clone(),
		body: cloneBytes(body),
		done: make(chan error, 1),
	}
	select {
	case s.writeChan <- request:
		return <-request.done
	case <-ctx.Done():
		return ctx.Err()
	}
}

// UpdateJobMeta synchronously updates metadata without changing the body.
func (s *Storage) UpdateJobMeta(ctx context.Context, meta *sdq.JobMeta) error {
	if meta == nil {
		return ErrNilJobMeta
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return sdq.ErrStorageClosed
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	result, err := s.db.ExecContext(ctx, updateJobMetaSQL, updateMetaValues(meta)...)
	if err != nil {
		return fmt.Errorf("libsql: update job metadata: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("libsql: inspect updated job metadata: %w", err)
	}
	if rowsAffected == 0 {
		return sdq.ErrNotFound
	}

	s.lastSaveTime.Store(time.Now().Unix())
	return nil
}

// GetJobMeta returns metadata for id.
func (s *Storage) GetJobMeta(ctx context.Context, id uint64) (*sdq.JobMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, sdq.ErrStorageClosed
	}

	meta, err := scanJobMeta(s.db.QueryRowContext(ctx, selectJobMetaSQL+" WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, sdq.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("libsql: get job metadata: %w", err)
	}

	s.lastLoadTime.Store(time.Now().Unix())
	return meta, nil
}

// ScanJobMeta returns metadata matching filter without loading job bodies.
func (s *Storage) ScanJobMeta(ctx context.Context, filter *sdq.JobMetaFilter) (*sdq.JobMetaList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, sdq.ErrStorageClosed
	}

	query := selectJobMetaSQL + " WHERE 1 = 1"
	args := make([]any, 0, 5)
	if filter != nil {
		query, args = appendFilter(query, args, filter, true)
	}
	query += " ORDER BY id ASC"

	limit := 0
	if filter != nil {
		limit = filter.Limit
		if filter.Limit > 0 {
			query += " LIMIT ?"
			args = append(args, int64(filter.Limit)+1)
			if filter.Offset > 0 {
				query += " OFFSET ?"
				args = append(args, filter.Offset)
			}
		} else if filter.Offset > 0 {
			query += " LIMIT -1 OFFSET ?"
			args = append(args, filter.Offset)
		}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("libsql: scan job metadata: %w", err)
	}
	defer func() { _ = rows.Close() }()

	capacity := 0
	if limit > 0 {
		capacity = min(limit+1, 1024)
	}
	metas := make([]*sdq.JobMeta, 0, capacity)
	for rows.Next() {
		meta, err := scanJobMeta(rows)
		if err != nil {
			return nil, fmt.Errorf("libsql: scan job metadata row: %w", err)
		}
		metas = append(metas, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("libsql: iterate job metadata: %w", err)
	}

	result := &sdq.JobMetaList{Metas: metas}
	if limit > 0 && len(result.Metas) > limit {
		result.Metas = result.Metas[:limit]
		result.HasMore = true
		result.NextCursor = result.Metas[len(result.Metas)-1].ID
	}
	if filter == nil || (filter.Limit <= 0 && filter.Offset <= 0) {
		result.Total = len(result.Metas)
	}

	s.lastLoadTime.Store(time.Now().Unix())
	return result, nil
}

// GetJobBody returns a copy of the body for id.
func (s *Storage) GetJobBody(ctx context.Context, id uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, sdq.ErrStorageClosed
	}

	var body []byte
	if err := s.db.QueryRowContext(ctx, "SELECT body FROM job_body WHERE id = ?", id).Scan(&body); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sdq.ErrNotFound
		}
		return nil, fmt.Errorf("libsql: get job body: %w", err)
	}

	s.lastLoadTime.Store(time.Now().Unix())
	return body, nil
}

// DeleteJob atomically removes metadata and its cascaded body for id.
func (s *Storage) DeleteJob(ctx context.Context, id uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return sdq.ErrStorageClosed
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	result, err := s.db.ExecContext(ctx, "DELETE FROM job_meta WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("libsql: delete job metadata: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("libsql: inspect deleted job metadata: %w", err)
	}
	if rowsAffected == 0 {
		return sdq.ErrNotFound
	}

	s.lastSaveTime.Store(time.Now().Unix())
	return nil
}

// CountJobs counts jobs matching the topic and state fields in filter.
func (s *Storage) CountJobs(ctx context.Context, filter *sdq.JobMetaFilter) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, sdq.ErrStorageClosed
	}

	query := "SELECT COUNT(*) FROM job_meta WHERE 1 = 1"
	args := make([]any, 0, 2)
	if filter != nil {
		query, args = appendFilter(query, args, filter, false)
	}

	var count int
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("libsql: count jobs: %w", err)
	}
	return count, nil
}

// GetMaxJobID returns the largest persisted job ID, or zero for an empty database.
func (s *Storage) GetMaxJobID(ctx context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, sdq.ErrStorageClosed
	}

	var maxID sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT MAX(id) FROM job_meta").Scan(&maxID); err != nil {
		return 0, fmt.Errorf("libsql: get maximum job ID: %w", err)
	}
	if !maxID.Valid {
		return 0, nil
	}
	return uint64(maxID.Int64), nil
}

// Stats returns logical storage statistics. MetaSize is an estimate consistent
// with the other sdq storage implementations.
func (s *Storage) Stats(ctx context.Context) (*sdq.StorageStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, sdq.ErrStorageClosed
	}

	stats := &sdq.StorageStats{Name: s.Name()}
	if err := s.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*), COUNT(DISTINCT topic) FROM job_meta",
	).Scan(&stats.TotalJobs, &stats.TotalTopics); err != nil {
		return nil, fmt.Errorf("libsql: collect job statistics: %w", err)
	}

	var bodySize sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT SUM(LENGTH(body)) FROM job_body").Scan(&bodySize); err != nil {
		return nil, fmt.Errorf("libsql: collect body statistics: %w", err)
	}
	if bodySize.Valid {
		stats.BodySize = bodySize.Int64
	}
	stats.MetaSize = stats.TotalJobs * estimatedMetaSize
	stats.TotalSize = stats.MetaSize + stats.BodySize
	stats.LastSaveTime = s.lastSaveTime.Load()
	stats.LastLoadTime = s.lastLoadTime.Load()
	if stats.TotalJobs > 0 {
		stats.AvgMetaSize = stats.MetaSize / stats.TotalJobs
		stats.AvgBodySize = stats.BodySize / stats.TotalJobs
	}
	return stats, nil
}

// Close releases the connection pool. It is idempotent.
func (s *Storage) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.writeChan)
	s.writeWG.Wait()
	return s.db.Close()
}

func (s *Storage) writeLoop() {
	for first := range s.writeChan {
		capacity := min(s.maxBatchSize, len(s.writeChan)+1)
		batch := make([]*writeRequest, 0, capacity)
		batch = append(batch, first)
		batchBytes := len(first.body)

	collect:
		for len(batch) < s.maxBatchSize && batchBytes < s.maxBatchBytes {
			select {
			case request, ok := <-s.writeChan:
				if !ok {
					break collect
				}
				batch = append(batch, request)
				batchBytes += len(request.body)
			default:
				break collect
			}
		}

		s.writeBatch(batch)
	}
}

func (s *Storage) writeBatch(batch []*writeRequest) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		notifyWriteBatch(batch, fmt.Errorf("libsql: begin write batch transaction: %w", err))
		return
	}
	defer func() { _ = tx.Rollback() }()

	requestErrors := make([]error, len(batch))
	for index, request := range batch {
		requestErr, err := saveJobTx(tx, request.meta, request.body)
		if err != nil {
			notifyWriteBatch(batch, err)
			return
		}
		requestErrors[index] = requestErr
	}

	if err := tx.Commit(); err != nil {
		notifyWriteBatch(batch, fmt.Errorf("libsql: commit write batch transaction: %w", err))
		return
	}

	wrote := false
	for index, request := range batch {
		if requestErrors[index] == nil {
			wrote = true
		}
		request.done <- requestErrors[index]
	}
	if wrote {
		s.lastSaveTime.Store(time.Now().Unix())
	}
}

func saveJobTx(tx *sql.Tx, meta *sdq.JobMeta, body []byte) (error, error) {
	result, err := tx.ExecContext(context.Background(), insertJobMetaSQL, metaValues(meta)...)
	if err != nil {
		return nil, fmt.Errorf("libsql: insert job metadata: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("libsql: inspect inserted job metadata: %w", err)
	}
	if rowsAffected == 0 {
		return sdq.ErrJobExists, nil
	}
	if _, err := tx.ExecContext(
		context.Background(),
		"INSERT INTO job_body (id, body) VALUES (?, ?)",
		meta.ID,
		body,
	); err != nil {
		return nil, fmt.Errorf("libsql: insert job body: %w", err)
	}
	return nil, nil
}

func notifyWriteBatch(batch []*writeRequest, err error) {
	for _, request := range batch {
		request.done <- err
	}
}

func (s *Storage) initialize(ctx context.Context) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("libsql: open database connection: %w", err)
	}
	_, configErr := conn.ExecContext(ctx, "PRAGMA journal_mode = 'mvcc'")
	if configErr == nil {
		_, configErr = conn.ExecContext(
			ctx,
			fmt.Sprintf(
				"PRAGMA mvcc_checkpoint_threshold = %d",
				s.mvccCheckpointThreshold,
			),
		)
	}
	if configErr == nil {
		_, configErr = conn.ExecContext(
			ctx,
			fmt.Sprintf("PRAGMA mvcc_gc_threshold = %d", s.mvccGCThreshold),
		)
	}
	closeErr := conn.Close()
	if err := errors.Join(configErr, closeErr); err != nil {
		return fmt.Errorf("libsql: enable MVCC: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, createJobMetaTableSQL); err != nil {
		return fmt.Errorf("libsql: create job metadata table: %w", err)
	}
	if err := s.ensureMetaColumns(ctx); err != nil {
		return err
	}

	statements := []struct {
		name string
		sql  string
	}{
		{name: "topic index", sql: "CREATE INDEX IF NOT EXISTS idx_job_meta_topic_id ON job_meta(topic, id)"},
		{name: "state index", sql: "CREATE INDEX IF NOT EXISTS idx_job_meta_state_id ON job_meta(state, id)"},
		{name: "job body table", sql: createJobBodyTableSQL},
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement.sql); err != nil {
			return fmt.Errorf("libsql: create %s: %w", statement.name, err)
		}
	}
	return nil
}

func (s *Storage) ensureMetaColumns(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info(job_meta)")
	if err != nil {
		return fmt.Errorf("libsql: inspect job metadata schema: %w", err)
	}

	columns := make(map[string]struct{})
	for rows.Next() {
		var (
			columnID     int
			name         string
			columnType   string
			notNull      int
			defaultValue any
			primaryKey   int
		)
		if err := rows.Scan(
			&columnID,
			&name,
			&columnType,
			&notNull,
			&defaultValue,
			&primaryKey,
		); err != nil {
			_ = rows.Close()
			return fmt.Errorf("libsql: scan job metadata schema: %w", err)
		}
		columns[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("libsql: iterate job metadata schema: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("libsql: close job metadata schema rows: %w", err)
	}

	migrations := []struct {
		column string
		sql    string
	}{
		{column: "last_touch_at", sql: "ALTER TABLE job_meta ADD COLUMN last_touch_at INTEGER"},
		{
			column: "total_touch_time",
			sql:    "ALTER TABLE job_meta ADD COLUMN total_touch_time INTEGER NOT NULL DEFAULT 0",
		},
	}
	for _, migration := range migrations {
		if _, exists := columns[migration.column]; exists {
			continue
		}
		if _, err := s.db.ExecContext(ctx, migration.sql); err != nil {
			return fmt.Errorf("libsql: add job metadata column %q: %w", migration.column, err)
		}
	}
	return nil
}

func appendFilter(
	query string,
	args []any,
	filter *sdq.JobMetaFilter,
	includeCursor bool,
) (string, []any) {
	if filter.Topic != "" {
		query += " AND topic = ?"
		args = append(args, filter.Topic)
	}
	if filter.State != nil {
		query += " AND state = ?"
		args = append(args, *filter.State)
	}
	if includeCursor && filter.Cursor > 0 {
		query += " AND id > ?"
		args = append(args, filter.Cursor)
	}
	return query, args
}

func metaValues(meta *sdq.JobMeta) []any {
	return []any{
		meta.ID,
		meta.Topic,
		meta.Priority,
		meta.State,
		int64(meta.Delay),
		int64(meta.TTR),
		meta.CreatedAt.Unix(),
		meta.ReadyAt.Unix(),
		nullableTime(meta.ReservedAt),
		nullableTime(meta.LastTouchAt),
		nullableTime(meta.BuriedAt),
		nullableTime(meta.DeletedAt),
		meta.Reserves,
		meta.Timeouts,
		meta.Releases,
		meta.Buries,
		meta.Kicks,
		meta.Touches,
		int64(meta.TotalTouchTime),
	}
}

func updateMetaValues(meta *sdq.JobMeta) []any {
	return []any{
		meta.Topic,
		meta.Priority,
		meta.State,
		int64(meta.Delay),
		int64(meta.TTR),
		meta.CreatedAt.Unix(),
		meta.ReadyAt.Unix(),
		nullableTime(meta.ReservedAt),
		nullableTime(meta.LastTouchAt),
		nullableTime(meta.BuriedAt),
		nullableTime(meta.DeletedAt),
		meta.Reserves,
		meta.Timeouts,
		meta.Releases,
		meta.Buries,
		meta.Kicks,
		meta.Touches,
		int64(meta.TotalTouchTime),
		meta.ID,
	}
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJobMeta(row rowScanner) (*sdq.JobMeta, error) {
	meta := &sdq.JobMeta{}
	var delay, ttr, createdAt, readyAt, totalTouchTime int64
	var reservedAt, lastTouchAt, buriedAt, deletedAt sql.NullInt64

	if err := row.Scan(
		&meta.ID,
		&meta.Topic,
		&meta.Priority,
		&meta.State,
		&delay,
		&ttr,
		&createdAt,
		&readyAt,
		&reservedAt,
		&lastTouchAt,
		&buriedAt,
		&deletedAt,
		&meta.Reserves,
		&meta.Timeouts,
		&meta.Releases,
		&meta.Buries,
		&meta.Kicks,
		&meta.Touches,
		&totalTouchTime,
	); err != nil {
		return nil, err
	}

	meta.Delay = time.Duration(delay)
	meta.TTR = time.Duration(ttr)
	meta.CreatedAt = time.Unix(createdAt, 0)
	meta.ReadyAt = time.Unix(readyAt, 0)
	meta.ReservedAt = fromNullableTime(reservedAt)
	meta.LastTouchAt = fromNullableTime(lastTouchAt)
	meta.BuriedAt = fromNullableTime(buriedAt)
	meta.DeletedAt = fromNullableTime(deletedAt)
	meta.TotalTouchTime = time.Duration(totalTouchTime)
	return meta, nil
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.Unix()
}

func fromNullableTime(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return time.Unix(value.Int64, 0)
}

func durationMillis(duration time.Duration) int {
	return int(duration / time.Millisecond)
}

func cloneBytes(value []byte) []byte {
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
