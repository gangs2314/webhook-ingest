// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

type Store struct {
	pool *pgxpool.Pool
}

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// underlying pool for tests and queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// EventExists - for confirm id is stored or not
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	return eventExists(ctx, s.pool, eventID)
}

func eventExists(ctx context.Context, q querier, eventID string) (bool, error) {
	var one int
	err := q.QueryRow(ctx,
		`SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) InsertEvent(ctx context.Context, e Event) (bool, error) {
	return insertEvent(ctx, s.pool, e)
}

func insertEvent(ctx context.Context, q querier, e Event) (bool, error) {
	tag, err := q.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) UpsertCall(ctx context.Context, e Event) error {
	return upsertCall(ctx, s.pool, e)
}

func upsertCall(ctx context.Context, q querier, e Event) error {
	_, err := q.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	return err
}

func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE calls SET recording_processed = TRUE, updated_at = now()
		 WHERE call_id = $1`, callID)
	return err
}

func (s *Store) IncrementAccountStats(ctx context.Context, accountID string, durationSec int) error {
	return incrementAccountStats(ctx, s.pool, accountID, durationSec)
}

func incrementAccountStats(ctx context.Context, q querier, accountID string, durationSec int) error {
	_, err := q.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		accountID, durationSec)
	return err
}

func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}

func (s *Store) IngestEvent(ctx context.Context, e Event) (inserted bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	inserted, err = insertEvent(ctx, tx, e)
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, nil
	}

	if err = upsertCall(ctx, tx, e); err != nil {
		return false, err
	}
	if err = incrementAccountStats(ctx, tx, e.AccountID, e.DurationSec); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
