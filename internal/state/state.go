// Package state owns the persistent SQLite store for kafka-tui.
//
// The only consumer today is the produce form's history (Task 15 / §7.5):
// past produces are recorded into `produce_history` so ctrl+p / ctrl+n can
// walk them, and so a freshly opened form can prefill from the most recent
// produce to the same topic.
//
// We deliberately use modernc.org/sqlite (pure Go) rather than the cgo
// driver so cross-compilation and static linking stay trivial.
package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/aleksey925/kafka-tui/internal/kafka"
)

// driverName is the database/sql driver registered by modernc.org/sqlite.
const driverName = "sqlite"

// DefaultPath returns the spec-defined location for the state DB:
// `~/.local/share/kafka-tui/state.db`. Returns an error when the home
// directory cannot be resolved or is empty so callers can decide whether
// to fall back to an in-memory store or skip persistence entirely.
func DefaultPath() (string, error) {
	if dir, err := os.UserHomeDir(); err == nil && dir != "" {
		return filepath.Join(dir, ".local", "share", "kafka-tui", "state.db"), nil
	} else if err != nil {
		return "", fmt.Errorf("state: resolve home dir: %w", err)
	}
	return "", errors.New("state: $HOME is empty")
}

// ProduceEntry is the row payload for `produce_history`. It mirrors the
// produce screen's `Entry` so wiring code can copy fields directly without
// importing the TUI package (which would create a cycle).
type ProduceEntry struct {
	Cluster     string
	Topic       string
	Key         []byte
	Value       []byte
	Headers     []kafka.Header
	Partition   int32
	Compression kafka.Compression
	Timestamp   time.Time
}

// Store is the SQLite-backed persistent state. Methods are safe to call from
// multiple goroutines: the underlying *sql.DB serializes access.
type Store struct {
	db   *sql.DB
	path string
}

// Open creates (if needed) the parent directory and opens the SQLite
// database, applying any pending migrations before returning.
//
// Special path values:
//
//   - ":memory:" — open an in-process DB, useful for tests.
//   - ""         — resolve via [DefaultPath].
func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		p, err := DefaultPath()
		if err != nil {
			return nil, err
		}
		path = p
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("state: create dir: %w", err)
		}
	}

	dsn := buildDSN(path)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("state: open sqlite: %w", err)
	}
	// in-memory connections are per-connection — pin the pool to one so
	// every query sees the same database.
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("state: ping sqlite: %w", err)
	}
	if err := applyMigrations(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, path: path}, nil
}

// buildDSN appends sane defaults to the file path. WAL keeps reads from
// blocking writes; busy_timeout avoids spurious "database is locked" errors
// when the TUI and a background watcher race on the file.
func buildDSN(path string) string {
	if path == ":memory:" {
		return path
	}
	return "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)"
}

// Path returns the on-disk location of the database file (":memory:" for
// the in-memory variant).
func (s *Store) Path() string { return s.path }

// Close releases the underlying *sql.DB.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("state: close sqlite: %w", err)
	}
	return nil
}

// AddProduce inserts a produce-history row and trims the table back down to
// `historySize` entries (newest kept). When `historySize <= 0` the trim step
// is skipped — callers can pass the value of `produce.history_size` from
// config directly.
func (s *Store) AddProduce(ctx context.Context, entry ProduceEntry, historySize int) error {
	headersJSON, err := encodeHeaders(entry.Headers)
	if err != nil {
		return err
	}
	ts := entry.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO produce_history
			(cluster, topic, key, value, headers, partition, compression, ts)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.Cluster,
		entry.Topic,
		nullableBlob(entry.Key),
		nullableBlob(entry.Value),
		headersJSON,
		entry.Partition,
		string(entry.Compression),
		ts.UnixNano(),
	); err != nil {
		return fmt.Errorf("state: insert produce_history: %w", err)
	}

	if historySize > 0 {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM produce_history
				WHERE id NOT IN (
					SELECT id FROM produce_history
					ORDER BY ts DESC, id DESC
					LIMIT ?
				)`,
			historySize,
		); err != nil {
			return fmt.Errorf("state: trim produce_history: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit produce_history: %w", err)
	}
	return nil
}

// LastProduceForTopic returns the newest entry for the given topic. The bool
// is false when no row exists for that topic.
func (s *Store) LastProduceForTopic(ctx context.Context, topic string) (ProduceEntry, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT cluster, topic, key, value, headers, partition, compression, ts
			FROM produce_history
			WHERE topic = ?
			ORDER BY ts DESC, id DESC
			LIMIT 1`,
		topic,
	)
	entry, err := scanEntry(row)
	if errors.Is(err, sql.ErrNoRows) {
		return ProduceEntry{}, false, nil
	}
	if err != nil {
		return ProduceEntry{}, false, err
	}
	return entry, true, nil
}

// RecentProduce returns up to `n` entries, newest-first. When `n <= 0` an
// empty slice is returned.
func (s *Store) RecentProduce(ctx context.Context, n int) ([]ProduceEntry, error) {
	if n <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT cluster, topic, key, value, headers, partition, compression, ts
			FROM produce_history
			ORDER BY ts DESC, id DESC
			LIMIT ?`,
		n,
	)
	if err != nil {
		return nil, fmt.Errorf("state: query produce_history: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]ProduceEntry, 0, n)
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate produce_history: %w", err)
	}
	return out, nil
}

// scanner is the minimal interface implemented by both *sql.Row and
// *sql.Rows so scanEntry can serve both lookup paths.
type scanner interface {
	Scan(dest ...any) error
}

func scanEntry(s scanner) (ProduceEntry, error) {
	var (
		entry       ProduceEntry
		key, value  []byte
		headers     string
		compression string
		tsNanos     int64
	)
	if err := s.Scan(
		&entry.Cluster,
		&entry.Topic,
		&key,
		&value,
		&headers,
		&entry.Partition,
		&compression,
		&tsNanos,
	); err != nil {
		return ProduceEntry{}, fmt.Errorf("state: scan produce_history row: %w", err)
	}
	entry.Key = key
	entry.Value = value
	entry.Compression = kafka.Compression(compression)
	entry.Timestamp = time.Unix(0, tsNanos).UTC()

	hdrs, err := decodeHeaders(headers)
	if err != nil {
		return ProduceEntry{}, err
	}
	entry.Headers = hdrs
	return entry, nil
}

// nullableBlob returns a nil interface for empty payloads so SQLite stores
// SQL NULL rather than an empty BLOB. This keeps the read path symmetrical
// with how the produce form treats absent keys/values.
func nullableBlob(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

// encodedHeader is the on-disk shape used in the `headers` JSON column.
// We use a separate type with base64-clean encoding so binary header values
// survive the round-trip — `[]byte` marshals to base64 by default in
// encoding/json, which is exactly what we want.
type encodedHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func encodeHeaders(headers []kafka.Header) (string, error) {
	if len(headers) == 0 {
		return "[]", nil
	}
	out := make([]encodedHeader, len(headers))
	for i, h := range headers {
		out[i] = encodedHeader{Key: h.Key, Value: h.Value}
	}
	buf, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("state: encode headers: %w", err)
	}
	return string(buf), nil
}

func decodeHeaders(raw string) ([]kafka.Header, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "[]" || raw == "null" {
		return nil, nil
	}
	var rows []encodedHeader
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		return nil, fmt.Errorf("state: decode headers: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]kafka.Header, len(rows))
	for i, r := range rows {
		out[i] = kafka.Header{Key: r.Key, Value: r.Value}
	}
	return out, nil
}
