// Package state owns the persistent SQLite store for kafka-tui.
//
// modernc.org/sqlite (pure Go) is used instead of the cgo driver so
// cross-compilation and static linking stay trivial.
package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

const driverName = "sqlite"

// DefaultPath returns `~/.local/share/kafka-tui/state.db`. Errors when $HOME
// cannot be resolved so callers can fall back to in-memory or skip
// persistence.
func DefaultPath() (string, error) {
	if dir, err := os.UserHomeDir(); err == nil && dir != "" {
		return filepath.Join(dir, ".local", "share", "kafka-tui", "state.db"), nil
	} else if err != nil {
		return "", fmt.Errorf("state: resolve home dir: %w", err)
	}
	return "", errors.New("state: $HOME is empty")
}

// Store is the SQLite-backed persistent state. Methods are safe to call from
// multiple goroutines: the underlying *sql.DB serializes access.
type Store struct {
	db   *sql.DB
	path string
}

// Open opens the SQLite database (creating the parent dir as needed) and
// applies pending migrations. path == ":memory:" opens an in-process DB;
// path == "" resolves via [DefaultPath].
func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		p, err := DefaultPath()
		if err != nil {
			return nil, err
		}
		path = p
	}
	if path != ":memory:" {
		// keep the state directory user-private.
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
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
	if path != ":memory:" {
		// sqlite creates the file with the process umask; force 0o600 so
		// a permissive umask can't widen access on shared hosts.
		if err := os.Chmod(path, 0o600); err != nil && !os.IsNotExist(err) {
			_ = db.Close()
			return nil, fmt.Errorf("state: chmod sqlite: %w", err)
		}
	}
	if err := applyMigrations(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, path: path}, nil
}

// buildDSN appends sane defaults: WAL keeps reads from blocking writes;
// busy_timeout avoids spurious "database is locked" errors when the TUI and
// a background watcher race on the file.
func buildDSN(path string) string {
	if path == ":memory:" {
		return path
	}
	return "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)"
}

func (s *Store) Path() string { return s.path }

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("state: close sqlite: %w", err)
	}
	return nil
}

// MessagesView is the persisted seek + partition configuration per
// (cluster, topic). SeekMode stays an int so this package does not need to
// import the screen package; mapping is the caller's responsibility.
type MessagesView struct {
	SeekMode   int
	Partition  int32
	Offset     int64
	Timestamp  time.Time
	HasPart    bool
	Partitions string
}

type messagesViewParams struct {
	Partition int32 `json:"partition,omitempty"`
	Offset    int64 `json:"offset,omitempty"`
	Timestamp int64 `json:"ts_nanos,omitempty"`
	HasPart   bool  `json:"has_part,omitempty"`
}

// LoadMessagesView returns the persisted seek + partition view; the bool is
// false when no row exists.
func (s *Store) LoadMessagesView(ctx context.Context, cluster, topic string) (MessagesView, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT seek_mode, seek_params, partitions
			FROM messages_view_state
			WHERE cluster_name = ? AND topic = ?`,
		cluster, topic,
	)
	var (
		mode       int
		paramsJSON string
		parts      string
	)
	err := row.Scan(&mode, &paramsJSON, &parts)
	if errors.Is(err, sql.ErrNoRows) {
		return MessagesView{}, false, nil
	}
	if err != nil {
		return MessagesView{}, false, fmt.Errorf("state: load messages_view_state: %w", err)
	}
	var p messagesViewParams
	if paramsJSON != "" {
		if err := json.Unmarshal([]byte(paramsJSON), &p); err != nil {
			return MessagesView{}, false, fmt.Errorf("state: decode seek_params: %w", err)
		}
	}
	view := MessagesView{
		SeekMode:   mode,
		Partition:  p.Partition,
		Offset:     p.Offset,
		HasPart:    p.HasPart,
		Partitions: parts,
	}
	if p.Timestamp != 0 {
		view.Timestamp = time.Unix(0, p.Timestamp).UTC()
	}
	return view, true, nil
}

func (s *Store) SaveMessagesView(ctx context.Context, cluster, topic string, view MessagesView) error {
	params := messagesViewParams{
		Partition: view.Partition,
		Offset:    view.Offset,
		HasPart:   view.HasPart,
	}
	if !view.Timestamp.IsZero() {
		params.Timestamp = view.Timestamp.UnixNano()
	}
	buf, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("state: encode seek_params: %w", err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO messages_view_state
			(cluster_name, topic, seek_mode, seek_params, partitions, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(cluster_name, topic) DO UPDATE SET
				seek_mode  = excluded.seek_mode,
				seek_params= excluded.seek_params,
				partitions = excluded.partitions,
				updated_at = excluded.updated_at`,
		cluster, topic, view.SeekMode, string(buf), view.Partitions, time.Now().UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("state: upsert messages_view_state: %w", err)
	}
	return nil
}

// LoadRefreshInterval returns the persisted refresh interval for a screen
// type. The bool is false when no row exists — callers should fall back to
// their config-level default. A stored value of 0 is a real choice ("manual")
// and is returned as (0, true, nil).
func (s *Store) LoadRefreshInterval(ctx context.Context, screenID string) (time.Duration, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT interval_ns FROM refresh_intervals WHERE screen_id = ?`,
		screenID,
	)
	var ns int64
	err := row.Scan(&ns)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("state: load refresh_intervals: %w", err)
	}
	// negative values can't have been written by SaveRefreshInterval (it clamps),
	// but defend against manual edits — treat them as "manual".
	if ns < 0 {
		ns = 0
	}
	return time.Duration(ns), true, nil
}

// SaveRefreshInterval upserts the user-chosen interval for a screen type.
// Negative durations are clamped to 0 ("manual") so the on-disk shape mirrors
// the in-memory contract from [components.Refresher].
func (s *Store) SaveRefreshInterval(ctx context.Context, screenID string, d time.Duration) error {
	if d < 0 {
		d = 0
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO refresh_intervals (screen_id, interval_ns, updated_at)
			VALUES (?, ?, ?)
			ON CONFLICT(screen_id) DO UPDATE SET
				interval_ns = excluded.interval_ns,
				updated_at  = excluded.updated_at`,
		screenID, int64(d), time.Now().UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("state: upsert refresh_intervals: %w", err)
	}
	return nil
}
