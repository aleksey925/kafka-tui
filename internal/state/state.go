// Package state owns the persistent kafka-tui store.
package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// memoryPath is the sentinel for in-memory mode: Open with this value skips
// file I/O and keeps the snapshot inside the *Store only.
const memoryPath = ":memory:"

// snapshotVersion is the on-disk schema marker. Only bump on incompatible
// changes — additive fields don't need it. A file carrying any other value
// is treated as unreadable (see [Store.quarantine]).
const snapshotVersion = 1

type snapshot struct {
	Version          int                                `json:"version"`
	MessagesView     map[string]map[string]MessagesView `json:"messages_view,omitempty"`
	RefreshIntervals map[string]time.Duration           `json:"refresh_intervals,omitempty"`
}

// MessagesView is the persisted seek + partition configuration per
// (cluster, topic). SeekMode stays an int so this package does not need to
// import the screen package; mapping is the caller's responsibility.
type MessagesView struct {
	SeekMode   int       `json:"seek_mode,omitzero"`
	Partition  int32     `json:"partition,omitzero"`
	Offset     int64     `json:"offset,omitzero"`
	Timestamp  time.Time `json:"timestamp,omitzero"`
	HasPart    bool      `json:"has_part,omitzero"`
	Partitions string    `json:"partitions,omitzero"`
}

// DefaultPath returns `~/.local/share/kafka-tui/state.json`. Errors when
// $HOME cannot be resolved so callers can fall back to in-memory or skip
// persistence.
func DefaultPath() (string, error) {
	if dir, err := os.UserHomeDir(); err == nil && dir != "" {
		return filepath.Join(dir, ".local", "share", "kafka-tui", "state.json"), nil
	} else if err != nil {
		return "", fmt.Errorf("state: resolve home dir: %w", err)
	}
	return "", errors.New("state: $HOME is empty")
}

// Store is a JSON-backed key-value store for screen view state. All methods
// are safe to call from multiple goroutines: a single mutex serializes
// reads, writes, and the on-disk save.
type Store struct {
	mu   sync.Mutex
	snap snapshot
	// path is "" in memory-only mode (path == [memoryPath] on Open).
	path string
}

// Open loads the snapshot from `path` (or [DefaultPath] when empty), or
// keeps state purely in memory when path == ":memory:". A missing file is
// fine — the store starts empty. A file that fails to parse is quarantined
// (renamed aside with a `.broken-<unixts>` suffix) so the user can recover
// it manually, and the store still starts empty: the worst-case outcome of
// a bad file is reset view state, never a crash.
//
// ctx is accepted for caller symmetry with the repository interfaces (which
// both still take ctx); the JSON I/O paths themselves are not context-aware.
func Open(_ context.Context, path string) (*Store, error) {
	if path == "" {
		p, err := DefaultPath()
		if err != nil {
			return nil, err
		}
		path = p
	}
	s := &Store{snap: snapshot{Version: snapshotVersion}}
	if path == memoryPath {
		return s, nil
	}
	s.path = path
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("state: create dir: %w", err)
	}
	s.load()
	return s, nil
}

// Close is a no-op — every save writes synchronously, so there is nothing
// to flush. Kept (and made nil-safe) so callers can `defer store.Close()`
// without guarding against an Open that failed.
func (s *Store) Close() error { return nil }

// load populates the snapshot from disk. Errors other than "file does not
// exist" are downgraded to warnings — the store always mounts.
func (s *Store) load() {
	buf, err := os.ReadFile(s.path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Default().Warn("state: read failed, starting empty", "path", s.path, "err", err)
		}
		return
	}
	var snap snapshot
	if err := json.Unmarshal(buf, &snap); err != nil {
		s.quarantine(err)
		return
	}
	if snap.Version != snapshotVersion {
		s.quarantine(fmt.Errorf("unknown version %d", snap.Version))
		return
	}
	s.snap = snap
}

// quarantine moves the unreadable file aside so the user can inspect or
// restore it. A failed rename falls through to a warning — the next save
// will overwrite the bad file anyway.
func (s *Store) quarantine(reason error) {
	bak := fmt.Sprintf("%s.broken-%d", s.path, time.Now().Unix())
	if err := os.Rename(s.path, bak); err != nil {
		slog.Default().Warn("state: file unreadable, quarantine failed",
			"path", s.path, "reason", reason, "err", err)
		return
	}
	slog.Default().Warn("state: file unreadable, quarantined and starting empty",
		"from", s.path, "to", bak, "reason", reason)
}

// save serializes the in-memory snapshot to disk via temp + rename.
// Caller must hold s.mu.
func (s *Store) save() error {
	if s.path == "" {
		return nil
	}
	buf, err := json.MarshalIndent(s.snap, "", "  ")
	if err != nil {
		return fmt.Errorf("state: marshal: %w", err)
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, "state-*.json.tmp")
	if err != nil {
		return fmt.Errorf("state: create tmp: %w", err)
	}
	tmp := f.Name()
	removeTmp := func() { _ = os.Remove(tmp) }
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		removeTmp()
		return fmt.Errorf("state: write tmp: %w", err)
	}
	// chmod before rename so the destination inherits user-only perms
	// regardless of the process umask.
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		removeTmp()
		return fmt.Errorf("state: chmod tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		removeTmp()
		return fmt.Errorf("state: close tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		removeTmp()
		return fmt.Errorf("state: rename tmp: %w", err)
	}
	return nil
}

func (s *Store) LoadMessagesView(_ context.Context, cluster, topic string) (MessagesView, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	topics, ok := s.snap.MessagesView[cluster]
	if !ok {
		return MessagesView{}, false, nil
	}
	view, ok := topics[topic]
	if !ok {
		return MessagesView{}, false, nil
	}
	return view, true, nil
}

func (s *Store) SaveMessagesView(_ context.Context, cluster, topic string, view MessagesView) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snap.MessagesView == nil {
		s.snap.MessagesView = make(map[string]map[string]MessagesView)
	}
	if s.snap.MessagesView[cluster] == nil {
		s.snap.MessagesView[cluster] = make(map[string]MessagesView)
	}
	s.snap.MessagesView[cluster][topic] = view
	return s.save()
}

// LoadRefreshInterval returns the persisted refresh interval for a screen
// type. The bool is false when no row exists — callers should fall back to
// their config-level default. A stored value of 0 is a real choice
// ("manual") and is returned as (0, true, nil).
func (s *Store) LoadRefreshInterval(_ context.Context, screenID string) (time.Duration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.snap.RefreshIntervals[screenID]
	if !ok {
		return 0, false, nil
	}
	// negative values can't have been written by SaveRefreshInterval
	// (it clamps), but defend against manual edits — treat as "manual".
	if d < 0 {
		d = 0
	}
	return d, true, nil
}

// SaveRefreshInterval upserts the user-chosen interval for a screen type.
// Negative durations are clamped to 0 ("manual") so the on-disk shape
// mirrors the in-memory contract from [components.Refresher].
func (s *Store) SaveRefreshInterval(_ context.Context, screenID string, d time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d < 0 {
		d = 0
	}
	if s.snap.RefreshIntervals == nil {
		s.snap.RefreshIntervals = make(map[string]time.Duration)
	}
	s.snap.RefreshIntervals[screenID] = d
	return s.save()
}
