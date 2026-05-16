package tui

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aleksey925/kafka-tui/internal/state"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
)

// stateRefreshIntervals adapts [state.Store] to
// [components.RefreshIntervalRepository]. Read errors degrade to ok=false so
// the screen falls back to its config-level default; write errors are logged
// and bubbled up so the screen can surface a toast.
type stateRefreshIntervals struct {
	store *state.Store
	log   *slog.Logger
}

func NewStateRefreshIntervals(store *state.Store, log *slog.Logger) components.RefreshIntervalRepository {
	if log == nil {
		log = slog.Default()
	}
	return &stateRefreshIntervals{store: store, log: log}
}

func (s *stateRefreshIntervals) LoadRefreshInterval(ctx context.Context, screenID string) (time.Duration, bool, error) {
	d, ok, err := s.store.LoadRefreshInterval(ctx, screenID)
	if err != nil {
		s.log.Warn("refresh intervals: load failed", "screen", screenID, "err", err)
		return 0, false, nil
	}
	return d, ok, nil
}

func (s *stateRefreshIntervals) SaveRefreshInterval(ctx context.Context, screenID string, d time.Duration) error {
	if err := s.store.SaveRefreshInterval(ctx, screenID, d); err != nil {
		s.log.Warn("refresh intervals: save failed", "screen", screenID, "interval", d, "err", err)
		return fmt.Errorf("refresh interval save: %w", err)
	}
	return nil
}
