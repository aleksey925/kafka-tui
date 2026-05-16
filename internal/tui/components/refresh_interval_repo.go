package components

import (
	"context"
	"time"
)

// RefreshIntervalIOTimeout caps the synchronous load (construction) and
// save (post-pick) — a stalled disk would otherwise block the cmd loop.
const RefreshIntervalIOTimeout = 500 * time.Millisecond

// RefreshIntervalRepository persists the user-chosen refresh cadence per
// screen type. nil implementations (or a missing row) mean "no persistence" —
// callers fall back to the config-level default.
//
// Load reports ok=false when no row exists. A stored value of 0 is a real
// user choice ("manual") and is returned as (0, true, nil).
type RefreshIntervalRepository interface {
	LoadRefreshInterval(ctx context.Context, screenID string) (time.Duration, bool, error)
	SaveRefreshInterval(ctx context.Context, screenID string, d time.Duration) error
}

// LoadRefreshIntervalOr reads the persisted cadence for screenID, returning
// fallback on missing rows, errors, or a nil repo.
func LoadRefreshIntervalOr(repo RefreshIntervalRepository, screenID string, fallback time.Duration) time.Duration {
	if repo == nil {
		return fallback
	}
	ctx, cancel := context.WithTimeout(context.Background(), RefreshIntervalIOTimeout)
	defer cancel()
	if d, ok, err := repo.LoadRefreshInterval(ctx, screenID); err == nil && ok {
		return d
	}
	return fallback
}

// SaveRefreshIntervalOrToast persists d under screenID. A nil repo is a
// no-op; a save error surfaces as a warning toast.
func SaveRefreshIntervalOrToast(repo RefreshIntervalRepository, screenID string, d time.Duration, toasts *Toasts) {
	if repo == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), RefreshIntervalIOTimeout)
	defer cancel()
	if err := repo.SaveRefreshInterval(ctx, screenID, d); err != nil && toasts != nil {
		toasts.Push(ToastWarning, "couldn't persist refresh interval: "+err.Error())
	}
}
