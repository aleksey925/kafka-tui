package components

import (
	"context"
	"time"
)

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
