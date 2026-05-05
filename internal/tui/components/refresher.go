package components

import (
	"time"

	tea "charm.land/bubbletea/v2"
)

// Refresher is a helper that screens embed when they need an auto-refresh
// tick chain. It owns the interval, the pause flag, and the wall-clock
// timestamp of the last successful load. The screen drives ticks by calling
// [Tick] from its Init and after each tick handler.
type Refresher struct {
	interval time.Duration
	paused   bool
	last     time.Time
	now      func() time.Time
}

// NewRefresher constructs a refresher. Zero or negative interval disables
// the tick chain — [Tick] returns nil. nil now falls back to [time.Now].
func NewRefresher(interval time.Duration, now func() time.Time) Refresher {
	if now == nil {
		now = time.Now
	}
	if interval < 0 {
		interval = 0
	}
	return Refresher{interval: interval, now: now}
}

// Interval is the configured cadence (0 means no auto-refresh).
func (r *Refresher) Interval() time.Duration { return r.interval }

func (r *Refresher) Paused() bool { return r.paused }

// SetPaused toggles the pause flag without stopping the tick chain.
func (r *Refresher) SetPaused(paused bool) { r.paused = paused }

// LastRefresh returns the wall-clock time of the most recent [MarkSuccess]
// call, or the zero time when none has happened yet.
func (r *Refresher) LastRefresh() time.Time { return r.last }

// MarkSuccess records that a load completed. Errors should NOT call this so
// "X ago" reflects the last *successful* refresh.
func (r *Refresher) MarkSuccess() { r.last = r.now() }

// Tick returns a [tea.Cmd] that emits msg after one [Interval], or nil when
// the refresher is disabled.
func (r *Refresher) Tick(msg tea.Msg) tea.Cmd {
	if r.interval <= 0 {
		return nil
	}
	return tea.Tick(r.interval, func(time.Time) tea.Msg { return msg })
}
