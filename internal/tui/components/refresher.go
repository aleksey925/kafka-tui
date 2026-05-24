package components

import (
	"time"

	tea "charm.land/bubbletea/v2"
)

// Refresher is a helper that screens embed when they need an auto-refresh
// tick chain. It owns the interval and the wall-clock timestamp of the last
// successful load. The screen drives ticks by calling [Tick] from its Init
// and after each tick handler.
type Refresher struct {
	interval time.Duration
	last     time.Time
	now      func() time.Time
	// manual flips on when the user presses `r` (vs. an auto tick) so the
	// handler can surface a success toast on the user-initiated cycle
	// only. CLAUDE.md "Auto-refresh: quiet by default" is the contract.
	manual bool
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

// SetInterval changes the cadence at runtime. Negative values clamp to 0
// (consistent with [NewRefresher]). The returned cmd bootstraps the tick
// chain on a 0 → >0 transition — without it the chain never starts because
// the previous [Tick] call returned nil and nothing scheduled a successor.
// Other transitions return nil: >0 → 0 lets the in-flight tick burn down
// naturally, and >0 → >0 leaves the in-flight tick to fire once at the
// old cadence before the next [Tick] picks up the new value.
func (r *Refresher) SetInterval(d time.Duration, tickMsg tea.Msg) tea.Cmd {
	if d < 0 {
		d = 0
	}
	bootstrap := r.interval == 0 && d > 0
	r.interval = d
	if bootstrap {
		return r.Tick(tickMsg)
	}
	return nil
}

// LastRefresh returns the wall-clock time of the most recent [MarkSuccess]
// call, or the zero time when none has happened yet.
func (r *Refresher) LastRefresh() time.Time { return r.last }

// MarkSuccess records that a load completed. Errors should NOT call this so
// "X ago" reflects the last *successful* refresh.
func (r *Refresher) MarkSuccess() { r.last = r.now() }

// MarkManual flags the in-flight load as user-initiated. The next
// [ConsumeManual] call returns true once and clears the flag.
func (r *Refresher) MarkManual() { r.manual = true }

// ConsumeManual returns true if the in-flight load is user-initiated and
// clears the flag. Use it to gate the "loud" success toast — auto ticks
// must stay silent per the [auto-refresh contract] in CLAUDE.md.
//
// Also call it on the error path so a failed manual refresh doesn't bleed
// its "loud" intent into the next (auto) cycle.
func (r *Refresher) ConsumeManual() bool {
	out := r.manual
	r.manual = false
	return out
}

// Tick returns a [tea.Cmd] that emits msg after one [Interval], or nil when
// the refresher is disabled.
func (r *Refresher) Tick(msg tea.Msg) tea.Cmd {
	if r.interval <= 0 {
		return nil
	}
	return tea.Tick(r.interval, func(time.Time) tea.Msg { return msg })
}
