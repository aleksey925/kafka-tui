package components

import (
	"time"

	tea "charm.land/bubbletea/v2"
)

// Refresher is a tiny helper screens embed when they need an auto-
// refresh tick chain. It owns the interval, the pause flag, and the
// wall-clock timestamp of the last successful load — three fields that
// were previously duplicated across topics, groups, clusters, and
// logs. It does NOT keep a Bubble Tea command queue; the screen drives
// ticks by calling [Tick] from its Init and after each tick handler.
//
// Typical usage in a screen:
//
//	type Model struct {
//	    refresher components.Refresher
//	    loading   bool
//	    // ...
//	}
//
//	type RefreshTickMsg struct{}
//
//	func (m *Model) Init() tea.Cmd {
//	    return tea.Batch(m.loadCmd(), m.refresher.Tick(RefreshTickMsg{}))
//	}
//
//	func (m *Model) handleTick() tea.Cmd {
//	    next := m.refresher.Tick(RefreshTickMsg{})
//	    if m.loading || m.refresher.Paused() {
//	        return next
//	    }
//	    return tea.Batch(m.loadCmd(), next)
//	}
//
//	func (m *Model) onLoaded(...) {
//	    m.refresher.MarkSuccess()
//	    // ...
//	}
//
// The screen then satisfies [tui.Refreshable] by forwarding three
// trivial getters/setter to the embedded Refresher.
type Refresher struct {
	interval time.Duration
	paused   bool
	last     time.Time
	now      func() time.Time
}

// NewRefresher constructs a refresher with the given cadence. Zero or
// negative interval disables the tick chain — [Tick] returns nil and
// the screen never fires its periodic refresh. now is used for
// [MarkSuccess]; nil falls back to [time.Now].
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

// Paused reports whether ticks should currently skip the workload.
func (r *Refresher) Paused() bool { return r.paused }

// SetPaused toggles the pause flag without stopping the tick chain.
// The screen keeps emitting ticks (so resuming is instantaneous) but
// skips the workload while paused — see the example in the package
// doc.
func (r *Refresher) SetPaused(paused bool) { r.paused = paused }

// LastRefresh returns the wall-clock time of the most recent
// [MarkSuccess] call, or the zero time when none has happened yet.
// Drives the chrome's "X ago" indicator.
func (r *Refresher) LastRefresh() time.Time { return r.last }

// MarkSuccess records that a load completed; the screen calls it on
// receiving its load-result message. Errors should NOT call this so
// "X ago" reflects the last *successful* refresh.
func (r *Refresher) MarkSuccess() { r.last = r.now() }

// Tick returns a [tea.Cmd] that emits msg after one [Interval], or
// nil when the refresher is disabled. The screen passes its concrete
// tick-message value (e.g. `RefreshTickMsg{}`) so its Update switch
// can discriminate ticks from data messages.
func (r *Refresher) Tick(msg tea.Msg) tea.Cmd {
	if r.interval <= 0 {
		return nil
	}
	return tea.Tick(r.interval, func(time.Time) tea.Msg { return msg })
}
