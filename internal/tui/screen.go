package tui

import (
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

// Screen is the minimum contract every hosted screen must satisfy. The
// host wires keystrokes, geometry, and rendering through these methods
// for any screen on the router stack.
//
// Optional behaviors live on the sub-interfaces below ([Refreshable],
// [Searchable], [Overlayable], [RawInputs], [Flasher], [Closer]) — the
// host probes for them via type assertion at the call site and falls
// back to sensible defaults when a screen does not implement them. This
// keeps screens that need only the basics (e.g. a static snapshot view)
// free of empty no-op methods.
type Screen interface {
	Init() tea.Cmd
	// Update routes a message into the screen and returns any follow-up
	// command. The screen mutates itself in place — no model swap is
	// needed (Bubble Tea's "return new model" pattern is unused here).
	Update(tea.Msg) tea.Cmd
	View() string
	SetSize(w, h int)
	KeyHints() []layout.KeyHint
	// Title is rendered in the top-left of the body frame (e.g.
	// "Topics[42]"). Empty hides the slot.
	Title() string
	// Breadcrumb is rendered in the top-right of the body frame, typically
	// the selected row identifier. Empty hides it.
	Breadcrumb() string
}

// Refreshable is implemented by screens whose body holds live broker
// data with a periodic auto-refresh tick (topics, groups, clusters).
// The host queries [RefreshInterval] for the chrome indicator and
// pauses ticking when the user toggles auto-refresh off.
type Refreshable interface {
	// RefreshInterval is the configured auto-refresh cadence (0 disables
	// auto-refresh — the screen still satisfies Refreshable but the
	// chrome shows "off" rather than "—").
	RefreshInterval() time.Duration
	// SetRefreshPaused puts the screen's refresh ticker on hold without
	// stopping it; flipping back to false resumes the regular cadence.
	SetRefreshPaused(bool)
	// LastRefresh returns the wall-clock time of the most recent
	// successful load, or zero when no load has completed yet. Drives
	// the chrome's "X ago" indicator.
	LastRefresh() time.Time
}

// Searchable is implemented by screens whose primary table can be
// filtered by the host's `/` prompt. Implementing the interface alone
// is enough — the host treats search as always available. Screens that
// need to dynamically disable search in some sub-mode also implement
// the [SearchGate] sub-interface.
type Searchable interface {
	// SetSearch applies a host-driven filter query to the screen's
	// primary table. Each keystroke from the prompt is forwarded here
	// so rows filter live; empty string clears the filter.
	SetSearch(string)
	// ActiveFilter returns the current applied query (empty when no
	// filter is set). The host uses it to drive the esc cascade
	// "clear filter first, then pop".
	ActiveFilter() string
}

// SearchGate is an optional refinement of [Searchable] for screens
// that dynamically disable search depending on their internal state
// (e.g. the messages screen has no rows to filter while in detail
// sub-mode). When [SearchAvailable] returns false the host silently
// swallows `/` instead of opening an inert prompt.
type SearchGate interface {
	Searchable
	SearchAvailable() bool
}

// Overlayable is implemented by screens that may render a transient
// modal-like overlay (confirm dialog, in-flight progress popup, modal
// detail sub-mode) which owns esc. When [HasOverlay] returns true the
// host yields esc to the screen instead of running its filter-clear
// or pop cascade.
type Overlayable interface {
	HasOverlay() bool
}

// RawInputs is implemented by screens whose primary state is editing
// free-form text (forms). When [WantsRawInput] returns true every key
// is routed straight to the screen, bypassing global shortcuts like
// `:`, `/`, `?`, and `ctrl+r`. ctrl+c always remains global.
type RawInputs interface {
	WantsRawInput() bool
}

// Flasher is implemented by screens that surface toast notifications.
// The host promotes the latest live toast to the global flash bar
// after every Update.
type Flasher interface {
	LatestFlash() (components.Toast, bool)
}

// HelpProvider is implemented by screens that want to contribute their
// own categorized key/description sections to the `?` overlay (k9s-style).
// The bottom hints bar is intentionally narrow (≤ 6 entries) — the help
// overlay is where the full set of bindings lives. Screens that don't
// implement this interface fall back to the global sections only.
type HelpProvider interface {
	HelpSections() []help.Section
}

// Closer is implemented by screens that own background resources
// (long-running goroutines, kafka client subscriptions, file handles).
// The host calls Close before swapping the active screen so resources
// are released instead of leaking until their outer context times out.
// Implementations must be idempotent.
type Closer interface {
	Close()
}

// --- host-side optional-capability helpers ---

// screenRefreshInterval returns the screen's configured auto-refresh
// interval, or 0 when the screen does not implement [Refreshable].
func screenRefreshInterval(s Screen) time.Duration {
	if r, ok := s.(Refreshable); ok {
		return r.RefreshInterval()
	}
	return 0
}

// screenLastRefresh returns the screen's last-refresh timestamp, or
// zero when the screen does not implement [Refreshable].
func screenLastRefresh(s Screen) time.Time {
	if r, ok := s.(Refreshable); ok {
		return r.LastRefresh()
	}
	return time.Time{}
}

// screenSupportsRefresh reports whether the screen has refresh
// machinery at all (i.e. implements [Refreshable]). Distinct from
// [Refreshable.RefreshInterval] returning 0 — that means refresh is
// configured off; this means the very concept doesn't apply.
func screenSupportsRefresh(s Screen) bool {
	_, ok := s.(Refreshable)
	return ok
}

// setScreenRefreshPaused forwards the pause flag to a [Refreshable]
// screen, or no-ops when the screen has no refresh ticker.
func setScreenRefreshPaused(s Screen, paused bool) {
	if r, ok := s.(Refreshable); ok {
		r.SetRefreshPaused(paused)
	}
}

// screenSupportsSearch reports whether the host should open the `/`
// prompt over this screen. Screens that implement [Searchable] but
// not [SearchGate] are always searchable; screens with [SearchGate]
// answer per their current state.
func screenSupportsSearch(s Screen) bool {
	if g, ok := s.(SearchGate); ok {
		return g.SearchAvailable()
	}
	_, ok := s.(Searchable)
	return ok
}

// setScreenSearch forwards a query to a [Searchable] screen, or
// no-ops when the screen has no filter machinery.
func setScreenSearch(s Screen, query string) {
	if sr, ok := s.(Searchable); ok {
		sr.SetSearch(query)
	}
}

// screenActiveFilter returns the screen's current filter, or "" when
// the screen is not [Searchable].
func screenActiveFilter(s Screen) string {
	if sr, ok := s.(Searchable); ok {
		return sr.ActiveFilter()
	}
	return ""
}

// screenHasOverlay reports whether the screen has a modal overlay
// open. Defaults to false when the screen is not [Overlayable].
func screenHasOverlay(s Screen) bool {
	if o, ok := s.(Overlayable); ok {
		return o.HasOverlay()
	}
	return false
}

// screenWantsRawInput reports whether the screen wants every key as
// raw input. Defaults to false for screens that are not [RawInputs].
func screenWantsRawInput(s Screen) bool {
	if r, ok := s.(RawInputs); ok {
		return r.WantsRawInput()
	}
	return false
}

// screenLatestFlash returns the screen's latest live toast, or
// (zero, false) when the screen is not a [Flasher].
func screenLatestFlash(s Screen) (components.Toast, bool) {
	if f, ok := s.(Flasher); ok {
		return f.LatestFlash()
	}
	return components.Toast{}, false
}

// screenHelpSections returns the screen's contributed help categories,
// or nil when the screen does not implement [HelpProvider].
func screenHelpSections(s Screen) []help.Section {
	if hp, ok := s.(HelpProvider); ok {
		return hp.HelpSections()
	}
	return nil
}

// closeScreen invokes [Closer.Close] when the screen owns background
// resources, otherwise no-ops.
func closeScreen(s Screen) {
	if c, ok := s.(Closer); ok {
		c.Close()
	}
}
