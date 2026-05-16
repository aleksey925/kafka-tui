package tui

import (
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

// Screen is the minimum contract every hosted screen must satisfy.
// Optional behaviors live on the sub-interfaces below ([Refreshable],
// [Searchable], [Overlayable], [RawInputs], [Flasher], [Closer]) and the
// host probes for them via type assertion at the call site.
type Screen interface {
	Init() tea.Cmd
	// Update mutates the screen in place — Bubble Tea's "return new model"
	// pattern is intentionally unused here.
	Update(tea.Msg) tea.Cmd
	View() string
	SetSize(w, h int)
	KeyHints() []layout.KeyHint
	// Title is rendered in the top-left of the body frame. Empty hides it.
	Title() string
	// Breadcrumb is rendered in the top-right of the body frame. Empty hides it.
	Breadcrumb() string
}

// Refreshable is implemented by screens with a periodic auto-refresh tick.
type Refreshable interface {
	// RefreshInterval is the auto-refresh cadence; 0 means the user picked
	// Manual (no ticks) — the chrome shows "manual".
	RefreshInterval() time.Duration
	// LastRefresh is the wall-clock time of the most recent successful load,
	// or zero when no load has completed yet. Drives the "X ago" indicator.
	LastRefresh() time.Time
}

// RefreshConfigurable is implemented by screens that let the user change
// the auto-refresh cadence at runtime via the ctrl+r popup. Separated from
// [Refreshable] so test stubs can opt out and so a future read-only-status
// screen could implement [Refreshable] without owning a picker.
type RefreshConfigurable interface {
	OpenRefreshPicker()
}

// Searchable is implemented by screens whose primary table can be
// filtered by the host's `/` prompt. Screens that need to dynamically
// disable search in a sub-mode also implement [SearchGate].
type Searchable interface {
	SetSearch(string)
	ActiveFilter() string
}

// SearchGate refines [Searchable] for screens that disable search
// depending on internal state (e.g. messages detail sub-mode).
type SearchGate interface {
	Searchable
	SearchAvailable() bool
}

// Overlayable is implemented by screens that may render a transient
// modal-like overlay which owns esc. When HasOverlay returns true the
// host yields esc to the screen instead of running its filter-clear or
// pop cascade.
type Overlayable interface {
	HasOverlay() bool
}

// RawInputs is implemented by screens whose primary state is editing
// free-form text. When WantsRawInput returns true every key is routed
// straight to the screen, bypassing global shortcuts. ctrl+c remains global.
type RawInputs interface {
	WantsRawInput() bool
}

// Flasher is implemented by screens that surface toast notifications.
type Flasher interface {
	LatestFlash() (components.Toast, bool)
}

// HelpProvider is implemented by screens that contribute their own
// categorized sections to the `?` overlay.
type HelpProvider interface {
	HelpSections() []help.Section
}

// Closer is implemented by screens that own background resources.
// The host calls Close before swapping the active screen.
// Implementations must be idempotent.
type Closer interface {
	Close()
}

func screenRefreshInterval(s Screen) time.Duration {
	if r, ok := s.(Refreshable); ok {
		return r.RefreshInterval()
	}
	return 0
}

func screenLastRefresh(s Screen) time.Time {
	if r, ok := s.(Refreshable); ok {
		return r.LastRefresh()
	}
	return time.Time{}
}

// screenSupportsRefresh reports whether the screen has refresh machinery
// at all. Distinct from RefreshInterval returning 0 (refresh configured
// off) — this means the concept doesn't apply.
func screenSupportsRefresh(s Screen) bool {
	_, ok := s.(Refreshable)
	return ok
}

// screenOpenRefreshPicker asks the active screen to mount its refresh-interval
// picker, returning false when the screen doesn't implement
// [RefreshConfigurable] — ctrl+r has nothing to do there.
func screenOpenRefreshPicker(s Screen) bool {
	rc, ok := s.(RefreshConfigurable)
	if !ok {
		return false
	}
	rc.OpenRefreshPicker()
	return true
}

func screenSupportsSearch(s Screen) bool {
	if g, ok := s.(SearchGate); ok {
		return g.SearchAvailable()
	}
	_, ok := s.(Searchable)
	return ok
}

func setScreenSearch(s Screen, query string) {
	if sr, ok := s.(Searchable); ok {
		sr.SetSearch(query)
	}
}

func screenActiveFilter(s Screen) string {
	if sr, ok := s.(Searchable); ok {
		return sr.ActiveFilter()
	}
	return ""
}

func screenHasOverlay(s Screen) bool {
	if o, ok := s.(Overlayable); ok {
		return o.HasOverlay()
	}
	return false
}

func screenWantsRawInput(s Screen) bool {
	if r, ok := s.(RawInputs); ok {
		return r.WantsRawInput()
	}
	return false
}

func screenLatestFlash(s Screen) (components.Toast, bool) {
	if f, ok := s.(Flasher); ok {
		return f.LatestFlash()
	}
	return components.Toast{}, false
}

func screenHelpSections(s Screen) []help.Section {
	if hp, ok := s.(HelpProvider); ok {
		return hp.HelpSections()
	}
	return nil
}

func closeScreen(s Screen) {
	if c, ok := s.(Closer); ok {
		c.Close()
	}
}
