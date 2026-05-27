package tui

import (
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

// minimalScreen is a Screen-only screen — it satisfies the mandatory
// methods but none of the optional sub-interfaces. Used to verify the
// host's optional-capability helpers gracefully fall back to defaults.
type minimalScreen struct{}

func (minimalScreen) Init() tea.Cmd              { return nil }
func (minimalScreen) Update(tea.Msg) tea.Cmd     { return nil }
func (minimalScreen) View() string               { return "" }
func (minimalScreen) SetSize(_, _ int)           {}
func (minimalScreen) KeyHints() []layout.KeyHint { return nil }
func (minimalScreen) Title() string              { return "" }
func (minimalScreen) Breadcrumb() string         { return "" }

// alwaysSearchable implements [Searchable] but not [SearchGate]. The
// host should treat it as always-searchable.
type alwaysSearchable struct {
	minimalScreen
	filter string
}

func (s *alwaysSearchable) SetSearch(q string)   { s.filter = q }
func (s *alwaysSearchable) ActiveFilter() string { return s.filter }

// gatedSearchable implements [SearchGate]: search availability is
// dynamic. Used to verify the host honors the runtime gate.
type gatedSearchable struct {
	alwaysSearchable
	available bool
}

func (s *gatedSearchable) SearchAvailable() bool { return s.available }

// refreshable implements [Refreshable] for tests that need a screen
// with a real refresh ticker.
type refreshable struct {
	minimalScreen
	interval time.Duration
	last     time.Time
}

func (r *refreshable) RefreshInterval() time.Duration { return r.interval }
func (r *refreshable) LastRefresh() time.Time         { return r.last }

// closable counts how many times Close was invoked.
type closable struct {
	minimalScreen
	closed int
}

func (c *closable) Close() { c.closed++ }

// flasher returns a fixed toast.
type flasher struct {
	minimalScreen
	toast components.Toast
	live  bool
}

func (f *flasher) LatestFlash() (components.Toast, bool) {
	return f.toast, f.live
}

// rawScreen implements [RawInputs].
type rawScreen struct {
	minimalScreen
	raw bool
}

func (s *rawScreen) WantsRawInput() bool { return s.raw }

// overlay implements [Overlayable].
type overlay struct {
	minimalScreen
	open bool
}

func (o *overlay) HasOverlay() bool { return o.open }

// --- screenSupportsRefresh ---

func TestScreenSupportsRefresh_TrueForRefreshable(t *testing.T) {
	r := &refreshable{interval: time.Second}
	assert.True(t, screenSupportsRefresh(r))
}

func TestScreenSupportsRefresh_FalseForMinimalScreen(t *testing.T) {
	assert.False(t, screenSupportsRefresh(minimalScreen{}))
}

// --- screenRefreshInterval / screenLastRefresh defaults ---

func TestScreenRefreshInterval_ZeroForNonRefreshable(t *testing.T) {
	assert.Equal(t, time.Duration(0), screenRefreshInterval(minimalScreen{}))
}

func TestScreenLastRefresh_ZeroForNonRefreshable(t *testing.T) {
	assert.True(t, screenLastRefresh(minimalScreen{}).IsZero())
}

// --- screenSupportsSearch ---

func TestScreenSupportsSearch_TrueForSearchableWithoutGate(t *testing.T) {
	assert.True(t, screenSupportsSearch(&alwaysSearchable{}),
		"Searchable without SearchGate is always-on — host opens the prompt")
}

func TestScreenSupportsSearch_FalseForNonSearchable(t *testing.T) {
	assert.False(t, screenSupportsSearch(minimalScreen{}),
		"a screen that doesn't implement Searchable cannot filter — host must swallow `/`")
}

func TestScreenSupportsSearch_HonorsSearchGate(t *testing.T) {
	g := &gatedSearchable{available: false}
	assert.False(t, screenSupportsSearch(g),
		"SearchGate.SearchAvailable=false must override the inherited Searchable")

	g.available = true
	assert.True(t, screenSupportsSearch(g))
}

// --- setScreenSearch / screenActiveFilter defaults ---

func TestSetScreenSearch_NoOpForNonSearchable(t *testing.T) {
	// must not panic; happens whenever the user types in the host's
	// `/` prompt while on a non-searchable sub-screen (defensive only —
	// SupportsSearch=false should already block the prompt opening).
	setScreenSearch(minimalScreen{}, "anything")
}

func TestScreenActiveFilter_EmptyForNonSearchable(t *testing.T) {
	assert.Empty(t, screenActiveFilter(minimalScreen{}))
}

func TestScreenActiveFilter_RoundTripsSearchableState(t *testing.T) {
	s := &alwaysSearchable{}
	setScreenSearch(s, "foo")
	assert.Equal(t, "foo", screenActiveFilter(s))
}

// --- screenSnapshot / screenRestore defaults ---

func TestScreenSnapshot_NoneForMinimalScreen(t *testing.T) {
	_, ok := screenSnapshot(minimalScreen{})
	assert.False(t, ok, "non-Stateful, non-Searchable screens have nothing to round-trip")
}

func TestScreenSnapshot_NoneForSearchableWithEmptyFilter(t *testing.T) {
	_, ok := screenSnapshot(&alwaysSearchable{})
	assert.False(t, ok, "empty filter must not pollute the session-state map")
}

func TestScreenSnapshotRestore_RoundTripsFilterForSearchableOnly(t *testing.T) {
	src := &alwaysSearchable{filter: "ord"}
	blob, ok := screenSnapshot(src)
	assert.True(t, ok, "applied filter must be captured for Searchable screens")

	dst := &alwaysSearchable{}
	screenRestore(dst, blob)
	assert.Equal(t, "ord", dst.filter, "filter must restore through the Searchable fallback")
}

func TestScreenRestore_IgnoresUnknownBlobOnMinimalScreen(t *testing.T) {
	// the host never calls Restore on a screen for which it never called
	// Snapshot, but defending against a stray blob keeps the helper safe.
	screenRestore(minimalScreen{}, defaultFilterState{filter: "x"})
}

// statefulScreen takes precedence over the Searchable fallback when both
// are implemented — Stateful is the opt-in for screens that need to
// preserve more than just the filter.
type statefulScreen struct {
	alwaysSearchable
	saved any
}

func (s *statefulScreen) Snapshot() any { return "from-stateful" }
func (s *statefulScreen) Restore(b any) { s.saved = b }

func TestScreenSnapshot_PrefersStatefulOverSearchableFallback(t *testing.T) {
	s := &statefulScreen{alwaysSearchable: alwaysSearchable{filter: "ord"}}
	blob, ok := screenSnapshot(s)
	assert.True(t, ok)
	assert.Equal(t, "from-stateful", blob, "Stateful must win — its Snapshot owns the contract")
}

func TestScreenRestore_RoutesToStatefulWhenImplemented(t *testing.T) {
	s := &statefulScreen{}
	screenRestore(s, "payload")
	assert.Equal(t, "payload", s.saved, "Restore must call Stateful, not the filter fallback")
}

// --- screenHasOverlay ---

func TestScreenHasOverlay_FalseForNonOverlayable(t *testing.T) {
	assert.False(t, screenHasOverlay(minimalScreen{}))
}

func TestScreenHasOverlay_HonorsOverlayable(t *testing.T) {
	o := &overlay{open: true}
	assert.True(t, screenHasOverlay(o))
	o.open = false
	assert.False(t, screenHasOverlay(o))
}

// --- screenWantsRawInput ---

func TestScreenWantsRawInput_FalseForNonRawScreen(t *testing.T) {
	assert.False(t, screenWantsRawInput(minimalScreen{}),
		"screens without RawInputs treat keys via the global pipeline")
}

func TestScreenWantsRawInput_HonorsRawInputs(t *testing.T) {
	r := &rawScreen{raw: true}
	assert.True(t, screenWantsRawInput(r))
	r.raw = false
	assert.False(t, screenWantsRawInput(r))
}

// --- screenLatestFlash ---

func TestScreenLatestFlash_NoneForNonFlasher(t *testing.T) {
	_, ok := screenLatestFlash(minimalScreen{})
	assert.False(t, ok, "non-Flasher screens must report 'no live toast' so the chrome bar stays clear")
}

func TestScreenLatestFlash_ForwardsToFlasher(t *testing.T) {
	f := &flasher{
		toast: components.Toast{Level: components.ToastSuccess, Message: "ok"},
		live:  true,
	}
	got, ok := screenLatestFlash(f)
	assert.True(t, ok)
	assert.Equal(t, "ok", got.Message)
}

// --- closeScreen ---

func TestCloseScreen_NoOpForNonCloser(t *testing.T) {
	// the host calls closeScreen on every active screen before swap;
	// non-Closer screens must accept this silently.
	closeScreen(minimalScreen{})
}

func TestCloseScreen_InvokesCloseOnCloser(t *testing.T) {
	c := &closable{}
	closeScreen(c)
	assert.Equal(t, 1, c.closed)

	closeScreen(c)
	assert.Equal(t, 2, c.closed, "closeScreen must remain idempotent across repeated calls — no guard")
}
