package tui

import (
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

// fakeScreen is a stand-in screen used to drive the host's raw-input dispatch
// path without booting a real produce form.
type fakeScreen struct {
	rawInput       bool
	supportsSearch bool
	hasOverlay     bool
	closed         int
	keys           []string
}

func (s *fakeScreen) Init() tea.Cmd { return nil }
func (s *fakeScreen) Update(msg tea.Msg) tea.Cmd {
	if k, ok := msg.(tea.KeyPressMsg); ok {
		s.keys = append(s.keys, k.String())
	}
	return nil
}
func (s *fakeScreen) View() string               { return "" }
func (s *fakeScreen) SetSize(_, _ int)           {}
func (s *fakeScreen) KeyHints() []layout.KeyHint { return nil }
func (s *fakeScreen) WantsRawInput() bool        { return s.rawInput }
func (s *fakeScreen) LatestFlash() (components.Toast, bool) {
	return components.Toast{}, false
}
func (s *fakeScreen) Title() string                  { return "" }
func (s *fakeScreen) Breadcrumb() string             { return "" }
func (s *fakeScreen) RefreshInterval() time.Duration { return 0 }
func (s *fakeScreen) SetRefreshPaused(bool)          {}
func (s *fakeScreen) LastRefresh() time.Time         { return time.Time{} }
func (s *fakeScreen) SupportsRefresh() bool          { return false }
func (s *fakeScreen) SupportsSearch() bool           { return s.supportsSearch }
func (s *fakeScreen) SetSearch(string)               {}
func (s *fakeScreen) ActiveFilter() string           { return "" }
func (s *fakeScreen) HasOverlay() bool               { return s.hasOverlay }
func (s *fakeScreen) Close()                         { s.closed++ }

func keyMsg(s string) tea.KeyPressMsg {
	if len(s) == 1 && s[0] >= ' ' && s[0] < 0x7f {
		return tea.KeyPressMsg{Code: rune(s[0]), Text: s}
	}
	return tea.KeyPressMsg{Code: tea.KeyExtended, Text: s}
}

func TestHandleNormalKey_RawInputBypassesGlobalShortcuts(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	fake := &fakeScreen{rawInput: true}
	m.active = fake

	for _, key := range []string{":", "/", "?", "ctrl+r"} {
		_, _ = m.Update(keyMsg(key))
	}

	assert.Equal(t, []string{":", "/", "?", "ctrl+r"}, fake.keys,
		"raw-input screen must receive every key as a literal")
	assert.Equal(t, ModeNormal, m.mode, "global modes must not activate")
	assert.True(t, m.autoRefresh, "auto-refresh toggle must not fire")
}

func TestHandleNormalKey_NonRawScreenStillSeesGlobalShortcuts(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	fake := &fakeScreen{rawInput: false}
	m.active = fake

	_, _ = m.Update(keyMsg(":"))

	assert.Equal(t, ModeCommand, m.mode)
	assert.Empty(t, fake.keys, "global shortcut must not reach screen")
}

// flashScreen surfaces a single toast, then nothing, so the host's flash
// promotion can be exercised without booting a real screen.
type flashScreen struct {
	fakeScreen
	toast components.Toast
	once  bool
}

func (s *flashScreen) LatestFlash() (components.Toast, bool) {
	if s.once {
		return components.Toast{}, false
	}
	s.once = true
	return s.toast, true
}

func TestPromoteFlash_PromotesScreenToastToBar(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	now := time.Now()
	fake := &flashScreen{toast: components.Toast{
		Level:     components.ToastSuccess,
		Message:   "topic created",
		CreatedAt: now,
		Lifetime:  3 * time.Second,
	}}
	m.active = fake

	_, _ = m.Update(keyMsg("a"))

	assert.Equal(t, "topic created", m.Flash().Text)
	assert.Equal(t, layout.FlashOK, m.Flash().Level)
}

func TestPromoteFlash_ClearsWhenScreenHasNothing(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	now := time.Now()
	fake := &flashScreen{toast: components.Toast{
		Level:     components.ToastError,
		Message:   "boom",
		CreatedAt: now,
		// sticky (lifetime 0): no auto-tick, but next update with no live
		// toast must clear the bar.
	}}
	m.active = fake

	_, _ = m.Update(keyMsg("a"))
	require.Equal(t, "boom", m.Flash().Text)

	// second update — flashScreen.LatestFlash now reports false, so the bar
	// must clear.
	_, _ = m.Update(keyMsg("a"))
	assert.Empty(t, m.Flash().Text)
}

// titledScreen is a fakeScreen variant with a non-empty Title/Breadcrumb,
// so the host's frame chrome can be exercised in isolation.
type titledScreen struct {
	fakeScreen
	title, breadcrumb string
}

func (s *titledScreen) Title() string      { return s.title }
func (s *titledScreen) Breadcrumb() string { return s.breadcrumb }
func (s *titledScreen) View() string       { return "row 1\nrow 2" }

func TestRender_WrapsBodyInFrameWithTitleAndBreadcrumb(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 60, Height: 20})
	m.active = &titledScreen{title: "Topics[42]", breadcrumb: "orders.events"}

	out := m.Render()

	assert.Contains(t, out, "Topics[42]")
	assert.Contains(t, out, "orders.events")
	// rounded border corners must appear once the frame is composed.
	assert.Contains(t, out, "╭")
	assert.Contains(t, out, "╯")
}

func TestHandleNormalKey_CtrlCQuitsEvenInRawInput(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	fake := &fakeScreen{rawInput: true}
	m.active = fake

	_, cmd := m.Update(keyMsg("ctrl+c"))

	assert.True(t, m.quit)
	assert.NotNil(t, cmd)
	assert.Empty(t, fake.keys, "ctrl+c must not be forwarded to the screen")
}

// TestSlash_BlockedWhenScreenDoesNotSupportSearch pins the host contract:
// `/` is silently swallowed (no prompt opens) when the active screen
// reports SupportsSearch=false. Otherwise users would be left typing
// into an inert prompt with no rows to filter (e.g. message detail view).
func TestSlash_BlockedWhenScreenDoesNotSupportSearch(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	m.active = &fakeScreen{supportsSearch: false}

	_, _ = m.Update(keyMsg("/"))

	assert.Equal(t, ModeNormal, m.mode, "search prompt must not open when SupportsSearch=false")
}

// TestSlash_OpensWhenScreenSupportsSearch is the dual: `/` activates
// ModeSearch when the active screen can usefully filter.
func TestSlash_OpensWhenScreenSupportsSearch(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	m.active = &fakeScreen{supportsSearch: true}

	_, _ = m.Update(keyMsg("/"))

	assert.Equal(t, ModeSearch, m.mode)
}

// TestEsc_OverlayPreservesScreen pins the q/esc fallback contract:
// when the active screen reports HasOverlay=true (e.g. messages or
// groups in their detail sub-mode), esc is forwarded to the screen but
// the host suppresses the screen pop, so the user lands back on the
// list rather than being kicked off the screen entirely.
func TestEsc_OverlayPreservesScreen(t *testing.T) {
	// arrange: two-level router so a pop would be observable
	m := New(Options{Width: 80, Height: 24})
	m.router.Push(ScreenClusters)
	m.router.Push(ScreenTopics)
	require.Equal(t, 2, m.router.Depth())
	fake := &fakeScreen{hasOverlay: true}
	m.active = fake

	_, _ = m.Update(keyMsg("esc"))

	assert.Equal(t, []string{"esc"}, fake.keys, "screen must receive esc to close its overlay")
	assert.Equal(t, 2, m.router.Depth(), "host must NOT pop while screen reports an active overlay")
}

// TestEsc_NoOverlayPopsScreen is the dual: without an overlay esc
// pops the active screen (router depth drops by one).
func TestEsc_NoOverlayPopsScreen(t *testing.T) {
	m := New(Options{Width: 80, Height: 24})
	m.router.Push(ScreenClusters)
	m.router.Push(ScreenTopics)
	require.Equal(t, 2, m.router.Depth())
	m.active = &fakeScreen{hasOverlay: false}

	_, _ = m.Update(keyMsg("esc"))

	assert.Equal(t, 1, m.router.Depth(), "host must pop when no overlay is active")
}

// TestPushScreen_ClosesPreviousActive pins the lifecycle contract:
// before swapping the active screen the host must call Close on the
// outgoing one so its background resources (clone goroutines, follow
// sessions) are released instead of leaking until ctx timeout.
func TestPushScreen_ClosesPreviousActive(t *testing.T) {
	m := New(Options{Width: 80, Height: 24})
	prev := &fakeScreen{}
	m.active = prev
	m.router.Push(ScreenClusters)

	m.pushScreen(ScreenTopics)

	assert.Equal(t, 1, prev.closed, "Close must run exactly once on the outgoing screen")
}
