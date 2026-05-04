package components_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
)

// tickMsg is a stand-in screen-side tick type. Tests assert the cmd
// returned by Tick produces exactly this value when invoked.
type tickMsg struct{}

func TestNewRefresher_DefaultsToTimeNowWhenNilClock(t *testing.T) {
	// arrange
	r := components.NewRefresher(time.Second, nil)

	// act
	r.MarkSuccess()

	// assert: a real time.Now-driven timestamp landed within a generous window.
	assert.WithinDuration(t, time.Now(), r.LastRefresh(), time.Second,
		"MarkSuccess with nil clock must use time.Now")
}

func TestNewRefresher_NegativeIntervalIsClampedToZero(t *testing.T) {
	r := components.NewRefresher(-5*time.Second, nil)
	assert.Equal(t, time.Duration(0), r.Interval(),
		"negative interval must be clamped to 0 so screens don't try to schedule a backwards tick")
}

func TestRefresher_PausedSetterAndGetter(t *testing.T) {
	r := components.NewRefresher(time.Second, nil)
	require.False(t, r.Paused(), "fresh refresher must not be paused")

	r.SetPaused(true)
	assert.True(t, r.Paused())

	r.SetPaused(false)
	assert.False(t, r.Paused(), "SetPaused(false) must resume")
}

func TestRefresher_MarkSuccessUsesInjectedClock(t *testing.T) {
	// arrange — fixed clock so the assertion is exact.
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r := components.NewRefresher(time.Second, func() time.Time { return fixed })

	// act
	r.MarkSuccess()

	// assert
	assert.Equal(t, fixed, r.LastRefresh())
}

func TestRefresher_LastRefreshIsZeroBeforeMarkSuccess(t *testing.T) {
	r := components.NewRefresher(time.Second, nil)
	assert.True(t, r.LastRefresh().IsZero(),
		"refresher with no MarkSuccess yet must report zero time so the chrome shows blank ago-counter")
}

func TestRefresher_TickReturnsNilWhenIntervalIsZero(t *testing.T) {
	r := components.NewRefresher(0, nil)
	assert.Nil(t, r.Tick(tickMsg{}),
		"Tick with interval=0 must return nil so screens treat refresh as disabled")
}

func TestRefresher_TickEmitsScreenSuppliedMessage(t *testing.T) {
	// arrange — short interval so the test isn't slow.
	r := components.NewRefresher(time.Millisecond, nil)
	cmd := r.Tick(tickMsg{})
	require.NotNil(t, cmd, "Tick with interval>0 must return a cmd")

	// act — running tea.Tick produces a message after the duration. We
	// invoke the cmd directly so the test stays deterministic.
	got := cmd()

	// assert — the cmd produces our typed value (tea.Tick wraps it).
	_, ok := got.(tickMsg)
	assert.True(t, ok, "Tick cmd must produce the screen-supplied message type, got %T", got)
}

func TestRefresher_IntervalGetter(t *testing.T) {
	r := components.NewRefresher(7*time.Second, nil)
	assert.Equal(t, 7*time.Second, r.Interval())
}

// TestRefresher_EmbedsCleanlyInScreen pins the host-facing usage shape:
// a screen embeds the Refresher as a value, the host calls
// SetRefreshPaused/RefreshInterval/LastRefresh through tiny delegating
// getters, and pointer-receiver mutations propagate because the
// enclosing Model is always pointer-passed.
func TestRefresher_EmbedsCleanlyInScreen(t *testing.T) {
	type screen struct {
		refresher components.Refresher
	}
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	s := &screen{refresher: components.NewRefresher(time.Second, func() time.Time { return fixed })}

	// pause through the embedded value — Go takes &s.refresher because s is *screen
	s.refresher.SetPaused(true)
	require.True(t, s.refresher.Paused())

	s.refresher.MarkSuccess()
	assert.Equal(t, fixed, s.refresher.LastRefresh())

	// Tick still emits the cmd even when paused — pause only affects whether
	// the screen runs the workload, not the tick chain.
	cmd := s.refresher.Tick(tickMsg{})
	require.NotNil(t, cmd, "Tick must keep firing while paused so resume is instantaneous")
	// type assertion to tea.Cmd is implicit in the require.NotNil — the
	// signature of Tick already pins the return.
}
