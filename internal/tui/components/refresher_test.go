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

func TestRefresher_SetInterval_BootstrapsChainOnZeroToNonZero(t *testing.T) {
	// arrange — chain is dead because interval starts at 0.
	r := components.NewRefresher(0, nil)
	require.Nil(t, r.Tick(tickMsg{}), "precondition: interval=0 returns nil tick")

	// act
	cmd := r.SetInterval(time.Millisecond, tickMsg{})

	// assert — bootstrap cmd produced, runs and emits the supplied message.
	require.NotNil(t, cmd, "0→>0 must return a bootstrap cmd so the chain restarts")
	assert.Equal(t, time.Millisecond, r.Interval())
	got := cmd()
	_, ok := got.(tickMsg)
	assert.True(t, ok, "bootstrap cmd must emit the supplied message type, got %T", got)
}

func TestRefresher_SetInterval_NoBootstrapOnNonZeroToZero(t *testing.T) {
	// arrange — chain alive.
	r := components.NewRefresher(time.Second, nil)

	// act
	cmd := r.SetInterval(0, tickMsg{})

	// assert — caller has nothing to schedule; in-flight tick burns down.
	assert.Nil(t, cmd, ">0→0 must return nil — the in-flight tick burns down naturally")
	assert.Equal(t, time.Duration(0), r.Interval())
	assert.Nil(t, r.Tick(tickMsg{}), "after setting to 0, subsequent Tick must also return nil")
}

func TestRefresher_SetInterval_NoBootstrapOnNonZeroToNonZero(t *testing.T) {
	// arrange
	r := components.NewRefresher(time.Second, nil)

	// act
	cmd := r.SetInterval(5*time.Second, tickMsg{})

	// assert — chain already alive; next Tick will pick up the new cadence.
	assert.Nil(t, cmd, ">0→>0 must return nil to avoid double-scheduling")
	assert.Equal(t, 5*time.Second, r.Interval())
}

func TestRefresher_SetInterval_NoBootstrapOnZeroToZero(t *testing.T) {
	r := components.NewRefresher(0, nil)
	assert.Nil(t, r.SetInterval(0, tickMsg{}), "0→0 must return nil")
	assert.Equal(t, time.Duration(0), r.Interval())
}

func TestRefresher_SetInterval_ClampsNegativeToZero(t *testing.T) {
	r := components.NewRefresher(time.Second, nil)
	cmd := r.SetInterval(-5*time.Second, tickMsg{})
	assert.Nil(t, cmd, "negative input is treated as 0 — same as >0→0")
	assert.Equal(t, time.Duration(0), r.Interval(),
		"negative interval must be clamped to 0 so screens don't try to schedule a backwards tick")
}

// TestRefresher_EmbedsCleanlyInScreen pins the host-facing usage shape:
// a screen embeds the Refresher as a value and the host calls
// RefreshInterval/LastRefresh through tiny delegating getters. Pointer-
// receiver mutations propagate because the enclosing Model is always
// pointer-passed.
func TestRefresher_EmbedsCleanlyInScreen(t *testing.T) {
	type screen struct {
		refresher components.Refresher
	}
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	s := &screen{refresher: components.NewRefresher(time.Second, func() time.Time { return fixed })}

	s.refresher.MarkSuccess()
	assert.Equal(t, fixed, s.refresher.LastRefresh())

	cmd := s.refresher.Tick(tickMsg{})
	require.NotNil(t, cmd, "Tick must return a cmd for a non-zero interval")
}
