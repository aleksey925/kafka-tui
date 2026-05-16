package components_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestToasts_PushAndLen(t *testing.T) {
	q := components.NewToasts()
	q.Push(components.ToastInfo, "hello")
	q.Push(components.ToastSuccess, "saved")
	assert.Equal(t, 2, q.Len())
}

func TestToasts_NonStickyExpire(t *testing.T) {
	now := time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	q := components.NewToasts(components.WithToastClock(clock))

	q.Push(components.ToastInfo, "info") // 3s lifetime
	q.PushWithLifetime(components.ToastWarning, "warn", 10*time.Second)

	// after 2s both still alive
	now = now.Add(2 * time.Second)
	q.Tick()
	assert.Equal(t, 2, q.Len())

	// after 5s total: info (3s) expired, warn (10s) still alive
	now = now.Add(3 * time.Second)
	q.Tick()
	assert.Equal(t, 1, q.Len())
	assert.Equal(t, "warn", q.Items()[0].Message)

	// after 11s total: warn also expired
	now = now.Add(6 * time.Second)
	q.Tick()
	assert.Equal(t, 0, q.Len())
}

func TestToasts_StickyErrorPersists(t *testing.T) {
	now := time.Now()
	q := components.NewToasts(components.WithToastClock(func() time.Time { return now }))
	q.Push(components.ToastError, "boom")

	now = now.Add(time.Hour)
	q.Tick()
	assert.Equal(t, 1, q.Len())
}

func TestToasts_DismissTopSticky(t *testing.T) {
	q := components.NewToasts()
	q.Push(components.ToastInfo, "info")
	q.Push(components.ToastError, "err1")
	q.Push(components.ToastError, "err2")

	dismissed := q.DismissTopSticky()
	assert.True(t, dismissed)

	items := q.Items()
	assert.Len(t, items, 2)
	assert.Equal(t, "err1", items[1].Message)
	assert.Equal(t, components.ToastError, items[1].Level)
}

func TestToasts_DismissNoStickyReturnsFalse(t *testing.T) {
	q := components.NewToasts()
	q.Push(components.ToastInfo, "info")
	assert.False(t, q.DismissTopSticky())
}

func TestToasts_KeyPressDismissesSticky(t *testing.T) {
	q := components.NewToasts()
	q.Push(components.ToastError, "boom")
	q, _ = q.Update(keyPressMsg("x"))
	assert.Equal(t, 0, q.Len())
}

func TestToasts_KeyPressIgnoresNonSticky(t *testing.T) {
	now := time.Now()
	q := components.NewToasts(components.WithToastClock(func() time.Time { return now }))
	q.Push(components.ToastInfo, "info")
	q, _ = q.Update(keyPressMsg("x"))
	assert.Equal(t, 1, q.Len())
}

func TestToasts_ViewRendersLevels(t *testing.T) {
	q := components.NewToasts()
	q.Push(components.ToastSuccess, "ok-msg")
	q.Push(components.ToastWarning, "warn-msg")
	q.Push(components.ToastError, "err-msg")

	out := q.View()
	assert.Contains(t, out, "ok-msg")
	assert.Contains(t, out, "warn-msg")
	assert.Contains(t, out, "err-msg")
	assert.Contains(t, out, "[OK]")
	assert.Contains(t, out, "[WARN]")
	assert.Contains(t, out, "[ERR]")
}

func TestToasts_ViewEmptyReturnsEmpty(t *testing.T) {
	q := components.NewToasts()
	assert.Empty(t, q.View())
}

func TestToast_StickyForErrors(t *testing.T) {
	tt := components.Toast{Level: components.ToastError, Lifetime: 0}
	assert.True(t, tt.Sticky())

	tt = components.Toast{Level: components.ToastInfo, Lifetime: 3 * time.Second}
	assert.False(t, tt.Sticky())
}

func TestToasts_LatestReturnsMostRecent(t *testing.T) {
	q := components.NewToasts()
	q.Push(components.ToastInfo, "first")
	q.Push(components.ToastSuccess, "second")

	got, ok := q.Latest()
	require.True(t, ok)
	assert.Equal(t, "second", got.Message)
}

func TestToasts_LatestEmptyReturnsFalse(t *testing.T) {
	q := components.NewToasts()
	_, ok := q.Latest()
	assert.False(t, ok)
}

func TestToasts_LatestPrunesExpiredBeforeReturning(t *testing.T) {
	now := time.Now()
	clk := &now
	q := components.NewToasts(components.WithToastClock(func() time.Time { return *clk }))
	q.PushWithLifetime(components.ToastInfo, "stale", 100*time.Millisecond)

	*clk = now.Add(time.Second)
	_, ok := q.Latest()
	assert.False(t, ok, "expired non-sticky toast must not be returned")
}

func TestWithToastStyles_AppliesPalette(t *testing.T) {
	// just exercise the option's body — render should still work.
	q := components.NewToasts(components.WithToastStyles(theme.DefaultStyles()))
	q.Push(components.ToastInfo, "x")
	assert.NotEmpty(t, q.View())
}
