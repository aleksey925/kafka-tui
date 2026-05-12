package layout_test

import (
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestFrame_RendersBorderAndCenteredTitle(t *testing.T) {
	s := theme.DefaultStyles()
	opts := layout.FrameOpts{Width: 40, Height: 5, Title: "Topics(prod)[3]"}

	out := layout.Frame(s, opts, "row 1\nrow 2\nrow 3")

	lines := strings.Split(out, "\n")
	require.Len(t, lines, 5)
	assert.Contains(t, lines[0], "Topics(prod)[3]")
	assert.Contains(t, lines[0], "╭")
	assert.Contains(t, lines[0], "╮")
	assert.Contains(t, lines[4], "╰")
	assert.Contains(t, lines[4], "╯")
	for _, l := range lines {
		assert.Equal(t, 40, lipgloss.Width(l), "every line must equal frame width")
	}
}

func TestFrame_PadsShortBody(t *testing.T) {
	s := theme.DefaultStyles()
	opts := layout.FrameOpts{Width: 30, Height: 6, Title: "T"}

	out := layout.Frame(s, opts, "only one line")

	lines := strings.Split(out, "\n")
	require.Len(t, lines, 6)
	// 4 body lines (height 6 - top - bottom) — first carries content,
	// the rest are blank-padded between │ │.
	for _, l := range lines {
		assert.Equal(t, 30, lipgloss.Width(l))
	}
}

func TestFrame_DropsTitleWhenTooNarrow(t *testing.T) {
	s := theme.DefaultStyles()
	opts := layout.FrameOpts{Width: 8, Height: 3, Title: "very-long-title"}

	out := layout.Frame(s, opts, "")

	top := strings.Split(out, "\n")[0]
	assert.NotContains(t, top, "very-long-title")
}

// Regression: a body row wider than the frame used to pass through unchanged,
// shifting the right border off-axis. padOrTruncate now routes through the
// shared TruncateText so overflowing content lands within the border and the
// frame stays a rectangle.
func TestFrame_TruncatesBodyRowWiderThanFrame(t *testing.T) {
	s := theme.DefaultStyles()
	opts := layout.FrameOpts{Width: 12, Height: 3}

	// body row is 50 cells; inner area is 10 cells (frame width minus borders).
	out := layout.Frame(s, opts, strings.Repeat("x", 50))

	lines := strings.Split(out, "\n")
	require.Len(t, lines, 3)
	for _, l := range lines {
		assert.Equal(t, 12, lipgloss.Width(l), "every frame line stays exactly the frame width")
	}
	assert.Contains(t, lines[1], "…", "overflowing body must end in the canonical ellipsis")
}
