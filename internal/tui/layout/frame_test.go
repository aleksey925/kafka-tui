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

func TestFrame_RendersBorderTitleAndBreadcrumb(t *testing.T) {
	s := theme.DefaultStyles()
	opts := layout.FrameOpts{Width: 40, Height: 5, Title: "Topics(prod)[3]", Breadcrumb: "orders"}

	out := layout.Frame(s, opts, "row 1\nrow 2\nrow 3")

	lines := strings.Split(out, "\n")
	require.Len(t, lines, 5)
	assert.Contains(t, lines[0], "Topics(prod)[3]")
	assert.Contains(t, lines[0], "orders")
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

func TestFrame_DropsBreadcrumbWhenTooNarrow(t *testing.T) {
	s := theme.DefaultStyles()
	opts := layout.FrameOpts{Width: 14, Height: 3, Title: "Title", Breadcrumb: "very-long-bc"}

	out := layout.Frame(s, opts, "")

	top := strings.Split(out, "\n")[0]
	assert.Contains(t, top, "Title")
	assert.NotContains(t, top, "very-long-bc")
}
