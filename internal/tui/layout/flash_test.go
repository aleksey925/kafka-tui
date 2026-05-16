package layout_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestFlashLine_EmptyTextPadsToWidth(t *testing.T) {
	s := theme.DefaultStyles()

	out := layout.FlashLine(s, layout.Flash{}, 40)

	// blank flash must reserve a full-width line so the screen geometry
	// doesn't shift when a toast appears or expires.
	assert.Equal(t, 40, lipgloss.Width(out))
	assert.Equal(t, strings.Repeat(" ", 40), out)
}

func TestFlashLine_RendersTagAndText(t *testing.T) {
	s := theme.DefaultStyles()

	out := layout.FlashLine(s, layout.Flash{Text: "topic created", Level: layout.FlashOK}, 60)

	assert.Contains(t, out, "[OK]")
	assert.Contains(t, out, "topic created")
	assert.Equal(t, 60, lipgloss.Width(out))
}

func TestFlashLine_ZeroWidthKeepsBareBody(t *testing.T) {
	s := theme.DefaultStyles()

	out := layout.FlashLine(s, layout.Flash{Text: "boom", Level: layout.FlashErr}, 0)

	assert.Contains(t, out, "[ERR]")
	assert.Contains(t, out, "boom")
}
