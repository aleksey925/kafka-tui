package layout_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

func TestPlaceCenteredTop_ZeroWidthReturnsPopupUnchanged(t *testing.T) {
	// arrange
	popup := "hello"

	// act
	out := layout.PlaceCenteredTop(0, 10, popup)

	// assert
	assert.Equal(t, popup, out)
}

func TestPlaceCenteredTop_NegativeWidthReturnsPopupUnchanged(t *testing.T) {
	// arrange / act
	out := layout.PlaceCenteredTop(-1, 10, "hi")

	// assert
	assert.Equal(t, "hi", out)
}

func TestPlaceCenteredTop_ZeroHeightSkipsVerticalPadding(t *testing.T) {
	// arrange / act
	out := layout.PlaceCenteredTop(20, 0, "hi")

	// assert — horizontally centered, no extra newlines for vertical fill.
	assert.Equal(t, 1, strings.Count(out, "\n")+1, "expected single line, got %q", out)
	assert.Contains(t, out, "hi")
}

func TestPlaceCenteredTop_PadsVerticalToBodyHeight(t *testing.T) {
	// arrange / act
	out := layout.PlaceCenteredTop(20, 5, "hi")

	// assert — popup is one line, body is 5 rows: expect 4 trailing
	// blank/padded lines so the total height matches.
	lines := strings.Split(out, "\n")
	assert.Len(t, lines, 5)
	assert.Contains(t, lines[0], "hi")
}
