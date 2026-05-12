package components_test

import (
	"testing"

	"github.com/charmbracelet/x/ansi"
	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
)

func TestTruncateText_ZeroOrNegativeWidthReturnsEmpty(t *testing.T) {
	assert.Empty(t, components.TruncateText("hello", 0))
	assert.Empty(t, components.TruncateText("hello", -5))
}

func TestTruncateText_FittingContentUnchanged(t *testing.T) {
	assert.Equal(t, "hi", components.TruncateText("hi", 5))
	assert.Equal(t, "exact", components.TruncateText("exact", 5))
}

func TestTruncateText_OverflowsGetEllipsis(t *testing.T) {
	got := components.TruncateText("abcdefghij", 5)
	// final result must fit in the budget; the helper picks the cut point.
	assert.Equal(t, 5, ansi.StringWidth(got))
	// the ellipsis sits at the end so the reader knows content was dropped.
	assert.Contains(t, got, "…")
}

func TestTruncateText_WidthOfOneIsJustEllipsis(t *testing.T) {
	got := components.TruncateText("anything", 1)
	assert.Equal(t, "…", got)
}

func TestTruncateText_PreservesANSIStyling(t *testing.T) {
	// red "VERY LONG ERROR" — styling must survive truncation, and only the
	// content bytes are counted toward the budget (not the escape sequences).
	styled := "\x1b[31mVERY LONG ERROR\x1b[0m"
	got := components.TruncateText(styled, 8)
	assert.Equal(t, 8, ansi.StringWidth(got), "styled truncation must respect the visual budget, not byte length")
	assert.Contains(t, got, "\x1b[31m", "the leading style escape must survive")
}

func TestTruncateText_DoubleWidthRunesCountedByCells(t *testing.T) {
	// "漢字" is 4 cells (each CJK rune is wide). A budget of 3 must truncate,
	// not pass through as if it were rune-count 2.
	got := components.TruncateText("漢字漢字", 4)
	assert.LessOrEqual(t, ansi.StringWidth(got), 4)
}

func TestTruncateText_EmptyInputReturnsEmpty(t *testing.T) {
	assert.Empty(t, components.TruncateText("", 10))
}
