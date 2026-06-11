package components

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenizeIdentifier_KeepsSeparatorsAttached(t *testing.T) {
	got := tokenizeIdentifier("events-v2.eu_west")
	assert.Equal(t, []string{"events-", "v2.", "eu_", "west"}, got)
}

func TestWrapIdentifier_BreaksAtSeparators(t *testing.T) {
	got := wrapIdentifier("payments.transactions.events-v2", 20)
	assert.Equal(t, []string{"payments.", "transactions.events-", "v2"}, got)
}

func TestWrapIdentifier_HardBreaksOversizedSegment(t *testing.T) {
	got := wrapIdentifier("aaaaaaaaaaaaaaaaaaaaaaaaa", 10)
	assert.Equal(t, []string{"aaaaaaaaaa", "aaaaaaaaaa", "aaaaa"}, got)
}

func TestWrapIdentifier_NoWrapWhenItFits(t *testing.T) {
	got := wrapIdentifier("orders", 20)
	assert.Equal(t, []string{"orders"}, got)
}

func TestModalContentWidthFor_ProportionalCapWraps(t *testing.T) {
	fields := []modalField{{Label: "Topic", Value: "payments.transactions.events-v2-eu-west-region"}}

	narrow := modalContentWidthFor("Delete topic", fields, "This cannot be undone.", "y yes  n no", 80)
	wide := modalContentWidthFor("Delete topic", fields, "This cannot be undone.", "y yes  n no", 160)

	// the terminal-proportional ceiling clamps the narrow case below the
	// value's natural inline width (forcing a wrap) while the wide terminal
	// leaves room for the value inline.
	assert.Equal(t, 48, narrow)
	assert.Equal(t, 54, wide)
}

func TestModalContentWidthFor_NeverBelowFloor(t *testing.T) {
	got := modalContentWidthFor("ok", []modalField{{Label: "Topic", Value: "x"}}, "", "y yes  n no", 200)
	assert.Equal(t, modalContentWidth, got)
}

func TestPlaceVerticalBiased_FillsHeightAndLiftsUp(t *testing.T) {
	content := "a\nb\nc" // 3 rows
	out := placeVerticalBiased(content, 13, modalTopBias)

	lines := strings.Split(out, "\n")
	assert.Len(t, lines, 13, "output must fill the full height")

	// slack is 10, a true center puts 5 blank rows above; the bias lifts the
	// block up by modalTopBias rows.
	above := 0
	for _, ln := range lines {
		if ln != "" {
			break
		}
		above++
	}
	assert.Equal(t, 5-modalTopBias, above)
}

func TestPlaceVerticalBiased_ClampsWhenNoSlack(t *testing.T) {
	content := "a\nb\nc"
	assert.Equal(t, content, placeVerticalBiased(content, 2, modalTopBias))
}
