package components_test

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestConfirm_PendingByDefault(t *testing.T) {
	c := components.NewConfirm("Delete topic", "Are you sure?")
	assert.Equal(t, components.ConfirmPending, c.Result())
}

func TestConfirm_YesKey(t *testing.T) {
	c := components.NewConfirm("title", "msg")
	c, _ = c.Update(keyPressMsg("y"))
	assert.Equal(t, components.ConfirmYes, c.Result())
}

func TestConfirm_NoKey(t *testing.T) {
	c := components.NewConfirm("title", "msg")
	c, _ = c.Update(keyPressMsg("n"))
	assert.Equal(t, components.ConfirmNo, c.Result())
}

func TestConfirm_EscapeIsNo(t *testing.T) {
	c := components.NewConfirm("title", "msg")
	c, _ = c.Update(keyPressMsg("esc"))
	assert.Equal(t, components.ConfirmNo, c.Result())
}

func TestConfirm_OtherKeysIgnored(t *testing.T) {
	c := components.NewConfirm("title", "msg")
	c, _ = c.Update(keyPressRune('x'))
	assert.Equal(t, components.ConfirmPending, c.Result())
}

// Enter must NOT commit — that's the anti-accident contract. A reflexive
// enter on the modal is a common muscle-memory mistake; only an explicit
// `y` confirms (see "Confirm for destructive actions" in CLAUDE.md).
func TestConfirm_EnterIsIgnored(t *testing.T) {
	c := components.NewConfirm("title", "msg")
	c, _ = c.Update(keyPressMsg("enter"))
	assert.Equal(t, components.ConfirmPending, c.Result())
}

func TestConfirm_Reset(t *testing.T) {
	c := components.NewConfirm("title", "msg")
	c, _ = c.Update(keyPressMsg("y"))
	c.Reset()
	assert.Equal(t, components.ConfirmPending, c.Result())
}

func TestConfirm_ViewIncludesTitleMessageHints(t *testing.T) {
	c := components.NewConfirm("Delete topic", "topic-name will be removed")
	out := c.View(0, 0)
	assert.Contains(t, out, "Delete topic")
	assert.Contains(t, out, "topic-name will be removed")
	assert.Contains(t, out, "y")
	assert.Contains(t, out, "n")
	assert.Contains(t, out, "yes")
	assert.Contains(t, out, "no")
}

func TestConfirm_ViewCenteredAtWidth(t *testing.T) {
	c := components.NewConfirm("t", "m")
	out := c.View(80, 0)
	assert.NotEmpty(t, out)
}

func TestConfirm_ViewVerticallyCentered(t *testing.T) {
	c := components.NewConfirm("Delete", "msg")
	out := c.View(80, 24)
	// vertical centering expands the rendered output to the full height,
	// so the line count must match (modulo trailing newline noise).
	lines := len(strings.Split(out, "\n"))
	assert.Equal(t, 24, lines, "view must fill the full height to center the box")
	assert.Contains(t, out, "Delete")
}

func TestWithConfirmStyles_AppliesPalette(t *testing.T) {
	c := components.NewConfirm("Title", "Body", components.WithConfirmStyles(theme.DefaultStyles()))
	assert.NotEmpty(t, c.View(40, 0))
}

func TestConfirm_FieldSurfacesLabelAndValue(t *testing.T) {
	c := components.NewConfirm("Delete topic", "This cannot be undone.",
		components.WithConfirmField("Topic", "orders"))
	out := c.View(80, 0)
	assert.Contains(t, out, "Topic:")
	assert.Contains(t, out, "orders")
}

// A long context value must wrap onto its own lines (broken at the `.`/`-`
// separators) rather than stretch the box to its full length.
func TestConfirm_LongFieldValueWraps(t *testing.T) {
	c := components.NewConfirm("Delete topic", "This cannot be undone.",
		components.WithConfirmField("Topic", "payments.transactions.events-v2-eu-west-region-extra-long"))
	out := boxLines(c.View(80, 0))

	headRow, tailRow := -1, -1
	for i, ln := range out {
		if strings.Contains(ln, "payments.") {
			headRow = i
		}
		if strings.Contains(ln, "extra-long") {
			tailRow = i
		}
	}
	assert.GreaterOrEqual(t, headRow, 0)
	assert.GreaterOrEqual(t, tailRow, 0)
	assert.NotEqual(t, headRow, tailRow, "value head and tail must land on different rows")
}

// The regression guard for the centering bug: every rendered row shares the
// same visual width, so the title and hints centered inside the box can't
// drift off-center when a long value widens it.
func TestConfirm_AllRowsShareWidth(t *testing.T) {
	c := components.NewConfirm("Delete topic", "This cannot be undone.",
		components.WithConfirmField("Topic", "payments.transactions.events-v2-eu-west-region-extra-long"))
	// width 0 skips outer placement, so the rows we measure are the box
	// itself rather than placement padding equalizing every line to the
	// screen width.
	lines := boxLines(c.View(0, 0))

	want := ansi.StringWidth(lines[0])
	for i, ln := range lines {
		assert.Equal(t, want, ansi.StringWidth(ln), "row %d width mismatch", i)
	}
}

func boxLines(view string) []string {
	return strings.Split(strings.Trim(view, "\n"), "\n")
}
