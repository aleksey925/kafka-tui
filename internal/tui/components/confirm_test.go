package components_test

import (
	"strings"
	"testing"

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
