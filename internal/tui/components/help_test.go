package components_test

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

func TestHelp_RendersTitleAndSections(t *testing.T) {
	h := components.NewHelp([]components.HelpSection{
		{Title: "Global", Keys: []layout.KeyHint{
			{Key: ":", Label: "command"},
			{Key: "/", Label: "search"},
		}},
		{Title: "Topics", Keys: []layout.KeyHint{
			{Key: "n", Label: "new"},
			{Key: "D", Label: "delete"},
		}},
	}, "v0.7.3 (a1b2c3d)")

	out := h.View(0, 0)
	assert.Contains(t, out, "Help")
	assert.Contains(t, out, "Global")
	assert.Contains(t, out, "Topics")
	assert.Contains(t, out, "command")
	assert.Contains(t, out, "search")
	assert.Contains(t, out, "new")
	assert.Contains(t, out, "delete")
	assert.Contains(t, out, "v0.7.3 (a1b2c3d)")
}

func TestHelp_VersionPlacedRightAtFixedWidth(t *testing.T) {
	h := components.NewHelp([]components.HelpSection{}, "v1")
	out := h.View(40, 0)

	lines := strings.Split(out, "\n")
	last := ansi.Strip(lines[len(lines)-1])
	// padded to width with version on the right.
	assert.True(t, strings.HasSuffix(strings.TrimRight(last, " "), "v1"),
		"expected last line to end with v1, got %q", last)
}

func TestHelp_NoVersionOmitsFooter(t *testing.T) {
	h := components.NewHelp([]components.HelpSection{
		{Title: "G", Keys: []layout.KeyHint{{Key: "?", Label: "help"}}},
	}, "")
	out := h.View(0, 0)
	assert.NotContains(t, ansi.Strip(out), "\n\nv")
}

func TestHelp_HeightAddsVerticalPadding(t *testing.T) {
	h := components.NewHelp([]components.HelpSection{
		{Title: "Global", Keys: []layout.KeyHint{{Key: ":", Label: "cmd"}}},
	}, "")
	out := h.View(0, 10)
	lines := strings.Split(out, "\n")
	assert.GreaterOrEqual(t, len(lines), 10)
}
