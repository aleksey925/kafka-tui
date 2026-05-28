package components_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestHintLine_EmptyReturnsEmpty(t *testing.T) {
	assert.Empty(t, components.HintLine(theme.DefaultStyles()))
}

func TestHintLine_IncludesEveryKeyAndLabel(t *testing.T) {
	out := components.HintLine(theme.DefaultStyles(),
		components.Hint{Key: "y", Label: "commit"},
		components.Hint{Key: "n/esc", Label: "cancel"},
		components.Hint{Key: "j/k", Label: "scroll"},
	)
	for _, want := range []string{"y", "commit", "n/esc", "cancel", "j/k", "scroll"} {
		assert.Contains(t, out, want)
	}
}

// HintKey is rendered in bold accent (theme.go) while HintLabel is
// muted; the ANSI prefix between them confirms the styles are not
// collapsed into a single label run.
func TestHintLine_AppliesDistinctStylesPerSegment(t *testing.T) {
	styles := theme.DefaultStyles()
	out := components.HintLine(styles, components.Hint{Key: "y", Label: "commit"})

	keyOnly := styles.HintKey.Render("y")
	labelOnly := styles.HintLabel.Render("commit")
	assert.Contains(t, out, keyOnly)
	assert.Contains(t, out, labelOnly)
}

func TestHintLine_PairsSeparatedByTwoSpaces(t *testing.T) {
	styles := theme.Styles{}
	out := components.HintLine(styles,
		components.Hint{Key: "a", Label: "one"},
		components.Hint{Key: "b", Label: "two"},
	)
	// with zero-valued styles HintKey/HintLabel.Render is identity,
	// so the rendered shape is exactly "a one  b two".
	assert.Equal(t, "a one  b two", out)
	assert.Equal(t, 1, strings.Count(out, "  "), "exactly one two-space separator between pairs")
}

func TestHintLine_EmptyKeyRendersLabelOnly(t *testing.T) {
	styles := theme.Styles{}
	out := components.HintLine(styles,
		components.Hint{Label: "readline:"},
		components.Hint{Key: "ctrl+u", Label: "kill line"},
	)
	assert.Equal(t, "readline:  ctrl+u kill line", out)
}

func TestHintLine_FullyEmptyEntrySkipped(t *testing.T) {
	styles := theme.Styles{}
	out := components.HintLine(styles,
		components.Hint{Key: "a", Label: "one"},
		components.Hint{},
		components.Hint{Key: "b", Label: "two"},
	)
	assert.Equal(t, "a one  b two", out)
}
