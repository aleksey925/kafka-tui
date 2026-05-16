package help_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestRender_RendersTitleAndAllHints(t *testing.T) {
	// arrange
	sections := []help.Section{
		{Title: "Browse", Hints: []help.Hint{
			{Key: "enter", Label: "open"},
			{Key: "esc", Label: "back"},
		}},
		{Title: "Filtering", Hints: []help.Hint{
			{Key: "/", Label: "filter"},
		}},
	}

	// act
	out := help.Render(help.Options{
		Width:    120,
		Screen:   "Topics",
		Sections: sections,
		Footer:   "v1.2.3",
		Styles:   theme.DefaultStyles(),
	})

	// assert — every binding's key and label must appear somewhere on screen.
	for _, sec := range sections {
		assert.Contains(t, out, sec.Title)
		for _, h := range sec.Hints {
			assert.Contains(t, out, h.Key)
			assert.Contains(t, out, h.Label)
		}
	}
	assert.Contains(t, out, "Topics")
	assert.Contains(t, out, "v1.2.3")
}

func TestRender_PadsToOptionsHeight(t *testing.T) {
	// arrange
	sections := []help.Section{
		{Title: "Browse", Hints: []help.Hint{{Key: "enter", Label: "open"}}},
	}

	// act
	out := help.Render(help.Options{
		Width:    120,
		Height:   40,
		Sections: sections,
		Footer:   "v1.2.3",
		Styles:   theme.DefaultStyles(),
	})

	// assert — output spans the full requested height (footer pinned to bottom).
	assert.Equal(t, 40, strings.Count(out, "\n")+1)
}

func TestRender_DistributesLeftoverWidthAsFlexGap(t *testing.T) {
	// arrange — narrow sections so leftover width is large.
	sections := []help.Section{
		{Title: "A", Hints: []help.Hint{{Key: "x", Label: "alpha"}}},
		{Title: "B", Hints: []help.Hint{{Key: "y", Label: "bravo"}}},
	}

	// act — render at a deliberately wide terminal.
	out := help.Render(help.Options{
		Width:    160,
		Sections: sections,
		Styles:   theme.DefaultStyles(),
	})

	// assert — between "alpha" and "<y>" there must be many spaces (flex gap),
	// far more than the 4-column minimum, proving the gap stretched.
	idxA := strings.Index(out, "alpha")
	idxB := strings.Index(out, "<y>")
	require.True(t, idxA >= 0 && idxB > idxA)
	assert.Greater(t, idxB-idxA-len("alpha"), 20)
}

func TestRender_FallsBackToSingleColumnAtSmallWidths(t *testing.T) {
	// arrange — long labels force multi-column layout to collapse.
	sections := []help.Section{
		{Title: "A", Hints: []help.Hint{{Key: "x", Label: strings.Repeat("y", 80)}}},
		{Title: "B", Hints: []help.Hint{{Key: "z", Label: strings.Repeat("w", 80)}}},
	}

	// act
	out := help.Render(help.Options{
		Width:    50,
		Sections: sections,
		Styles:   theme.DefaultStyles(),
	})

	// assert — both sections still appear (no truncation).
	assert.Contains(t, out, "A")
	assert.Contains(t, out, "B")
}

func TestSectionsFromBindings_GroupsByCategoryPreservingOrder(t *testing.T) {
	// arrange
	bs := []keymap.Binding{
		{Keys: []string{"a"}, Label: "alpha", Category: "First"},
		{Keys: []string{"b"}, Label: "bravo", Category: "Second"},
		{Keys: []string{"c"}, Label: "charlie", Category: "First"},
		{Keys: []string{"x"}, Label: "hidden"},
	}

	// act
	got := help.SectionsFromBindings(bs)

	// assert
	require.Len(t, got, 2)
	assert.Equal(t, "First", got[0].Title)
	assert.Equal(t, "Second", got[1].Title)
	assert.Equal(t, "alpha", got[0].Hints[0].Label)
	assert.Equal(t, "charlie", got[0].Hints[1].Label)
}

func TestGeneralSections_ContainsCoreShortcuts(t *testing.T) {
	// arrange + act
	got := help.GeneralSections()

	// assert
	require.NotEmpty(t, got)
	titles := make([]string, 0, len(got))
	for _, s := range got {
		titles = append(titles, s.Title)
	}
	for _, want := range []string{"General", "Commands", "Navigation"} {
		assert.Contains(t, titles, want)
	}
}
