package components_test

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestTable_NavigationJK(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(5))

	tbl, _ = tbl.Update(keyPressMsg("j"))
	tbl, _ = tbl.Update(keyPressMsg("j"))
	assert.Equal(t, 2, tbl.Cursor())

	tbl, _ = tbl.Update(keyPressMsg("k"))
	assert.Equal(t, 1, tbl.Cursor())
}

func TestTable_NavigationArrows(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(3))

	tbl, _ = tbl.Update(keyPressMsg("down"))
	tbl, _ = tbl.Update(keyPressMsg("down"))
	assert.Equal(t, 2, tbl.Cursor())

	tbl, _ = tbl.Update(keyPressMsg("up"))
	assert.Equal(t, 1, tbl.Cursor())
}

func TestTable_NavigationHomeEnd(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(20))

	tbl, _ = tbl.Update(keyPressMsg("end"))
	assert.Equal(t, 19, tbl.Cursor())

	tbl, _ = tbl.Update(keyPressMsg("home"))
	assert.Equal(t, 0, tbl.Cursor())
}

func TestTable_NavigationPaging(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetHeight(10)
	tbl.SetRows(simpleRows(50))

	tbl, _ = tbl.Update(keyPressMsg("ctrl+f"))
	assert.Equal(t, 9, tbl.Cursor())

	tbl, _ = tbl.Update(keyPressMsg("ctrl+b"))
	assert.Equal(t, 0, tbl.Cursor())

	tbl, _ = tbl.Update(keyPressMsg("pgdown"))
	assert.Equal(t, 9, tbl.Cursor())

	tbl, _ = tbl.Update(keyPressMsg("pgup"))
	assert.Equal(t, 0, tbl.Cursor())
}

func TestTable_NavigationClampsAtEdges(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(3))

	tbl, _ = tbl.Update(keyPressMsg("k"))
	assert.Equal(t, 0, tbl.Cursor())

	tbl, _ = tbl.Update(keyPressMsg("end"))
	tbl, _ = tbl.Update(keyPressMsg("j"))
	assert.Equal(t, 2, tbl.Cursor())
}

func TestTable_FuzzySearchFiltersAndCountsMatches(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows([]components.Row{
		{ID: "1", Values: []string{"orders", "active"}},
		{ID: "2", Values: []string{"events", "active"}},
		{ID: "3", Values: []string{"order-history", "stale"}},
		{ID: "4", Values: []string{"users", "active"}},
	})

	tbl, _ = tbl.Update(keyPressMsg("/"))
	require.True(t, tbl.SearchActive())
	for _, ch := range "ord" {
		tbl, _ = tbl.Update(keyPressRune(ch))
	}
	tbl, _ = tbl.Update(keyPressMsg("enter"))

	assert.False(t, tbl.SearchActive())
	assert.Equal(t, "ord", tbl.Search())

	// only `orders` and `order-history` match → cursor still on first.
	row, ok := tbl.SelectedRow()
	require.True(t, ok)
	assert.Equal(t, "1", row.ID)

	// jump to next match
	tbl, _ = tbl.Update(keyPressMsg("n"))
	row, _ = tbl.SelectedRow()
	assert.Equal(t, "3", row.ID)

	// wrap around
	tbl, _ = tbl.Update(keyPressMsg("n"))
	row, _ = tbl.SelectedRow()
	assert.Equal(t, "1", row.ID)

	// previous goes back
	tbl, _ = tbl.Update(keyPressMsg("N"))
	row, _ = tbl.SelectedRow()
	assert.Equal(t, "3", row.ID)
}

func TestTable_SearchEscapeClears(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(4))

	tbl, _ = tbl.Update(keyPressMsg("/"))
	tbl, _ = tbl.Update(keyPressRune('a'))
	tbl, _ = tbl.Update(keyPressMsg("esc"))

	assert.False(t, tbl.SearchActive())
	assert.Empty(t, tbl.Search())
	assert.Len(t, tbl.Rows(), 4)
}

func TestTable_SortCycleAscDescNone(t *testing.T) {
	tbl := components.NewTable([]components.Column{
		{Title: "name", Sortable: true},
		{Title: "value", Sortable: true},
	})
	tbl.SetRows([]components.Row{
		{ID: "1", Values: []string{"banana", "1"}},
		{ID: "2", Values: []string{"apple", "2"}},
		{ID: "3", Values: []string{"cherry", "3"}},
	})

	// s → ascending on name (alphabetical)
	tbl, _ = tbl.Update(keyPressMsg("s"))
	col, dir := tbl.Sort()
	assert.Equal(t, 0, col)
	assert.Equal(t, components.SortAsc, dir)
	row, _ := tbl.SelectedRow()
	assert.Equal(t, "2", row.ID) // apple

	// s → descending
	tbl, _ = tbl.Update(keyPressMsg("s"))
	_, dir = tbl.Sort()
	assert.Equal(t, components.SortDesc, dir)
	row, _ = tbl.SelectedRow()
	assert.Equal(t, "3", row.ID) // cherry

	// s → cleared
	tbl, _ = tbl.Update(keyPressMsg("s"))
	col, dir = tbl.Sort()
	assert.Equal(t, -1, col)
	assert.Equal(t, components.SortNone, dir)
}

func TestTable_SortCapitalSwitchesColumn(t *testing.T) {
	tbl := components.NewTable([]components.Column{
		{Title: "name", Sortable: true},
		{Title: "value", Sortable: true},
	})
	tbl.SetRows([]components.Row{
		{ID: "1", Values: []string{"banana", "3"}},
		{ID: "2", Values: []string{"apple", "1"}},
		{ID: "3", Values: []string{"cherry", "2"}},
	})

	// S sorts column 0 first time
	tbl, _ = tbl.Update(keyPressMsg("S"))
	col, _ := tbl.Sort()
	assert.Equal(t, 0, col)

	// S advances to next sortable column
	tbl, _ = tbl.Update(keyPressMsg("S"))
	col, dir := tbl.Sort()
	assert.Equal(t, 1, col)
	assert.Equal(t, components.SortAsc, dir)

	row, _ := tbl.SelectedRow()
	assert.Equal(t, "2", row.ID)
}

func TestTable_SortSkipsNonSortable(t *testing.T) {
	tbl := components.NewTable([]components.Column{
		{Title: "name"},
		{Title: "value", Sortable: true},
	})
	tbl.SetRows(simpleRows(3))

	tbl, _ = tbl.Update(keyPressMsg("s"))
	col, _ := tbl.Sort()
	assert.Equal(t, 1, col)
}

func TestTable_MultiSelectToggle(t *testing.T) {
	tbl := components.NewTable(simpleColumns(), components.WithSelectable(true))
	tbl.SetRows(simpleRows(3))

	tbl, _ = tbl.Update(keyPressMsg(" "))
	tbl, _ = tbl.Update(keyPressMsg("j"))
	tbl, _ = tbl.Update(keyPressMsg(" "))

	assert.Equal(t, []string{"row-0", "row-1"}, tbl.SelectedIDs())
	assert.True(t, tbl.IsSelected("row-0"))
	assert.True(t, tbl.IsSelected("row-1"))
	assert.False(t, tbl.IsSelected("row-2"))

	// toggle off
	tbl, _ = tbl.Update(keyPressMsg("k"))
	tbl, _ = tbl.Update(keyPressMsg(" "))
	assert.False(t, tbl.IsSelected("row-0"))
	assert.Equal(t, []string{"row-1"}, tbl.SelectedIDs())

	tbl.ClearSelection()
	assert.Empty(t, tbl.SelectedIDs())
}

func TestTable_MultiSelectIgnoredWhenDisabled(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(2))
	tbl, _ = tbl.Update(keyPressMsg(" "))
	assert.Empty(t, tbl.SelectedIDs())
}

func TestTable_ViewRendersHeaderAndRows(t *testing.T) {
	tbl := components.NewTable([]components.Column{
		{Title: "name", Width: 10, Sortable: true},
		{Title: "state", Width: 8},
	})
	tbl.SetRows([]components.Row{
		{ID: "a", Values: []string{"alpha", "ok"}},
		{ID: "b", Values: []string{"beta", "fail"}},
	})

	out := tbl.View()
	assert.Contains(t, out, "name")
	assert.Contains(t, out, "state")
	assert.Contains(t, out, "alpha")
	assert.Contains(t, out, "beta")
	// cursor row uses the inverted-bg Cursor style — the ANSI sequence for
	// the accent background must appear somewhere in the output.
	assert.Contains(t, out, "\x1b[")
}

func TestTable_ViewEmpty(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	out := tbl.View()
	assert.Contains(t, out, "(no rows)")
}

func TestTable_ViewShowsSortIndicator(t *testing.T) {
	tbl := components.NewTable([]components.Column{{Title: "name", Sortable: true}})
	tbl.SetRows(simpleRows(2))

	tbl, _ = tbl.Update(keyPressMsg("s"))
	out := tbl.View()
	assert.Contains(t, out, "↑")
	assert.Contains(t, out, "sort: name asc")
}

func TestTable_ViewportScrollsWithCursor(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetHeight(3)
	tbl.SetRows(simpleRows(20))

	for range 5 {
		tbl, _ = tbl.Update(keyPressMsg("j"))
	}
	out := tbl.View()
	// cursor at row 5 means viewport contains row-3..row-5
	assert.Contains(t, out, "row-5")
	assert.NotContains(t, out, "row-0")
}

func TestTable_SetSortProgrammatic(t *testing.T) {
	tbl := components.NewTable([]components.Column{
		{Title: "name", Sortable: true},
	})
	tbl.SetRows([]components.Row{
		{ID: "1", Values: []string{"b"}},
		{ID: "2", Values: []string{"a"}},
	})
	tbl.SetSort(0, components.SortDesc)
	row, _ := tbl.SelectedRow()
	assert.Equal(t, "1", row.ID)
}

func TestTable_SearchSurvivesRowReplacement(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows([]components.Row{
		{ID: "a", Values: []string{"orders", "ok"}},
		{ID: "b", Values: []string{"events", "ok"}},
	})
	tbl, _ = tbl.Update(keyPressMsg("/"))
	for _, ch := range "ord" {
		tbl, _ = tbl.Update(keyPressRune(ch))
	}

	tbl.SetRows([]components.Row{
		{ID: "a", Values: []string{"orders", "ok"}},
		{ID: "c", Values: []string{"order-history", "ok"}},
	})
	tbl, _ = tbl.Update(keyPressMsg("enter"))

	assert.Equal(t, "ord", tbl.Search())
	row, _ := tbl.SelectedRow()
	assert.Equal(t, "a", row.ID)
}

func TestTable_GoToID_Found(t *testing.T) {
	// arrange
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows([]components.Row{
		{ID: "a", Values: []string{"alpha", "1"}},
		{ID: "b", Values: []string{"beta", "2"}},
		{ID: "c", Values: []string{"gamma", "3"}},
	})

	// act
	found := tbl.GoToID("c")

	// assert
	assert.True(t, found)
	assert.Equal(t, 2, tbl.Cursor())
	row, ok := tbl.SelectedRow()
	require.True(t, ok)
	assert.Equal(t, "c", row.ID)
}

func TestTable_GoToID_NotFound(t *testing.T) {
	// arrange
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows([]components.Row{
		{ID: "a", Values: []string{"alpha", "1"}},
		{ID: "b", Values: []string{"beta", "2"}},
	})
	tbl.GoToID("b")

	// act
	found := tbl.GoToID("nonexistent")

	// assert
	assert.False(t, found)
	assert.Equal(t, 1, tbl.Cursor())
}

// ----- helpers -----

func simpleColumns() []components.Column {
	return []components.Column{
		{Title: "name", Width: 10},
		{Title: "value", Width: 6},
	}
}

func simpleRows(n int) []components.Row {
	out := make([]components.Row, n)
	for i := range n {
		out[i] = components.Row{
			ID:     "row-" + itoa(i),
			Values: []string{"row-" + itoa(i), "v-" + itoa(i)},
		}
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var b strings.Builder
	for ; i > 0; i /= 10 {
		b.WriteByte(byte('0' + i%10))
	}
	out := reverse(b.String())
	if neg {
		return "-" + out
	}
	return out
}

func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

var keyPressTable = map[string]tea.KeyPressMsg{
	"enter":         {Code: tea.KeyEnter},
	"esc":           {Code: tea.KeyEscape},
	"backspace":     {Code: tea.KeyBackspace},
	"tab":           {Code: tea.KeyTab},
	"shift+tab":     {Code: tea.KeyTab, Mod: tea.ModShift},
	"ctrl+a":        {Code: 'a', Mod: tea.ModCtrl},
	"ctrl+b":        {Code: 'b', Mod: tea.ModCtrl},
	"ctrl+d":        {Code: 'd', Mod: tea.ModCtrl},
	"ctrl+e":        {Code: 'e', Mod: tea.ModCtrl},
	"ctrl+f":        {Code: 'f', Mod: tea.ModCtrl},
	"ctrl+k":        {Code: 'k', Mod: tea.ModCtrl},
	"ctrl+u":        {Code: 'u', Mod: tea.ModCtrl},
	"ctrl+w":        {Code: 'w', Mod: tea.ModCtrl},
	"alt+b":         {Code: 'b', Mod: tea.ModAlt},
	"alt+f":         {Code: 'f', Mod: tea.ModAlt},
	"alt+backspace": {Code: tea.KeyBackspace, Mod: tea.ModAlt},
	"pgup":          {Code: tea.KeyPgUp},
	"pgdown":        {Code: tea.KeyPgDown},
	"down":          {Code: tea.KeyDown},
	"up":            {Code: tea.KeyUp},
	"left":          {Code: tea.KeyLeft},
	"right":         {Code: tea.KeyRight},
	"home":          {Code: tea.KeyHome},
	"end":           {Code: tea.KeyEnd},
	"delete":        {Code: tea.KeyDelete},
	" ":             {Code: ' ', Text: " "},
	"space":         {Code: ' ', Text: " "},
}

func keyPressMsg(name string) tea.KeyPressMsg {
	if msg, ok := keyPressTable[name]; ok {
		return msg
	}
	if len(name) == 1 {
		r := rune(name[0])
		return tea.KeyPressMsg{Code: r, Text: string(r)}
	}
	return tea.KeyPressMsg{}
}

func keyPressRune(r rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: r, Text: string(r)}
}

func TestTable_FilteredCount_AndTotalCount(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(5))

	assert.Equal(t, 5, tbl.TotalCount())
	assert.Equal(t, 5, tbl.FilteredCount())

	tbl.SetSearch("row-2")
	assert.Equal(t, 5, tbl.TotalCount(), "TotalCount must ignore filter")
	assert.Equal(t, 1, tbl.FilteredCount())
}

func TestTable_SetSearch_LiveFiltersAndClears(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(4))

	tbl.SetSearch("row-3")
	row, ok := tbl.SelectedRow()
	require.True(t, ok)
	assert.Equal(t, "row-3", row.ID)

	tbl.SetSearch("")
	assert.Equal(t, 4, tbl.FilteredCount(), "empty query restores full view")
}

func TestTable_SetHeight_AndSetTotalWidth_DontPanic(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(20))

	tbl.SetHeight(5)
	tbl.SetTotalWidth(80)

	assert.Contains(t, tbl.View(), "row-0")
}

func TestTable_TruncateCellsLongerThanColumnWidth(t *testing.T) {
	cols := []components.Column{{Title: "name", Width: 6}}
	tbl := components.NewTable(cols)
	tbl.SetRows([]components.Row{
		{ID: "x", Values: []string{"verylongvalue"}},
	})

	out := tbl.View()
	// truncated cell ends with the ellipsis rune.
	assert.Contains(t, out, "…")
}

// Regression: a styled cell that overflowed used to bail out of truncation
// (the legacy code refused to cut content containing ANSI escapes), letting
// the row shift the entire table to the right. The shared TruncateText
// helper is ANSI-aware, so styled cells now truncate just like plain ones.
func TestTable_TruncatesStyledCells(t *testing.T) {
	cols := []components.Column{{Title: "name", Width: 8}}
	tbl := components.NewTable(cols)
	tbl.SetRows([]components.Row{
		{ID: "x", Values: []string{"\x1b[31mvery-long-styled-value\x1b[0m"}},
	})

	out := tbl.View()
	assert.Contains(t, out, "…", "styled cells must truncate, not pass through")
}

// Regression: wide runes (CJK / emoji) can't be split by the column boundary.
// Without the pad-after-truncate guard, TruncateText returns one cell short
// when a wide rune doesn't fit the post-ellipsis budget, leaving the next
// column visually shifted left.
func TestTable_AlignsTruncatedWideCharCells(t *testing.T) {
	// two narrow columns: the first holds wide-char overflow, the second
	// holds a sentinel we can position-check.
	cols := []components.Column{
		{Title: "a", Width: 4},
		{Title: "b", Width: 3},
	}
	tbl := components.NewTable(cols)
	tbl.SetRows([]components.Row{
		{ID: "x", Values: []string{"漢字漢字", "OK!"}},
	})

	out := tbl.View()
	lines := strings.Split(out, "\n")
	require.GreaterOrEqual(t, len(lines), 2, "expected at least header + 1 row")
	// gutter (2) + col-a (4) + separator (2) + col-b (3) = 11 cells.
	const expected = 11
	assert.Equal(t, expected, ansi.StringWidth(lines[1]),
		"wide-char overflow in col-a must pad to the full column width so col-b stays aligned")
}

func TestTable_WithStyles_ConstructsWithoutPanic(t *testing.T) {
	tbl := components.NewTable(simpleColumns(), components.WithStyles(theme.DefaultStyles()))
	tbl.SetRows(simpleRows(2))
	assert.NotEmpty(t, tbl.View())
}

// Regression: when sorting is active AND at least one column is flex, the
// 2 cells reserved for the sort arrow used to be excluded from fixedTotal.
// Flex distribution then over-allocated leftover by 2, and the rendered
// row was 2 cells wider than SetTotalWidth.
func TestTable_SortArrowDoesNotBreakFlexWidth(t *testing.T) {
	const totalWidth = 60
	cols := []components.Column{
		{Title: "name", Flex: true, Sortable: true},
		{Title: "state", Width: 8},
	}
	tbl := components.NewTable(cols)
	tbl.SetRows([]components.Row{
		{ID: "a", Values: []string{"alpha", "ok"}},
		{ID: "b", Values: []string{"beta", "fail"}},
	})
	tbl.SetTotalWidth(totalWidth)

	// arrange: turn sort on (cycles None → Asc).
	tbl, _ = tbl.Update(keyPressMsg("s"))

	// act
	out := tbl.View()
	lines := strings.Split(out, "\n")

	// assert
	require.GreaterOrEqual(t, len(lines), 2, "expected header + at least one row")
	assert.LessOrEqual(t, ansi.StringWidth(lines[0]), totalWidth,
		"header must not exceed SetTotalWidth when sort is active")
	assert.LessOrEqual(t, ansi.StringWidth(lines[1]), totalWidth,
		"row must not exceed SetTotalWidth when sort is active")
}
