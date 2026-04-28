package components_test

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
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

func TestTable_NavigationGGandG(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(20))

	tbl, _ = tbl.Update(keyPressMsg("G"))
	assert.Equal(t, 19, tbl.Cursor())

	// gg returns to top after two presses
	tbl, _ = tbl.Update(keyPressMsg("g"))
	assert.Equal(t, 19, tbl.Cursor()) // first g primes only
	tbl, _ = tbl.Update(keyPressMsg("g"))
	assert.Equal(t, 0, tbl.Cursor())
}

func TestTable_NavigationCtrlDU(t *testing.T) {
	tbl := components.NewTable(simpleColumns(), components.WithHeight(10))
	tbl.SetRows(simpleRows(50))

	tbl, _ = tbl.Update(keyPressMsg("ctrl+d"))
	assert.Equal(t, 5, tbl.Cursor())

	tbl, _ = tbl.Update(keyPressMsg("ctrl+u"))
	assert.Equal(t, 0, tbl.Cursor())
}

func TestTable_NavigationClampsAtEdges(t *testing.T) {
	tbl := components.NewTable(simpleColumns())
	tbl.SetRows(simpleRows(3))

	tbl, _ = tbl.Update(keyPressMsg("k"))
	assert.Equal(t, 0, tbl.Cursor())

	tbl, _ = tbl.Update(keyPressMsg("G"))
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
	// cursor marker on the first row
	assert.Contains(t, out, ">")
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
	tbl := components.NewTable(simpleColumns(), components.WithHeight(3))
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

func keyPressMsg(name string) tea.KeyPressMsg {
	switch name {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "backspace":
		return tea.KeyPressMsg{Code: tea.KeyBackspace}
	case "tab":
		return tea.KeyPressMsg{Code: tea.KeyTab}
	case "shift+tab":
		return tea.KeyPressMsg{Code: tea.KeyTab, Mod: tea.ModShift}
	case "ctrl+a":
		return tea.KeyPressMsg{Code: 'a', Mod: tea.ModCtrl}
	case "ctrl+d":
		return tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl}
	case "ctrl+u":
		return tea.KeyPressMsg{Code: 'u', Mod: tea.ModCtrl}
	case "down":
		return tea.KeyPressMsg{Code: tea.KeyDown}
	case "up":
		return tea.KeyPressMsg{Code: tea.KeyUp}
	case "left":
		return tea.KeyPressMsg{Code: tea.KeyLeft}
	case "right":
		return tea.KeyPressMsg{Code: tea.KeyRight}
	case " ", "space":
		return tea.KeyPressMsg{Code: ' ', Text: " "}
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
