// Package components provides reusable Bubble Tea building blocks shared by
// every screen: the navigable Table, modal dialogs (Confirm, Toast, Help),
// and the input Form.
//
// Each component is a pure value type with explicit Update/View methods and a
// small public API. Components do not own terminal I/O; screens compose them
// and route key messages.
package components

import (
	"fmt"
	"sort"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Column declares a Table column.
type Column struct {
	Title string
	// Width is the rendered width in characters. 0 means "auto" (use the
	// max of title and longest value).
	Width int
	// Align controls horizontal alignment of cell content. Defaults to left.
	Align lipgloss.Position
	// Sortable reports whether the column participates in the `s`/`S` sort
	// rotation. Non-sortable columns are skipped.
	Sortable bool
}

// Row holds the displayed values of one row, plus a stable ID supplied by the
// caller (for callbacks and multi-select tracking).
type Row struct {
	ID     string
	Values []string
}

// SortDirection indicates ascending / descending / off.
type SortDirection int

const (
	// SortNone leaves rows in caller-supplied order.
	SortNone SortDirection = iota
	// SortAsc sorts ascending on the active column.
	SortAsc
	// SortDesc sorts descending on the active column.
	SortDesc
)

// Table is a navigable, searchable, sortable, selectable list of rows.
//
// It owns *only* the view/selection/sort state; callers feed it data with
// SetRows. Multi-select is opt-in (Selectable=true).
type Table struct {
	columns []Column
	rows    []Row // original rows as supplied by caller
	view    []int // indices into rows after filter+sort

	cursor   int
	viewport int // index in `view` that begins the visible window
	height   int // visible rows (0 = fit-all, no scrolling)

	search       string
	searchActive bool
	matches      []int // indices into `view` that contain matches
	matchCursor  int

	sortCol int
	sortDir SortDirection

	selectable bool
	selected   map[string]struct{}

	gPrimed bool // first `g` of the `gg` jump-to-top sequence

	styles theme.Styles
}

// TableOption configures a Table at construction.
type TableOption func(*Table)

// WithSelectable enables Space-to-multi-select.
func WithSelectable(on bool) TableOption {
	return func(t *Table) { t.selectable = on }
}

// WithHeight sets the visible body height (rows). 0 means "fit all".
func WithHeight(rows int) TableOption {
	return func(t *Table) { t.height = rows }
}

// WithStyles overrides the theme styles (mostly for tests).
func WithStyles(s theme.Styles) TableOption {
	return func(t *Table) { t.styles = s }
}

// NewTable constructs a Table with the given columns.
func NewTable(cols []Column, opts ...TableOption) *Table {
	t := &Table{
		columns:  append([]Column(nil), cols...),
		styles:   theme.DefaultStyles(),
		selected: make(map[string]struct{}),
		sortCol:  -1,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// SetRows replaces the rows. The cursor is clamped, the view is rebuilt
// (filter + sort reapplied), and current selection (by ID) is preserved.
func (t *Table) SetRows(rows []Row) {
	t.rows = append([]Row(nil), rows...)
	t.rebuildView()
}

// Rows returns the underlying row slice (defensive copy).
func (t *Table) Rows() []Row {
	out := make([]Row, len(t.rows))
	copy(out, t.rows)
	return out
}

// Cursor returns the current row index in the *view* (post filter/sort).
func (t *Table) Cursor() int { return t.cursor }

// GoToID moves the cursor to the row with the given ID. Returns true if found.
func (t *Table) GoToID(id string) bool {
	for i, idx := range t.view {
		if t.rows[idx].ID == id {
			t.cursor = i
			return true
		}
	}
	return false
}

// SelectedRow returns the row currently under the cursor, or false if the
// view is empty.
func (t *Table) SelectedRow() (Row, bool) {
	if len(t.view) == 0 {
		return Row{}, false
	}
	return t.rows[t.view[t.cursor]], true
}

// SelectedIDs returns the IDs of all multi-selected rows in stable order.
func (t *Table) SelectedIDs() []string {
	ids := make([]string, 0, len(t.selected))
	for _, r := range t.rows {
		if _, ok := t.selected[r.ID]; ok {
			ids = append(ids, r.ID)
		}
	}
	return ids
}

// IsSelected reports whether a row ID is currently multi-selected.
func (t *Table) IsSelected(id string) bool {
	_, ok := t.selected[id]
	return ok
}

// ClearSelection deselects every row.
func (t *Table) ClearSelection() { t.selected = make(map[string]struct{}) }

// Sort returns the active sort column index (-1 if none) and direction.
func (t *Table) Sort() (int, SortDirection) { return t.sortCol, t.sortDir }

// SetSort applies a sort programmatically. Pass -1 / SortNone to clear. The
// cursor is reset to the top so the new ordering is visible.
func (t *Table) SetSort(col int, dir SortDirection) {
	t.sortCol, t.sortDir = col, dir
	t.rebuildView()
	t.cursor = 0
	t.viewport = 0
}

// Search returns the current search query (empty if no search active).
func (t *Table) Search() string { return t.search }

// SearchActive reports whether the `/` search prompt is open.
func (t *Table) SearchActive() bool { return t.searchActive }

// SetHeight changes the visible body height. 0 disables scrolling.
func (t *Table) SetHeight(rows int) {
	t.height = rows
	t.clampViewport()
}

// Update routes a key message into the table. It returns the (possibly
// updated) table and a command (always nil today; reserved for future).
//
// Hotkeys handled:
//
//	j / down     — cursor down
//	k / up       — cursor up
//	ctrl+d       — half-page down
//	ctrl+u       — half-page up
//	g g          — top
//	G            — bottom
//	/            — open search
//	enter / esc  — close search prompt (keep / clear filter)
//	n / N        — next / previous match
//	s / S        — cycle sort on current column (asc → desc → none)
//	space        — toggle multi-select on current row (if selectable)
//
// Unrecognized keys are ignored so screens can layer their own.
func (t *Table) Update(msg tea.Msg) (*Table, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return t, nil
	}
	if t.searchActive {
		t.handleSearchKey(key)
		return t, nil
	}
	t.handleNormalKey(key)
	return t, nil
}

func (t *Table) handleNormalKey(key tea.KeyPressMsg) {
	switch key.String() {
	case "j", "down":
		t.move(+1)
	case "k", "up":
		t.move(-1)
	case "ctrl+d":
		t.move(+t.pageStep())
	case "ctrl+u":
		t.move(-t.pageStep())
	case "G":
		t.cursor = max(0, len(t.view)-1)
		t.clampViewport()
		t.gPrimed = false
	case "g":
		if t.gPrimed {
			t.cursor = 0
			t.clampViewport()
			t.gPrimed = false
		} else {
			t.gPrimed = true
		}
		return
	case "/":
		t.searchActive = true
		t.search = ""
	case "n":
		t.jumpMatch(+1)
	case "N":
		t.jumpMatch(-1)
	case "s":
		t.cycleSort(true)
	case "S":
		t.cycleSort(false)
	case " ", "space":
		t.toggleSelectAtCursor()
	}
	t.gPrimed = false
}

func (t *Table) handleSearchKey(key tea.KeyPressMsg) {
	switch key.String() {
	case "esc":
		t.searchActive = false
		t.search = ""
		t.rebuildView()
	case "enter":
		t.searchActive = false
		t.rebuildView()
	case "backspace":
		if n := len(t.search); n > 0 {
			t.search = t.search[:n-1]
			t.rebuildView()
		}
	default:
		if text := key.Text; text != "" {
			t.search += text
			t.rebuildView()
		}
	}
}

func (t *Table) move(delta int) {
	if len(t.view) == 0 {
		t.cursor = 0
		return
	}
	t.cursor = clamp(t.cursor+delta, 0, len(t.view)-1)
	t.clampViewport()
}

func (t *Table) pageStep() int {
	if t.height > 1 {
		return t.height / 2
	}
	return 5
}

func (t *Table) clampViewport() {
	if t.height <= 0 {
		t.viewport = 0
		return
	}
	if t.cursor < t.viewport {
		t.viewport = t.cursor
	}
	if t.cursor >= t.viewport+t.height {
		t.viewport = t.cursor - t.height + 1
	}
	if t.viewport < 0 {
		t.viewport = 0
	}
}

func (t *Table) toggleSelectAtCursor() {
	if !t.selectable || len(t.view) == 0 {
		return
	}
	id := t.rows[t.view[t.cursor]].ID
	if id == "" {
		return
	}
	if _, ok := t.selected[id]; ok {
		delete(t.selected, id)
	} else {
		t.selected[id] = struct{}{}
	}
}

// cycleSort handles `s` (toggleDir=true) and `S` (toggleDir=false):
//
//   - `s` flips direction asc → desc → none on the current sort column. If
//     no column is yet sorted, it picks the first sortable column (asc).
//   - `S` advances to the next sortable column (asc), wrapping around. If
//     no column is yet sorted, behaves the same as `s`.
func (t *Table) cycleSort(toggleDir bool) {
	first := firstSortable(t.columns, 0, +1)
	if first < 0 {
		return
	}
	if t.sortCol < 0 {
		t.sortCol, t.sortDir = first, SortAsc
		t.rebuildView()
		t.cursor = 0
		t.viewport = 0
		return
	}
	if toggleDir {
		switch t.sortDir {
		case SortAsc:
			t.sortDir = SortDesc
		case SortDesc:
			t.sortDir, t.sortCol = SortNone, -1
		default:
			t.sortDir = SortAsc
		}
	} else {
		next := firstSortable(t.columns, (t.sortCol+1)%len(t.columns), +1)
		if next < 0 {
			next = first
		}
		t.sortCol, t.sortDir = next, SortAsc
	}
	t.rebuildView()
	t.cursor = 0
	t.viewport = 0
}

// firstSortable returns the first sortable column index starting from `from`,
// scanning by direction (+1 or -1) and wrapping. Returns -1 if none exists.
func firstSortable(cols []Column, from, direction int) int {
	if len(cols) == 0 {
		return -1
	}
	idx := ((from % len(cols)) + len(cols)) % len(cols)
	for range cols {
		if cols[idx].Sortable {
			return idx
		}
		idx = (idx + direction + len(cols)) % len(cols)
	}
	return -1
}

func (t *Table) jumpMatch(direction int) {
	if len(t.matches) == 0 {
		return
	}
	t.matchCursor = (t.matchCursor + direction + len(t.matches)) % len(t.matches)
	t.cursor = t.matches[t.matchCursor]
	t.clampViewport()
}

// rebuildView reapplies the search filter and sort, then clamps the cursor.
func (t *Table) rebuildView() {
	// preserve currently focused row id so the cursor stays on it.
	focusID := ""
	if len(t.view) > 0 && t.cursor < len(t.view) {
		focusID = t.rows[t.view[t.cursor]].ID
	}

	t.view = t.view[:0]
	for i := range t.rows {
		if t.matchesRow(t.rows[i]) {
			t.view = append(t.view, i)
		}
	}
	if t.sortCol >= 0 && t.sortCol < len(t.columns) && t.sortDir != SortNone {
		col := t.sortCol
		dir := t.sortDir
		sort.SliceStable(t.view, func(a, b int) bool {
			va := safeVal(t.rows[t.view[a]], col)
			vb := safeVal(t.rows[t.view[b]], col)
			if dir == SortAsc {
				return va < vb
			}
			return va > vb
		})
	}

	t.cursor = 0
	if focusID != "" {
		for i, idx := range t.view {
			if t.rows[idx].ID == focusID {
				t.cursor = i
				break
			}
		}
	}

	t.matches = t.matches[:0]
	t.matchCursor = 0
	if t.search != "" {
		needle := strings.ToLower(t.search)
		for vi, idx := range t.view {
			if rowContains(t.rows[idx], needle) {
				t.matches = append(t.matches, vi)
			}
		}
	}
	t.clampViewport()
}

func (t *Table) matchesRow(r Row) bool {
	if t.search == "" {
		return true
	}
	return rowContains(r, strings.ToLower(t.search))
}

func rowContains(r Row, needle string) bool {
	for _, v := range r.Values {
		if strings.Contains(strings.ToLower(v), needle) {
			return true
		}
	}
	return false
}

func safeVal(r Row, col int) string {
	if col < 0 || col >= len(r.Values) {
		return ""
	}
	return r.Values[col]
}

// ----- View -----

// View renders the table.
func (t *Table) View() string {
	colWidths := t.computeWidths()

	header := t.renderHeader(colWidths)

	var body []string
	if len(t.view) == 0 {
		body = []string{t.styles.StatusInfo.Render("(no rows)")}
	} else {
		start, end := t.viewportRange()
		for i := start; i < end; i++ {
			body = append(body, t.renderRow(i, colWidths))
		}
	}

	out := []string{header}
	out = append(out, body...)

	if t.searchActive || t.search != "" {
		out = append(out, t.renderSearchLine())
	}
	if t.sortCol >= 0 && t.sortDir != SortNone {
		dir := "asc"
		if t.sortDir == SortDesc {
			dir = "desc"
		}
		out = append(out, t.styles.StatusInfo.Render(
			fmt.Sprintf("sort: %s %s", t.columns[t.sortCol].Title, dir),
		))
	}
	return strings.Join(out, "\n")
}

func (t *Table) viewportRange() (int, int) {
	if t.height <= 0 || t.height >= len(t.view) {
		return 0, len(t.view)
	}
	start := t.viewport
	end := start + t.height
	if end > len(t.view) {
		end = len(t.view)
		start = max(0, end-t.height)
	}
	return start, end
}

func (t *Table) computeWidths() []int {
	widths := make([]int, len(t.columns))
	for i, c := range t.columns {
		w := c.Width
		if w == 0 {
			w = lipgloss.Width(c.Title)
			for _, r := range t.rows {
				if i < len(r.Values) {
					if rw := lipgloss.Width(r.Values[i]); rw > w {
						w = rw
					}
				}
			}
		}
		widths[i] = w
	}
	return widths
}

func (t *Table) renderHeader(widths []int) string {
	cells := make([]string, len(t.columns))
	for i, c := range t.columns {
		title := c.Title
		if i == t.sortCol && t.sortDir != SortNone {
			arrow := " ↑"
			if t.sortDir == SortDesc {
				arrow = " ↓"
			}
			title += arrow
		}
		cells[i] = padCell(title, widths[i], c.Align)
	}
	return t.styles.HelpTitle.Render(strings.Join(cells, "  "))
}

func (t *Table) renderRow(viewIdx int, widths []int) string {
	rowIdx := t.view[viewIdx]
	r := t.rows[rowIdx]
	cells := make([]string, len(t.columns))
	for i, c := range t.columns {
		v := ""
		if i < len(r.Values) {
			v = r.Values[i]
		}
		cells[i] = padCell(v, widths[i], c.Align)
	}
	prefix := "  "
	if t.selectable {
		mark := " "
		if _, ok := t.selected[r.ID]; ok {
			mark = "✓"
		}
		prefix = "[" + mark + "] "
	}
	line := prefix + strings.Join(cells, "  ")
	if viewIdx == t.cursor {
		return t.styles.HintKey.Render("> ") + line[2:]
	}
	return line
}

func (t *Table) renderSearchLine() string {
	prefix := t.styles.CommandHL.Render("/")
	body := prefix + t.styles.Command.Render(t.search)
	if !t.searchActive && len(t.matches) > 0 {
		body += "  " + t.styles.StatusInfo.Render(
			fmt.Sprintf("[%d/%d]", t.matchCursor+1, len(t.matches)),
		)
	} else if !t.searchActive && t.search != "" {
		body += "  " + t.styles.StatusWarn.Render("no matches")
	}
	return body
}

func padCell(s string, width int, align lipgloss.Position) string {
	w := lipgloss.Width(s)
	if w >= width {
		return s
	}
	pad := strings.Repeat(" ", width-w)
	switch align {
	case lipgloss.Right:
		return pad + s
	case lipgloss.Center:
		left := (width - w) / 2
		return strings.Repeat(" ", left) + s + strings.Repeat(" ", width-w-left)
	default:
		return s + pad
	}
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
