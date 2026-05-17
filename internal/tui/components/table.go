// Package components provides reusable Bubble Tea building blocks shared by
// every screen (Table, Form, Confirm, Toasts, Menu, Refresher).
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
	// Width is the rendered width. 0 means "auto" (max of title and longest
	// value). Ignored when Flex is true.
	Width int
	// MinWidth is the lower bound for a Flex column. 0 falls back to the
	// title width.
	MinWidth int
	// Flex distributes any leftover width evenly across Flex columns.
	// Requires the total width to be set via SetTotalWidth.
	Flex     bool
	Align    lipgloss.Position
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
	SortNone SortDirection = iota
	SortAsc
	SortDesc
)

// Table is a navigable, searchable, sortable, selectable list of rows. It
// owns *only* the view/selection/sort state; callers feed it data via SetRows.
type Table struct {
	columns []Column
	rows    []Row
	view    []int // indices into rows after filter+sort

	cursor   int
	viewport int
	height   int // 0 = fit-all, no scrolling
	width    int // 0 disables flex distribution

	search      string
	matches     []int // indices into `view` that contain matches
	matchCursor int

	sortCol int
	sortDir SortDirection

	selectable bool
	selected   map[string]struct{}

	styles theme.Styles
}

type TableOption func(*Table)

func WithSelectable(on bool) TableOption {
	return func(t *Table) { t.selectable = on }
}

func WithStyles(s theme.Styles) TableOption {
	return func(t *Table) { t.styles = s }
}

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

// SetRows replaces the rows. The view is rebuilt and selection (by ID) is
// preserved.
func (t *Table) SetRows(rows []Row) {
	t.rows = append([]Row(nil), rows...)
	t.rebuildView()
}

func (t *Table) Rows() []Row {
	out := make([]Row, len(t.rows))
	copy(out, t.rows)
	return out
}

// Cursor returns the current row index in the *view* (post filter/sort).
func (t *Table) Cursor() int { return t.cursor }

// GoToID moves the cursor to the row with the given ID.
func (t *Table) GoToID(id string) bool {
	for i, idx := range t.view {
		if t.rows[idx].ID == id {
			t.cursor = i
			return true
		}
	}
	return false
}

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

func (t *Table) IsSelected(id string) bool {
	_, ok := t.selected[id]
	return ok
}

func (t *Table) ClearSelection() { t.selected = make(map[string]struct{}) }

// Sort returns the active sort column index (-1 if none) and direction.
func (t *Table) Sort() (int, SortDirection) { return t.sortCol, t.sortDir }

// SetSort applies a sort programmatically. Pass -1 / SortNone to clear.
func (t *Table) SetSort(col int, dir SortDirection) {
	t.sortCol, t.sortDir = col, dir
	t.rebuildView()
	t.cursor = 0
	t.viewport = 0
}

func (t *Table) Search() string { return t.search }

// SetSearch replaces the search query and re-applies the filter. Used by
// hosts that render the search prompt themselves and just want live row
// filtering.
func (t *Table) SetSearch(query string) {
	t.search = query
	t.rebuildView()
}

func (t *Table) FilteredCount() int { return len(t.view) }

func (t *Table) TotalCount() int { return len(t.rows) }

// SetHeight changes the table's visible height. h is the total rendered
// height the table will occupy — column header + data rows + an optional
// sort indicator when a sort is active. The table computes its own data
// row capacity from h, so callers pass through whatever vertical space
// the surrounding screen gave them without subtracting anything for the
// table's chrome. 0 means "fit-all rows, no scrolling".
func (t *Table) SetHeight(rows int) {
	t.height = rows
	t.clampViewport()
}

// tableHeaderRows is the fixed chrome the table always renders above its
// data rows (the column header line).
const tableHeaderRows = 1

// bodyRows reports how many data rows fit in the table's current height
// after subtracting its own chrome (column header plus the sort indicator
// when a sort is active). Returns 0 when SetHeight has not been called
// (height == 0) — the "fit-all" sentinel — or when h is smaller than the
// chrome. Hot path, called from every key event and render.
func (t *Table) bodyRows() int {
	if t.height <= 0 {
		return 0
	}
	chrome := tableHeaderRows
	if t.sortCol >= 0 && t.sortDir != SortNone {
		chrome++
	}
	if t.height <= chrome {
		return 0
	}
	return t.height - chrome
}

// SetTotalWidth tells the table how many columns are available for its
// rendered output. Required for Flex columns. 0 disables flex distribution.
func (t *Table) SetTotalWidth(cols int) { t.width = cols }

// Update routes a key message into the table. Unrecognized keys are ignored
// so screens can layer their own.
func (t *Table) Update(msg tea.Msg) (*Table, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
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
	case "ctrl+f", "pgdown":
		t.move(+t.pageStep())
	case "ctrl+b", "pgup":
		t.move(-t.pageStep())
	case "end":
		t.cursor = max(0, len(t.view)-1)
		t.clampViewport()
	case "home":
		t.cursor = 0
		t.clampViewport()
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
	body := t.bodyRows()
	if body > 1 {
		return body - 1
	}
	return 1
}

func (t *Table) clampViewport() {
	body := t.bodyRows()
	if body <= 0 {
		t.viewport = 0
		return
	}
	if t.cursor < t.viewport {
		t.viewport = t.cursor
	}
	if t.cursor >= t.viewport+body {
		t.viewport = t.cursor - body + 1
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

// cycleSort: s flips direction asc → desc → none on the current sort column;
// S advances to the next sortable column (asc), wrapping around.
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

// rebuildView reapplies the search filter and sort, preserving the focused
// row id so the cursor stays on it.
func (t *Table) rebuildView() {
	focusID := ""
	if len(t.view) > 0 && t.cursor < len(t.view) {
		if idx := t.view[t.cursor]; idx >= 0 && idx < len(t.rows) {
			focusID = t.rows[idx].ID
		}
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

	// inline search line intentionally not rendered: the host owns the
	// prompt and surfaces filter + match count in the frame title.
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
	// height == 0 is the "fit-all, no scrolling" sentinel — render every
	// row regardless of count. Distinct from "h smaller than the table's
	// own chrome" below, where the caller asked for a tiny height and we
	// must render zero data rows rather than overflow it.
	if t.height <= 0 {
		return 0, len(t.view)
	}
	body := t.bodyRows()
	if body <= 0 {
		return 0, 0
	}
	if body >= len(t.view) {
		return 0, len(t.view)
	}
	start := t.viewport
	end := start + body
	if end > len(t.view) {
		end = len(t.view)
		start = max(0, end-body)
	}
	return start, end
}

func (t *Table) computeWidths() []int {
	widths := make([]int, len(t.columns))
	flexIdxs := make([]int, 0, len(t.columns))
	fixedTotal := 0
	for i, c := range t.columns {
		if c.Flex {
			lo := c.MinWidth
			if lo == 0 {
				lo = lipgloss.Width(c.Title)
			}
			widths[i] = lo
			flexIdxs = append(flexIdxs, i)
			fixedTotal += lo
			continue
		}
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
		fixedTotal += w
	}
	// reserve space for the sort arrow on the active sort column.
	// fixedTotal must absorb the bump too — otherwise flex distribution
	// below over-allocates by 2 cells and rows exceed t.width.
	if t.sortCol >= 0 && t.sortCol < len(widths) && t.sortDir != SortNone {
		widths[t.sortCol] += 2
		fixedTotal += 2
	}
	if t.width > 0 && len(flexIdxs) > 0 {
		// row layout: optional "[ ] " (4 cols) + per-column "  " separators.
		separators := 2 * (len(t.columns) - 1)
		prefix := 0
		if t.selectable {
			prefix = 4
		}
		leftover := t.width - fixedTotal - separators - prefix
		if leftover > 0 {
			share := leftover / len(flexIdxs)
			extra := leftover % len(flexIdxs)
			for j, i := range flexIdxs {
				widths[i] += share
				if j < extra {
					widths[i]++
				}
			}
		}
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
	// align over the row's "[ ] " multi-select prefix when selectable.
	prefix := ""
	if t.selectable {
		prefix = "    "
	}
	return t.styles.HelpTitle.Render(prefix + strings.Join(cells, "  "))
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
	prefix := ""
	if t.selectable {
		mark := " "
		if _, ok := t.selected[r.ID]; ok {
			mark = "✓"
		}
		prefix = "[" + mark + "] "
	}
	line := prefix + strings.Join(cells, "  ")
	if viewIdx != t.cursor {
		return line
	}
	// k9s-style highlight: bg-only style preserves per-cell foreground
	// colors (cluster swatches, group state). Pad to t.width so the
	// background spans the full table interior when the host sized us.
	if t.width > 0 {
		if w := lipgloss.Width(line); w < t.width {
			line += strings.Repeat(" ", t.width-w)
		}
	}
	return applyRowHighlight(t.styles.TableCursor, line)
}

// applyRowHighlight wraps line with style's background, re-applying the
// bg SGR after every inner SGR reset so per-cell fg-styled glyphs
// (cluster swatches, group state) don't punch holes in the highlight.
//
// lipgloss v2 emits "\x1b[m" (short form) as its only reset; "\x1b[0m"
// is also handled for robustness against pre-styled content originating
// outside lipgloss. Compound resets like "\x1b[0;1m" or bare bg-default
// "\x1b[49m" are not handled — lipgloss does not produce them today.
//
// The opening SGR is extracted by probing the style with a sentinel
// character and slicing what lipgloss put in front of it. The sentinel
// is SOH (0x01) rather than NUL (0x00) so we don't tempt future
// C-string-aware sanitizers; the regression test
// TestTable_CursorRowHighlightSurvivesNestedANSIResets catches silent
// breakage if the probe ever stops returning the prefix.
func applyRowHighlight(style lipgloss.Style, line string) string {
	const sentinel = "\x01"
	probe := style.Render(sentinel)
	idx := strings.Index(probe, sentinel)
	if idx <= 0 {
		// no SGR emitted (style disabled / no color) — render as-is.
		return style.Render(line)
	}
	openSGR := probe[:idx]
	line = strings.ReplaceAll(line, "\x1b[0m", "\x1b[0m"+openSGR)
	line = strings.ReplaceAll(line, "\x1b[m", "\x1b[m"+openSGR)
	return openSGR + line + "\x1b[0m"
}

func padCell(s string, width int, align lipgloss.Position) string {
	w := lipgloss.Width(s)
	if w == width {
		return s
	}
	if w > width {
		out := TruncateText(s, width)
		// wide runes (CJK, emoji) can't be split by the column boundary, so
		// TruncateText may stop one cell short of the budget. Right-pad the
		// remainder so the next column stays aligned.
		if ow := lipgloss.Width(out); ow < width {
			out += strings.Repeat(" ", width-ow)
		}
		return out
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
