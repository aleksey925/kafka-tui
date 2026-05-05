// Package help renders the full-screen help overlay shown when the user
// presses `?`. The layout mirrors k9s: a title row, then a grid of
// categorized key/description tables, with the build identity at the
// bottom.
//
// Screens contribute their own sections (navigation, filtering, actions,
// etc.) via the HelpProvider interface; the host then appends the
// fixed global categories returned by [GeneralSections] (General,
// Navigation, Commands) so every screen surfaces the same baseline
// shortcuts at the bottom of the grid.
package help

import (
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Hint is a single key → description row inside a [Section].
type Hint struct {
	Key   string
	Label string
}

// Section is a named group of related shortcuts (e.g. "Navigation",
// "Filtering"). Sections render as a captioned 2-column table.
type Section struct {
	Title string
	Hints []Hint
}

// Options configure a single render pass.
type Options struct {
	Width  int
	Height int

	// Screen names the active screen — when set, an internal title row
	// "Help · <Screen>" is rendered above the grid. Hosts that already
	// surface the title elsewhere (e.g. inside a [layout.Frame] border)
	// should leave this empty; in that case the title row is skipped
	// entirely and the grid sits at the top of the output.
	Screen string

	// Sections is the merged list to render — the caller is responsible
	// for prepending the screen-specific block and appending the global
	// ones. The renderer treats this slice as the final authoritative list.
	Sections []Section

	// Footer is rendered right-aligned under the grid. Used for the build
	// identity line ("v0.4.2 (a1b2c3d)").
	Footer string

	Styles theme.Styles
}

// Render produces the final overlay string. The grid is laid out in
// 1..maxColumns columns depending on Width — sections never split
// across columns. The output is padded vertically to opts.Height so
// the footer pins to the bottom of the terminal (k9s-style: the help
// view fills the full screen rather than hugging its content).
func Render(opts Options) string {
	styles := opts.Styles
	width := opts.Width
	if width < 1 {
		width = 80
	}

	hint := styles.StatusInfo.Render("press ? · esc · q to close")

	grid := layoutGrid(opts.Sections, width, styles)

	footer := ""
	if opts.Footer != "" || hint != "" {
		footer = hint
		if opts.Footer != "" {
			version := styles.StatusInfo.Render(opts.Footer)
			pad := max(1, width-lipgloss.Width(hint)-lipgloss.Width(version))
			footer = hint + strings.Repeat(" ", pad) + version
		}
	}

	// the optional internal title is for callers that don't wrap the
	// overlay in a frame with its own title slot — when present it
	// renders above the grid with one blank separator line.
	body := grid
	if opts.Screen != "" {
		title := styles.HelpTitle.Render("Help") +
			"  " + styles.StatusInfo.Render("· "+opts.Screen)
		body = title + "\n\n" + grid
	}
	if footer == "" {
		return body
	}
	bodyLines := strings.Count(body, "\n") + 1
	footerLines := strings.Count(footer, "\n") + 1
	// pad with blank lines so the footer sits on the bottom row when
	// Height is known. Inserting N '\n' chars between body and footer
	// adds (N - 1) empty lines, so for total = Height we need
	// N = Height - bodyLines - footerLines + 1. Falls back to a single
	// blank separator (N=2) when Height is unknown.
	gap := 2
	if opts.Height > 0 {
		gap = max(2, opts.Height-bodyLines-footerLines+1)
	}
	return body + strings.Repeat("\n", gap) + footer
}

// minColumnGap is the smallest gap between adjacent section columns;
// any leftover horizontal space beyond this minimum is distributed
// evenly between columns to mimic k9s' stretchy spacer cell.
const minColumnGap = 4

// maxColumns caps the grid width; with our typical screen-side
// contribution (3–5 categories) plus 3 host categories, more than 4
// columns produces a wide, sparse grid that's harder to scan.
const maxColumns = 4

// layoutGrid arranges sections into a grid. The renderer computes each
// section's intrinsic width, picks the largest column count that fits
// (using the actual per-column max widths, not a uniform max), then
// distributes sections in declaration order — newspaper-style: the
// first ⌈n/cols⌉ sections fill column 0 top-to-bottom, the next chunk
// fills column 1, and so on. Section heights within a column are
// equalized so the grid lines up vertically.
func layoutGrid(sections []Section, width int, styles theme.Styles) string {
	if len(sections) == 0 {
		return ""
	}
	rendered := make([]string, len(sections))
	widths := make([]int, len(sections))
	heights := make([]int, len(sections))
	for i, sec := range sections {
		rendered[i] = renderSection(sec, styles)
		widths[i] = lipgloss.Width(rendered[i])
		heights[i] = strings.Count(rendered[i], "\n") + 1
	}

	cols, groupsIdx := pickLayout(widths, width)
	colWidths := make([]int, cols)
	for c, idxs := range groupsIdx {
		w := 0
		for _, i := range idxs {
			if widths[i] > w {
				w = widths[i]
			}
		}
		colWidths[c] = w
	}

	rendered = padSectionsByRow(rendered, heights, groupsIdx)

	columns := make([]string, cols)
	for c, idxs := range groupsIdx {
		bodies := make([]string, len(idxs))
		for i, idx := range idxs {
			bodies[i] = rendered[idx]
		}
		columns[c] = strings.Join(bodies, "\n\n")
	}

	return joinColumnsFlex(columns, colWidths, width)
}

// padSectionsByRow equalizes section heights row-by-row across columns:
// the i-th section in every column is padded with blank lines up to the
// tallest section at that row position. This keeps section boundaries
// (and therefore subsequent section captions) on the same y-coordinate
// across the grid — k9s' "all sections same height" effect generalized
// to a multi-row grid.
func padSectionsByRow(rendered []string, heights []int, groups [][]int) []string {
	if len(groups) == 0 {
		return rendered
	}
	maxRow := 0
	for _, idxs := range groups {
		if len(idxs) > maxRow {
			maxRow = len(idxs)
		}
	}
	out := make([]string, len(rendered))
	copy(out, rendered)
	for row := range maxRow {
		target := 0
		for _, idxs := range groups {
			if row < len(idxs) && heights[idxs[row]] > target {
				target = heights[idxs[row]]
			}
		}
		for _, idxs := range groups {
			if row >= len(idxs) {
				continue
			}
			i := idxs[row]
			if pad := target - heights[i]; pad > 0 {
				out[i] += strings.Repeat("\n", pad)
			}
		}
	}
	return out
}

// pickLayout picks the column count and the index grouping in one
// pass: it tries cols from maxColumns down to 1 and keeps the first
// configuration whose per-column max widths sum (plus minimum gaps)
// fits within `total`. Sections are chunked in declaration order.
func pickLayout(widths []int, total int) (int, [][]int) {
	n := len(widths)
	for cols := maxColumns; cols >= 2; cols-- {
		if cols > n {
			continue
		}
		groups := chunkIndices(n, cols)
		need := (cols - 1) * minColumnGap
		for _, idxs := range groups {
			w := 0
			for _, i := range idxs {
				if widths[i] > w {
					w = widths[i]
				}
			}
			need += w
		}
		if need <= total {
			return cols, groups
		}
	}
	return 1, [][]int{intRange(n)}
}

// chunkIndices splits [0,n) into `cols` consecutive index groups whose
// sizes differ by at most 1. Earlier groups absorb the remainder so the
// left-most columns are the densest — matches how readers scan
// top-to-bottom, left-to-right.
func chunkIndices(n, cols int) [][]int {
	groups := make([][]int, cols)
	base := n / cols
	extra := n % cols
	pos := 0
	for c := range cols {
		size := base
		if c < extra {
			size++
		}
		idxs := make([]int, size)
		for i := range size {
			idxs[i] = pos + i
		}
		groups[c] = idxs
		pos += size
	}
	return groups
}

func intRange(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// joinColumnsFlex joins rendered column bodies horizontally, distributing
// leftover horizontal space as equal gaps between columns. This mimics
// k9s' stretchy spacer cell (`SetExpansion(1)`): sections keep their
// intrinsic width, and unused width pushes the columns apart instead of
// piling up on the right.
func joinColumnsFlex(columns []string, colWidths []int, total int) string {
	if len(columns) == 0 {
		return ""
	}
	if len(columns) == 1 {
		return columns[0]
	}
	used := 0
	for _, w := range colWidths {
		used += w
	}
	gaps := len(columns) - 1
	leftover := total - used
	gapW := max(minColumnGap, leftover/gaps)
	gap := strings.Repeat(" ", gapW)
	parts := make([]string, 0, 2*len(columns)-1)
	for i, col := range columns {
		if i > 0 {
			parts = append(parts, gap)
		}
		parts = append(parts, col)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, parts...)
}

// renderSection draws one captioned key/label table. Keys are
// right-padded to a uniform width so descriptions align inside the
// section.
func renderSection(sec Section, styles theme.Styles) string {
	if sec.Title == "" && len(sec.Hints) == 0 {
		return ""
	}
	keyW := 0
	for _, h := range sec.Hints {
		w := lipgloss.Width(formatKey(h.Key))
		if w > keyW {
			keyW = w
		}
	}
	lines := []string{styles.HelpTitle.Render(sec.Title)}
	for _, h := range sec.Hints {
		key := formatKey(h.Key)
		pad := max(0, keyW-lipgloss.Width(key))
		row := styles.HintKey.Render(key) + strings.Repeat(" ", pad) +
			"  " + styles.HintLabel.Render(h.Label)
		lines = append(lines, row)
	}
	return strings.Join(lines, "\n")
}

// formatKey wraps a key in angle brackets for visual consistency with
// the bottom hints bar (`<ctrl+r>`).
func formatKey(k string) string {
	if k == "" {
		return ""
	}
	return "<" + k + ">"
}

// SectionsFromBindings groups [keymap.Binding] entries by Category
// (in first-seen order) and returns one [Section] per non-empty
// category. Bindings without a Category are skipped — they are
// intentionally hidden from the user. Lives here so the dependency
// arrow points help → keymap, not the other way around.
func SectionsFromBindings(bindings []keymap.Binding) []Section {
	order := make([]string, 0)
	byCat := make(map[string][]Hint)
	for _, b := range bindings {
		if b.Category == "" || len(b.Keys) == 0 {
			continue
		}
		if _, seen := byCat[b.Category]; !seen {
			order = append(order, b.Category)
		}
		byCat[b.Category] = append(byCat[b.Category], Hint{
			Key:   b.Display(),
			Label: b.Label,
		})
	}
	sections := make([]Section, 0, len(order))
	for _, cat := range order {
		sections = append(sections, Section{Title: cat, Hints: byCat[cat]})
	}
	return sections
}

// GeneralSections returns the global, screen-agnostic categories
// appended after every screen's contribution. Order mirrors k9s:
// General → Navigation → Commands (k9s: GENERAL → NAVIGATION → HOTKEYS).
func GeneralSections() []Section {
	return []Section{
		{
			Title: "General",
			Hints: []Hint{
				{Key: ":", Label: "open command bar"},
				{Key: "/", Label: "filter list"},
				{Key: "ctrl+r", Label: "toggle auto-refresh"},
				{Key: "?", Label: "toggle help"},
				{Key: "q", Label: "back / quit"},
				{Key: "esc", Label: "cancel / clear filter"},
				{Key: "ctrl+c", Label: "force quit"},
			},
		},
		{
			Title: "Navigation",
			Hints: []Hint{
				{Key: "↑ / k", Label: "row up"},
				{Key: "↓ / j", Label: "row down"},
				{Key: "pgup / ctrl+b", Label: "page up"},
				{Key: "pgdn / ctrl+f", Label: "page down"},
				{Key: "g / home", Label: "first row"},
				{Key: "G / end", Label: "last row"},
				{Key: "enter", Label: "open / drill in"},
			},
		},
		{
			Title: "Commands",
			Hints: []Hint{
				{Key: ":topics", Label: "topics list"},
				{Key: ":groups", Label: "consumer groups"},
				{Key: ":clusters", Label: "cluster list"},
				{Key: ":cluster <name>", Label: "switch cluster"},
				{Key: ":logs", Label: "log viewer"},
				{Key: ":config sources", Label: "config provenance"},
			},
		},
	}
}
