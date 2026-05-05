// Package help renders the full-screen help overlay shown when the user
// presses `?`. The layout mirrors k9s: a title row, then a grid of
// categorized key/description tables, with the build identity at the
// bottom.
//
// Screens contribute their own sections (navigation, filtering, actions,
// etc.) via the HelpProvider interface; the host always appends a
// fixed General category covering global shortcuts (`:`, `/`, `?`,
// `ctrl+r`, `ctrl+c`, `q`, `esc`).
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

	// Screen names the active screen — surfaced in the title so the user
	// understands the context-specific block belongs to "Topics" / "Messages"
	// / etc. Empty falls back to "Help".
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
// 1, 2, or 3 columns depending on Width — sections never split across
// columns.
func Render(opts Options) string {
	styles := opts.Styles
	width := opts.Width
	if width < 1 {
		width = 80
	}

	title := styles.HelpTitle.Render("Help")
	if opts.Screen != "" {
		title += "  " + styles.StatusInfo.Render("· "+opts.Screen)
	}
	hint := styles.StatusInfo.Render("press ? · esc · q to close")

	grid := layoutGrid(opts.Sections, width, styles)

	parts := []string{title, "", grid}
	if opts.Footer != "" || hint != "" {
		parts = append(parts, "")
		footer := hint
		if opts.Footer != "" {
			version := styles.StatusInfo.Render(opts.Footer)
			pad := max(1, width-lipgloss.Width(hint)-lipgloss.Width(version))
			footer = hint + strings.Repeat(" ", pad) + version
		}
		parts = append(parts, footer)
	}
	return strings.Join(parts, "\n")
}

// columnGap is the number of blank columns between adjacent section
// columns in the grid.
const columnGap = 4

// layoutGrid arranges sections into 1/2/3 columns. The renderer first
// computes each section's intrinsic width, then greedily packs sections
// top-to-bottom by column, balancing total height.
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

	cols := pickColumnCount(widths, width)
	groups := packIntoColumns(rendered, heights, cols)

	columns := make([]string, len(groups))
	for c, group := range groups {
		columns[c] = strings.Join(group, "\n\n")
	}
	gap := strings.Repeat(" ", columnGap)
	return joinHorizontalWithGap(columns, gap)
}

// pickColumnCount returns the largest column count that still fits the
// widest section into the available width (with gaps), capped at 3.
func pickColumnCount(widths []int, total int) int {
	maxW := 0
	for _, w := range widths {
		if w > maxW {
			maxW = w
		}
	}
	if maxW == 0 {
		return 1
	}
	for cols := 3; cols >= 1; cols-- {
		need := cols*maxW + (cols-1)*columnGap
		if need <= total {
			return cols
		}
	}
	return 1
}

// packIntoColumns distributes sections into `cols` columns by appending
// each section to the currently shortest column. This keeps the overall
// grid balanced without splitting sections.
func packIntoColumns(rendered []string, heights []int, cols int) [][]string {
	groups := make([][]string, cols)
	totals := make([]int, cols)
	for i, body := range rendered {
		// pick the shortest column; ties go to the leftmost.
		pick := 0
		for c := 1; c < cols; c++ {
			if totals[c] < totals[pick] {
				pick = c
			}
		}
		groups[pick] = append(groups[pick], body)
		// +2 accounts for the blank separator line between sections.
		totals[pick] += heights[i] + 2
	}
	return groups
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

// joinHorizontalWithGap is a thin wrapper around lipgloss.JoinHorizontal
// that injects a fixed-width gap string between adjacent columns. We
// keep the helper local so the package depends on lipgloss only for
// width measurement.
func joinHorizontalWithGap(columns []string, gap string) string {
	if len(columns) == 0 {
		return ""
	}
	if len(columns) == 1 {
		return columns[0]
	}
	parts := make([]string, 0, 2*len(columns)-1)
	for i, col := range columns {
		if i > 0 {
			parts = append(parts, gap)
		}
		parts = append(parts, col)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, parts...)
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
// appended after every screen's contribution: command bar, search,
// auto-refresh, and quit/help/exit shortcuts.
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
	}
}
