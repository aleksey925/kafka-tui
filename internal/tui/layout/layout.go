// Package layout renders the persistent chrome around every screen: the
// global header (cluster identity), the command bar, the status bar (refresh
// state), and the bottom-row key hints.
package layout

import (
	"fmt"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/lineedit"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// RefreshMode mirrors the high-level state of the auto-refresh subsystem.
type RefreshMode int

const (
	RefreshOff RefreshMode = iota
	RefreshAuto
	// RefreshManual: the user disabled auto-refresh for this screen.
	RefreshManual
	// RefreshPaused: a modal/search is open and refresh is temporarily paused.
	RefreshPaused
	// RefreshOnEdit: the screen reloads itself in response to filesystem
	// events (no periodic poll).
	RefreshOnEdit
	// RefreshNotApplicable: the screen is conceptually static; rendered as
	// a dash to distinguish from RefreshOff ("supported but turned off").
	RefreshNotApplicable
)

// HeaderInfo describes everything required to render the top bar.
type HeaderInfo struct {
	Cluster      string
	ClusterColor string
	ReadOnly     bool
	FromCLI      bool
	// Context names the configuration source: "cli" when launched with
	// --brokers, otherwise the source of clusters.yaml ("global" / "project").
	Context string
	// Filter, when non-empty, surfaces the active screen-level filter.
	Filter string
}

// StatusInfo describes the right-aligned status block.
type StatusInfo struct {
	Mode        RefreshMode
	Interval    time.Duration
	LastRefresh time.Time // zero value means never
	Now         time.Time // injected for deterministic rendering
}

// KeyHint is one entry in the bottom hints bar.
type KeyHint struct {
	Key   string
	Label string
}

// CommandBar is the prompt shown when the user types `:` or `/`.
type CommandBar struct {
	Active     bool
	Prefix     rune // ':' or '/'
	Buffer     string
	Cursor     int    // rune offset within Buffer
	Suggestion string // ghost text shown after the buffer (tab to accept)
	Error      string
}

// HeaderRows is the fixed height of the multi-pane header block.
const HeaderRows = 5

// Build is the binary's identity surfaced in the header's right pane.
type Build struct {
	Version string
	Commit  string
}

// Header renders the three-pane header (cluster info | menu | brand). The
// block is exactly [HeaderRows] tall so body geometry stays stable.
func Header(s theme.Styles, info HeaderInfo, status StatusInfo, hints []KeyHint, build Build, width int) string {
	if width < 40 {
		return compactHeader(s, info)
	}
	rightW := 22
	leftW := 30
	if width < 80 {
		leftW = max(20, width/3)
		rightW = max(16, width/4)
	}
	midW := max(10, width-leftW-rightW)

	left := renderClusterInfo(s, info, status, leftW)
	mid := renderMenu(s, hints, midW)
	right := renderBrand(s, build, rightW)

	return lipgloss.JoinHorizontal(lipgloss.Top, left, mid, right)
}

func compactHeader(s theme.Styles, info HeaderInfo) string {
	parts := []string{s.Header.Render("kafka-tui")}
	if info.Cluster != "" {
		parts = append(parts, "· "+s.Cluster.Render(info.Cluster))
	}
	if info.ReadOnly {
		parts = append(parts, s.ReadOnly.Render("[RO]"))
	}
	return strings.Join(parts, " ")
}

func renderClusterInfo(s theme.Styles, info HeaderInfo, status StatusInfo, width int) string {
	cluster := "—"
	if info.Cluster != "" {
		swatch := lipgloss.NewStyle().
			Foreground(s.Palette.ClusterColor(info.ClusterColor)).
			Render("●")
		cluster = swatch + " " + s.Cluster.Render(info.Cluster)
	}
	mode := "read-write"
	if info.ReadOnly {
		mode = s.ReadOnly.Render("read-only")
	}
	context := info.Context
	if context == "" {
		if info.FromCLI {
			context = "cli"
		} else {
			context = "—"
		}
	}
	filter := info.Filter
	if filter == "" {
		filter = "—"
	}
	rows := []struct{ key, val string }{
		{"Context", context},
		{"Cluster", cluster},
		{"Refresh", refreshLabel(s, status)},
		{"Mode", mode},
		{"Filter", filter},
	}
	lines := make([]string, 0, HeaderRows)
	keyStyle := lipgloss.NewStyle().Foreground(s.Palette.Muted)
	// max content width inside the left pane reserves clusterPaneGutter cols
	// so even worst-case values don't sit flush against the menu pane.
	contentWidth := max(width-clusterPaneGutter, 1)
	for _, r := range rows {
		key := keyStyle.Render(padRight(r.key+":", 9))
		line := " " + key + " " + r.val
		if lipgloss.Width(line) > contentWidth {
			line = ansi.Truncate(line, contentWidth, "…")
		}
		lines = append(lines, padLine(line, width))
	}
	for len(lines) < HeaderRows {
		lines = append(lines, padLine("", width))
	}
	return strings.Join(lines[:HeaderRows], "\n")
}

// clusterPaneGutter is the minimum number of trailing blank columns the left
// header pane reserves before the menu pane, so long values (Refresh, long
// cluster names) never butt up against the next column.
const clusterPaneGutter = 2

func refreshLabel(s theme.Styles, status StatusInfo) string {
	switch status.Mode {
	case RefreshAuto:
		body := "auto " + formatDuration(status.Interval)
		if elapsed, ok := elapsedSince(status); ok {
			body += " · " + formatElapsed(elapsed)
		}
		return body
	case RefreshManual:
		return "manual"
	case RefreshPaused:
		return s.StatusWarn.Render("paused")
	case RefreshOnEdit:
		body := "on edit"
		if elapsed, ok := elapsedSince(status); ok {
			body += " · " + formatElapsed(elapsed)
		}
		return body
	case RefreshOff:
		return "off"
	case RefreshNotApplicable:
		return "—"
	default:
		return "—"
	}
}

// elapsedSince reports the time since the last refresh, returning false when
// either side of the timestamp pair is zero (i.e. the screen has never
// refreshed or the wall-clock isn't wired up yet).
func elapsedSince(status StatusInfo) (time.Duration, bool) {
	if status.LastRefresh.IsZero() || status.Now.IsZero() {
		return 0, false
	}
	return max(0, status.Now.Sub(status.LastRefresh)), true
}

// formatElapsed renders the time since last refresh in a compact form
// (typically 2-3 chars; 4 once values cross 100s/100m/100h). Sub-minute is
// precise; longer ranges floor to whole minutes / hours so the chrome stays
// dense — the trailing "ago" word is intentionally dropped, as the "·"
// separator already conveys "since last refresh".
func formatElapsed(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Round(time.Second).Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	default:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
}

func renderMenu(s theme.Styles, hints []KeyHint, width int) string {
	if len(hints) == 0 {
		hints = []KeyHint{
			{Key: ":", Label: "command"},
			{Key: "?", Label: "help"},
			{Key: "q", Label: "quit"},
		}
	}
	cols := 2
	if width < 40 {
		cols = 1
	}
	colW := width / cols
	rowsN := min((len(hints)+cols-1)/cols, HeaderRows)
	cells := make([]string, len(hints))
	for i, h := range hints {
		cells[i] = padRight(s.HintKey.Render("<"+h.Key+">")+" "+s.HintLabel.Render(h.Label), colW)
	}
	lines := make([]string, HeaderRows)
	for r := range HeaderRows {
		var line strings.Builder
		for c := range cols {
			// only emit a cell when it falls inside its own column slot,
			// otherwise the row leaks the next column's content.
			idx := c*rowsN + r
			if r < rowsN && idx < len(cells) {
				line.WriteString(cells[idx])
			} else {
				line.WriteString(strings.Repeat(" ", colW))
			}
		}
		lines[r] = padLine(line.String(), width)
	}
	return strings.Join(lines, "\n")
}

func renderBrand(s theme.Styles, build Build, width int) string {
	title := s.Header.Render("kafka-tui")
	version := build.Version
	if version == "" {
		version = "(dev)"
	}
	commit := ""
	if build.Commit != "" {
		commit = " (" + build.Commit + ")"
	}
	versionLine := s.StatusInfo.Render(version + commit)
	lines := []string{
		"",
		padLine(centerInWidth(title, width), width),
		padLine(centerInWidth(versionLine, width), width),
		"",
		"",
	}
	return strings.Join(lines, "\n")
}

func centerInWidth(s string, width int) string {
	w := lipgloss.Width(s)
	if w >= width {
		return s
	}
	left := (width - w) / 2
	return strings.Repeat(" ", left) + s
}

func padLine(s string, width int) string {
	w := lipgloss.Width(s)
	if w >= width {
		return s
	}
	return s + strings.Repeat(" ", width-w)
}

func padRight(s string, width int) string {
	w := lipgloss.Width(s)
	if w >= width {
		return s
	}
	return s + strings.Repeat(" ", width-w)
}

// HintsFromBindings projects [keymap.Binding] entries flagged Hint=true
// into the bottom-bar [KeyHint] form, preserving slice order.
func HintsFromBindings(bindings []keymap.Binding) []KeyHint {
	out := make([]KeyHint, 0, len(bindings))
	for _, b := range bindings {
		if !b.Hint || len(b.Keys) == 0 {
			continue
		}
		out = append(out, KeyHint{Key: b.Display(), Label: b.Label})
	}
	return out
}

// KeyHints renders the bottom row of `key label` pairs.
func KeyHints(s theme.Styles, hints []KeyHint) string {
	if len(hints) == 0 {
		return ""
	}
	var b strings.Builder
	for i, h := range hints {
		if i > 0 {
			b.WriteString("  ")
		}
		b.WriteString(s.HintKey.Render(h.Key))
		b.WriteString(" ")
		b.WriteString(s.HintLabel.Render(h.Label))
	}
	return b.String()
}

// CommandRows is the height a rendered command/search prompt occupies when
// active (top border + body + bottom border).
const CommandRows = 3

// CommandLine renders the command bar prompt as a bordered single-row box,
// or returns "" when inactive. Hosts reserve [CommandRows] only when
// [CommandBar.Active] is true.
func CommandLine(s theme.Styles, c CommandBar, width int) string {
	if !c.Active {
		return ""
	}
	prefix := string(c.Prefix)
	body := s.CommandHL.Render(prefix) + " " + renderBufferWithCursor(s, c.Buffer, c.Cursor)
	// the ghost suggestion is only meaningful when appended after the buffer —
	// hide it while the user is editing mid-line so the rendering stays sane.
	atEnd := c.Cursor >= lineedit.RuneLen(c.Buffer)
	if c.Suggestion != "" && atEnd {
		ghost := strings.TrimPrefix(c.Suggestion, strings.ToLower(c.Buffer))
		if ghost != "" {
			body += s.CommandGhost.Render(ghost)
		}
	}
	if c.Error != "" {
		body += "  " + s.StatusErr.Render(c.Error)
	}

	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(s.Palette.Accent).
		Padding(0, 1)
	if width > 4 {
		// subtract two side borders + 2 cols of padding from inner width.
		box = box.Width(width - 2)
	}
	return box.Render(body)
}

// renderBufferWithCursor draws the buffer with a reverse-video block cursor at
// the rune offset cur. When cur sits past the last rune, a trailing space
// stands in for "past end".
func renderBufferWithCursor(s theme.Styles, buffer string, cur int) string {
	runes := []rune(buffer)
	if cur < 0 {
		cur = 0
	}
	if cur > len(runes) {
		cur = len(runes)
	}
	before := string(runes[:cur])
	var underCursor, after string
	if cur >= len(runes) {
		underCursor = " "
	} else {
		underCursor = string(runes[cur])
		after = string(runes[cur+1:])
	}
	return s.Command.Render(before) + s.Cursor.Render(underCursor) + s.Command.Render(after)
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Round(time.Second).Seconds()))
	case d < time.Hour:
		m := int(d / time.Minute)
		s := int((d % time.Minute) / time.Second)
		if s == 0 {
			return fmt.Sprintf("%dm", m)
		}
		return fmt.Sprintf("%dm%ds", m, s)
	default:
		h := int(d / time.Hour)
		m := int((d % time.Hour) / time.Minute)
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%dm", h, m)
	}
}
