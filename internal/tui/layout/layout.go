// Package layout renders the persistent chrome around every screen: the
// global header (cluster identity), the command bar, the status bar (refresh
// state), and the bottom-row key hints.
package layout

import (
	"fmt"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// RefreshMode mirrors the high-level state of the auto-refresh subsystem.
type RefreshMode int

const (
	// RefreshOff: the active screen does not auto-refresh.
	RefreshOff RefreshMode = iota
	// RefreshAuto: the active screen polls on a fixed interval.
	RefreshAuto
	// RefreshManual: the user disabled auto-refresh for this screen.
	RefreshManual
	// RefreshPaused: a modal/search is open and refresh is temporarily paused.
	RefreshPaused
	// RefreshOnEdit: the screen reloads itself in response to filesystem
	// events (no periodic poll). Used by the clusters screen which wires
	// into config.Watcher.
	RefreshOnEdit
	// RefreshNotApplicable: the screen is conceptually static (e.g. a
	// single-message detail view, a form, a one-shot snapshot) — refresh
	// just doesn't apply. Rendered as a dash so the user understands the
	// row exists but isn't relevant here, vs RefreshOff which means
	// "supported but turned off".
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
	// Filter, when non-empty, surfaces the active screen-level filter
	// (e.g. the open `/` search buffer) in the cluster info pane.
	Filter string
}

// StatusInfo describes the right-aligned status block.
type StatusInfo struct {
	Mode        RefreshMode
	Interval    time.Duration
	LastRefresh time.Time // zero value means never
	Now         time.Time // injected for deterministic rendering
}

// KeyHint is one entry in the bottom hints bar (e.g. `?` → `help`).
type KeyHint struct {
	Key   string
	Label string
}

// CommandBar is the optional prompt that appears when the user types `:` or
// `/`. Visible only when Active is true.
type CommandBar struct {
	Active     bool
	Prefix     rune // ':' or '/'
	Buffer     string
	Suggestion string // ghost text shown after the buffer (tab to accept)
	Error      string // shown beneath the prompt when set
}

// HeaderRows is the fixed height of the multi-pane header block. Hosts
// reserve this many rows above the body frame.
const HeaderRows = 5

// Build is the binary's identity surfaced in the header's right pane.
type Build struct {
	Version string
	Commit  string
}

// Header renders the k9s-style three-pane header:
//
//	┌──────────────────┬──────────────────────────────┬──────────────┐
//	│ ClusterInfo k:v  │  Menu <key>  Description     │  kafka-tui   │
//	│                  │                              │  v0.4.2      │
//	└──────────────────┴──────────────────────────────┴──────────────┘
//
// The block is exactly [HeaderRows] tall so the body geometry stays stable.
func Header(s theme.Styles, info HeaderInfo, status StatusInfo, hints []KeyHint, build Build, width int) string {
	if width < 40 {
		// fallback for very narrow terminals: a single compact line.
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
	for _, r := range rows {
		key := keyStyle.Render(padRight(r.key+":", 9))
		lines = append(lines, padLine(" "+key+" "+r.val, width))
	}
	for len(lines) < HeaderRows {
		lines = append(lines, padLine("", width))
	}
	return strings.Join(lines[:HeaderRows], "\n")
}

func refreshLabel(s theme.Styles, status StatusInfo) string {
	switch status.Mode {
	case RefreshAuto:
		body := "auto " + formatDuration(status.Interval)
		if !status.LastRefresh.IsZero() && !status.Now.IsZero() {
			elapsed := max(0, status.Now.Sub(status.LastRefresh))
			body += " · " + formatDuration(elapsed.Round(time.Second)) + " ago"
		}
		return body
	case RefreshManual:
		return "manual"
	case RefreshPaused:
		return s.StatusWarn.Render("paused")
	case RefreshOnEdit:
		body := "on edit"
		if !status.LastRefresh.IsZero() && !status.Now.IsZero() {
			elapsed := max(0, status.Now.Sub(status.LastRefresh))
			body += " · " + formatDuration(elapsed.Round(time.Second)) + " ago"
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

func renderMenu(s theme.Styles, hints []KeyHint, width int) string {
	if len(hints) == 0 {
		hints = []KeyHint{
			{Key: ":", Label: "command"},
			{Key: "?", Label: "help"},
			{Key: "q", Label: "quit"},
		}
	}
	// pack into 2 columns (≤ 12 hints = 6 rows). For small terminals the
	// second column wraps to a single column.
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
			// only emit a cell if it falls inside its own column slot —
			// otherwise the row leaks the next column's content (a hint
			// shown twice). Empty rows beyond rowsN stay blank padding.
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

// KeyHints renders the bottom row of `key label` pairs joined by spaces.
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
// active (top border + body + bottom border). Hosts always reserve this many
// rows so the body geometry stays stable when the prompt opens.
const CommandRows = 3

// CommandLine renders the command bar prompt as a focused, bordered single-
// row box, or returns an empty string when the bar is inactive. Hosts must
// account for the `CommandRows` height only when [CommandBar.Active] is
// true so the body uses the full screen otherwise.
func CommandLine(s theme.Styles, c CommandBar, width int) string {
	if !c.Active {
		return ""
	}
	prefix := string(c.Prefix)
	body := s.CommandHL.Render(prefix) + " " + s.Command.Render(c.Buffer)
	if c.Suggestion != "" {
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
		// account for the box's two side borders + 2 cols of padding when
		// sizing the inner content so it spans the terminal width.
		box = box.Width(width - 2)
	}
	return box.Render(body)
}

// formatDuration prints durations like "5s", "30s", "2m" — short and readable.
// Designed for human-friendly UI strings, not log timestamps.
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
