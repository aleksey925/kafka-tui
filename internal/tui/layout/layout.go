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
)

// HeaderInfo describes everything required to render the top bar.
type HeaderInfo struct {
	Cluster      string
	ClusterColor string
	ReadOnly     bool
	FromCLI      bool
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
	Active bool
	Prefix rune // ':' or '/'
	Buffer string
	Error  string // shown beneath the prompt when set
}

// Header renders the title bar `kafka-tui · <cluster> (<color>) [RO] (cli)`.
func Header(s theme.Styles, info HeaderInfo) string {
	parts := []string{s.Header.Render("kafka-tui")}

	if info.Cluster != "" {
		swatch := lipgloss.NewStyle().
			Foreground(s.Palette.ClusterColor(info.ClusterColor)).
			Render("●")
		clusterName := s.Cluster.Render(info.Cluster)
		colorTag := ""
		if info.ClusterColor != "" {
			colorTag = " " + s.StatusInfo.Render(fmt.Sprintf("(%s)", info.ClusterColor))
		}
		parts = append(parts, "·", swatch+" "+clusterName+colorTag)
	}
	if info.ReadOnly {
		parts = append(parts, s.ReadOnly.Render("[RO]"))
	}
	if info.FromCLI {
		parts = append(parts, s.StatusInfo.Render("(cli)"))
	}

	return strings.Join(parts, " ")
}

// Status renders the right-aligned refresh indicator.
//
// Examples:
//
//	"auto: 5s, refreshed 3s ago"
//	"manual"
//	"paused"
func Status(s theme.Styles, info StatusInfo) string {
	switch info.Mode {
	case RefreshOff:
		return ""
	case RefreshManual:
		return s.StatusInfo.Render("manual")
	case RefreshPaused:
		return s.StatusWarn.Render("paused")
	case RefreshAuto:
		body := "auto: " + formatDuration(info.Interval)
		if !info.LastRefresh.IsZero() {
			now := info.Now
			if now.IsZero() {
				now = time.Now()
			}
			elapsed := max(0, now.Sub(info.LastRefresh))
			body += ", refreshed " + formatDuration(elapsed.Round(time.Second)) + " ago"
		}
		return s.StatusInfo.Render(body)
	default:
		return ""
	}
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

// CommandLine renders the command bar prompt (or empty string when inactive).
func CommandLine(s theme.Styles, c CommandBar) string {
	if !c.Active {
		return ""
	}
	prefix := string(c.Prefix)
	body := s.CommandHL.Render(prefix) + s.Command.Render(c.Buffer)
	if c.Error != "" {
		body += "  " + s.StatusErr.Render(c.Error)
	}
	return body
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
