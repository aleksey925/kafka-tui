// Package theme provides the color palette and reusable styles for the
// kafka-tui interface (Claude Code dark theme).
package theme

import (
	"image/color"

	"charm.land/lipgloss/v2"
)

// Cluster color identifiers accepted by clusters.yaml. These are the only
// values valid for the color swatch in headers and the cluster list.
const (
	ClusterRed    = "red"
	ClusterYellow = "yellow"
	ClusterGreen  = "green"
	ClusterGray   = "gray"
	ClusterWhite  = "white"
)

// AllowedClusterColors lists every cluster color the application accepts.
// It is exposed for validation and tests.
var AllowedClusterColors = []string{
	ClusterRed,
	ClusterYellow,
	ClusterGreen,
	ClusterGray,
	ClusterWhite,
}

// Palette describes the colors used by the dark theme.
type Palette struct {
	Background color.Color
	Foreground color.Color
	Muted      color.Color
	Subtle     color.Color
	Accent     color.Color

	Red    color.Color
	Yellow color.Color
	Green  color.Color
	Gray   color.Color
	White  color.Color

	StatusOK    color.Color
	StatusWarn  color.Color
	StatusError color.Color
}

// Default returns the default Claude Code dark palette.
func Default() Palette {
	return Palette{
		Background: lipgloss.Color("#1e1e1e"),
		Foreground: lipgloss.Color("#e8e8e8"),
		Muted:      lipgloss.Color("#7a7a7a"),
		Subtle:     lipgloss.Color("#3a3a3a"),
		Accent:     lipgloss.Color("#d18a45"),

		Red:    lipgloss.Color("#e06c75"),
		Yellow: lipgloss.Color("#e5c07b"),
		Green:  lipgloss.Color("#98c379"),
		Gray:   lipgloss.Color("#7a7a7a"),
		White:  lipgloss.Color("#e8e8e8"),

		StatusOK:    lipgloss.Color("#98c379"),
		StatusWarn:  lipgloss.Color("#e5c07b"),
		StatusError: lipgloss.Color("#e06c75"),
	}
}

// ClusterColor returns the color configured for the cluster swatch. Unknown
// values fall back to the default foreground.
func (p Palette) ClusterColor(name string) color.Color {
	switch name {
	case ClusterRed:
		return p.Red
	case ClusterYellow:
		return p.Yellow
	case ClusterGreen:
		return p.Green
	case ClusterGray:
		return p.Gray
	case ClusterWhite:
		return p.White
	default:
		return p.Foreground
	}
}

// Styles bundles the reusable lip-gloss styles shared by every screen.
type Styles struct {
	Palette Palette

	Header       lipgloss.Style
	HeaderBar    lipgloss.Style
	Cluster      lipgloss.Style
	ReadOnly     lipgloss.Style
	StatusBar    lipgloss.Style
	StatusInfo   lipgloss.Style
	StatusWarn   lipgloss.Style
	StatusErr    lipgloss.Style
	KeyHints     lipgloss.Style
	HintKey      lipgloss.Style
	HintLabel    lipgloss.Style
	Command      lipgloss.Style
	CommandHL    lipgloss.Style
	CommandGhost lipgloss.Style
	Toast        lipgloss.Style
	HelpTitle    lipgloss.Style
}

// New builds the default Styles using the supplied palette.
func New(p Palette) Styles {
	return Styles{
		Palette:      p,
		Header:       lipgloss.NewStyle().Foreground(p.Foreground).Bold(true),
		HeaderBar:    lipgloss.NewStyle().Foreground(p.Foreground).Background(p.Subtle).Padding(0, 1),
		Cluster:      lipgloss.NewStyle().Bold(true),
		ReadOnly:     lipgloss.NewStyle().Foreground(p.StatusWarn).Bold(true),
		StatusBar:    lipgloss.NewStyle().Foreground(p.Muted),
		StatusInfo:   lipgloss.NewStyle().Foreground(p.Muted),
		StatusWarn:   lipgloss.NewStyle().Foreground(p.StatusWarn),
		StatusErr:    lipgloss.NewStyle().Foreground(p.StatusError),
		KeyHints:     lipgloss.NewStyle().Foreground(p.Muted),
		HintKey:      lipgloss.NewStyle().Foreground(p.Accent).Bold(true),
		HintLabel:    lipgloss.NewStyle().Foreground(p.Muted),
		Command:      lipgloss.NewStyle().Foreground(p.Foreground),
		CommandHL:    lipgloss.NewStyle().Foreground(p.Accent).Bold(true),
		CommandGhost: lipgloss.NewStyle().Foreground(p.Muted),
		Toast:        lipgloss.NewStyle().Foreground(p.Foreground).Background(p.Subtle).Padding(0, 1),
		HelpTitle:    lipgloss.NewStyle().Foreground(p.Accent).Bold(true),
	}
}

// DefaultStyles returns Styles built from the default palette.
func DefaultStyles() Styles {
	return New(Default())
}
