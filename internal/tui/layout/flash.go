package layout

import (
	"image/color"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// FlashLevel categorizes a flash message for color and tag selection.
type FlashLevel int

const (
	FlashInfo FlashLevel = iota
	FlashOK
	FlashWarn
	FlashErr
)

// Flash is the global one-line bar at the bottom of the host view. Empty
// Text yields a blank fixed-height line so body geometry stays stable.
type Flash struct {
	Text  string
	Level FlashLevel
}

// FlashLine renders the bar. width<=0 returns the bare body; otherwise the
// line is right-padded to width.
func FlashLine(s theme.Styles, f Flash, width int) string {
	body := ""
	if f.Text != "" {
		tag, c := flashStyle(s, f.Level)
		tagStyle := lipgloss.NewStyle().Foreground(c).Bold(true)
		body = tagStyle.Render(tag) + " " + s.Command.Render(f.Text)
	}
	if width <= 0 {
		return body
	}
	pad := width - lipgloss.Width(body)
	if pad <= 0 {
		return body
	}
	return body + strings.Repeat(" ", pad)
}

func flashStyle(s theme.Styles, lvl FlashLevel) (string, color.Color) {
	switch lvl {
	case FlashOK:
		return "[OK]", s.Palette.StatusOK
	case FlashWarn:
		return "[WARN]", s.Palette.StatusWarn
	case FlashErr:
		return "[ERR]", s.Palette.StatusError
	default:
		return "[INFO]", s.Palette.Foreground
	}
}
