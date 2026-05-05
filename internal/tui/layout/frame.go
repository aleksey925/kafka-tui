package layout

import (
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// FrameOpts describes the frame chrome wrapped around a screen body.
type FrameOpts struct {
	// Width and Height are the outer dimensions (including the border). The
	// inner area is Width-2 by Height-2.
	Width, Height int
	// Title is centered inside the top border (k9s-style single title). Empty
	// hides the slot.
	Title string
	// Focused renders the border with the focus color rather than the muted
	// default.
	Focused bool
}

// FrameSidePadding is the number of blank columns inserted between each
// vertical border (`│`) and the body content, on each side. Hosts sizing
// screens for the inner area must subtract `2*FrameSidePadding + 2` from
// the terminal width to account for borders + padding. Matches k9s'
// `SetBorderPadding(0,0,1,1)`. Any visual gap between the frame border
// and the terminal edge is the host's responsibility, not this package.
const FrameSidePadding = 1

// Frame wraps body in a rounded box with the title embedded in the top
// border. Body is split into lines and padded/truncated to fit the inner
// rectangle (terminal-width minus borders minus side padding).
//
// Geometry: top + (Height-2) body lines + bottom. Each body line is
// '│' + spaces(FrameSidePadding) + content + spaces(FrameSidePadding) +
// '│'. Lines exceeding the inner width are left as-is (caller is expected
// to size the body to fit).
func Frame(s theme.Styles, opts FrameOpts, body string) string {
	if opts.Width < 4+2*FrameSidePadding || opts.Height < 3 {
		return body
	}
	innerEdge := opts.Width - 2             // space between vertical borders
	inner := innerEdge - 2*FrameSidePadding // usable cell width
	bodyH := opts.Height - 2

	border := frameBorderStyle(s, opts.Focused)
	// render the corners as standalone styled segments — wrapping the
	// whole "╭…╮" string in a single border.Render leaves the right
	// corner unstyled because frameTopLine emits internal resets
	// (\x1b[m) for its title segment, which collapse the outer style
	// before it can paint the closing corner.
	top := border.Render("╭") + frameTopLine(s, opts.Title, innerEdge) + border.Render("╮")
	bottom := border.Render("╰" + strings.Repeat("─", innerEdge) + "╯")

	lines := strings.Split(body, "\n")
	out := make([]string, 0, opts.Height)
	out = append(out, top)
	side := border.Render("│")
	pad := strings.Repeat(" ", FrameSidePadding)
	for i := range bodyH {
		var content string
		if i < len(lines) {
			content = lines[i]
		}
		out = append(out, side+pad+padOrTruncate(content, inner)+pad+side)
	}
	out = append(out, bottom)
	return strings.Join(out, "\n")
}

func frameBorderStyle(s theme.Styles, focused bool) lipgloss.Style {
	if focused {
		return lipgloss.NewStyle().Foreground(s.Palette.Accent)
	}
	return lipgloss.NewStyle().Foreground(s.Palette.Muted)
}

// frameTopLine builds the inner part of the top border line (between the
// two corner runes), centering the title inside continuous dashes:
// "──── <title> ────". When the title doesn't fit, it's dropped and the
// border collapses to a plain dash run.
func frameTopLine(s theme.Styles, title string, inner int) string {
	border := lipgloss.NewStyle().Foreground(s.Palette.Muted)
	if title == "" {
		return border.Render(strings.Repeat("─", inner))
	}
	seg := " " + title + " "
	segW := lipgloss.Width(seg)
	if segW+2 > inner {
		return border.Render(strings.Repeat("─", inner))
	}
	left := (inner - segW) / 2
	right := inner - segW - left
	return border.Render(strings.Repeat("─", left)) +
		s.HelpTitle.Render(seg) +
		border.Render(strings.Repeat("─", right))
}

// padOrTruncate fits content into width: pads with spaces when shorter,
// returns as-is when wider (truncating styled output safely is non-trivial
// and screens are responsible for sizing themselves).
func padOrTruncate(content string, width int) string {
	w := lipgloss.Width(content)
	if w >= width {
		return content
	}
	return content + strings.Repeat(" ", width-w)
}
