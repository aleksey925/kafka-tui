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
	// Title appears in the top border, left-aligned. Empty hides the slot.
	Title string
	// Breadcrumb appears in the top border, right-aligned. Empty hides it.
	Breadcrumb string
	// Focused renders the border with the focus color rather than the muted
	// default.
	Focused bool
}

// Frame wraps body in a rounded box with title and breadcrumb embedded in
// the top border. Body is split into lines and padded/truncated to fit the
// inner rectangle.
//
// Geometry: top + (Height-2) body lines + bottom. Each body line is
// '│' + content padded to (Width-2) + '│'. Lines exceeding the inner width
// are left as-is (caller is expected to size the body to fit).
func Frame(s theme.Styles, opts FrameOpts, body string) string {
	if opts.Width < 4 || opts.Height < 3 {
		return body
	}
	inner := opts.Width - 2
	bodyH := opts.Height - 2

	border := frameBorderStyle(s, opts.Focused)
	top := border.Render("╭" + frameTopLine(s, opts.Title, opts.Breadcrumb, inner) + "╮")
	bottom := border.Render("╰" + strings.Repeat("─", inner) + "╯")

	lines := strings.Split(body, "\n")
	out := make([]string, 0, opts.Height)
	out = append(out, top)
	side := border.Render("│")
	for i := range bodyH {
		var content string
		if i < len(lines) {
			content = lines[i]
		}
		out = append(out, side+padOrTruncate(content, inner)+side)
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

// frameTopLine assembles the inner part of the top border line (between the
// two corner runes). Layout: "─ <title> ─...─ <breadcrumb> ─". When there
// isn't enough room for either decoration, drops it gracefully.
func frameTopLine(s theme.Styles, title, breadcrumb string, inner int) string {
	border := lipgloss.NewStyle().Foreground(s.Palette.Muted)
	titleStyle := s.HelpTitle
	bcStyle := s.StatusInfo

	// Always one leading dash + corner gives room.
	left := border.Render("─")
	leftWidth := 1
	if title != "" {
		seg := " " + title + " "
		if leftWidth+lipgloss.Width(seg)+1 <= inner {
			left = border.Render("─") + titleStyle.Render(seg)
			leftWidth = 1 + lipgloss.Width(seg)
		}
	}

	right := border.Render("─")
	rightWidth := 1
	if breadcrumb != "" {
		seg := " " + breadcrumb + " "
		if leftWidth+lipgloss.Width(seg)+rightWidth+1 <= inner {
			right = bcStyle.Render(seg) + border.Render("─")
			rightWidth = lipgloss.Width(seg) + 1
		}
	}

	mid := max(inner-leftWidth-rightWidth, 0)
	return left + border.Render(strings.Repeat("─", mid)) + right
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
