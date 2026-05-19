package components

import (
	"strings"

	"charm.land/lipgloss/v2"
)

// HighlightRow paints line with style's background, k9s-style: right-pads
// to width with spaces so the highlight spans the full row, then wraps
// the result with style.
//
// The bg SGR is re-applied after every inner SGR reset emitted by
// lipgloss, so per-cell foreground styling (cluster swatches, group
// state, etc.) does not punch holes in the highlight. lipgloss v2 emits
// "\x1b[m" (short form) as its only reset; "\x1b[0m" is also handled for
// robustness against pre-styled content originating outside lipgloss.
// Compound resets like "\x1b[0;1m" or bare bg-default "\x1b[49m" are not
// handled — lipgloss does not produce them today.
//
// width <= 0 disables padding (line is highlighted at its natural width).
// A style that emits no opening SGR (no foreground / background rules)
// falls back to style.Render with no manual wrapping, so a misconfigured
// caller does not corrupt the line — but production callers always pass
// a bg-bearing style; the no-SGR branch is a safety net, not a feature.
//
// The opening SGR is extracted by probing the style with a sentinel
// character and slicing what lipgloss put in front of it. The sentinel
// is SOH (0x01) rather than NUL (0x00) so we don't tempt future
// C-string-aware sanitizers; the regression test
// TestTable_CursorRowHighlightSurvivesNestedANSIResets catches silent
// breakage if the probe ever stops returning the prefix.
func HighlightRow(style lipgloss.Style, width int, line string) string {
	if width > 0 {
		if w := lipgloss.Width(line); w < width {
			line += strings.Repeat(" ", width-w)
		}
	}
	const sentinel = "\x01"
	probe := style.Render(sentinel)
	idx := strings.Index(probe, sentinel)
	if idx <= 0 {
		return style.Render(line)
	}
	openSGR := probe[:idx]
	line = strings.ReplaceAll(line, "\x1b[0m", "\x1b[0m"+openSGR)
	line = strings.ReplaceAll(line, "\x1b[m", "\x1b[m"+openSGR)
	return openSGR + line + "\x1b[0m"
}
