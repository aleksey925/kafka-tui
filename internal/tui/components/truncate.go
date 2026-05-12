package components

import "github.com/charmbracelet/x/ansi"

// TruncateText returns text trimmed to fit within maxWidth visual cells,
// appending a single "…" when the original exceeds the budget. It is
// ANSI-aware: styling sequences in the input are preserved and don't count
// toward the width budget. When maxWidth is 1 the result is just the
// ellipsis; when maxWidth <= 0 the result is empty.
//
// This is the canonical "fit a single-line value into a bounded column"
// helper. Table cells, frame chrome, message-list previews and any other
// place that drops overflow on a single row should route through it so the
// ellipsis glyph, width semantics and ANSI handling stay uniform — the
// horizontal counterpart of [Viewport]'s vertical scroll for multi-line
// content.
func TruncateText(text string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	if ansi.StringWidth(text) <= maxWidth {
		return text
	}
	return ansi.Truncate(text, maxWidth, "…")
}
