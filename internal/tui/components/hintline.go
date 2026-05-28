package components

import (
	"strings"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Hint is one entry in an inline hint footer — see § "Inline hint
// footer" in CLAUDE.md for the contract.
type Hint struct {
	Key   string
	Label string
}

// HintLine is the single renderer for every `<key> <label>` hint
// surface in the app. See § "Inline hint footer" in CLAUDE.md.
func HintLine(s theme.Styles, hints ...Hint) string {
	if len(hints) == 0 {
		return ""
	}
	parts := make([]string, 0, len(hints))
	for _, h := range hints {
		switch {
		case h.Key == "" && h.Label == "":
			continue
		case h.Key == "":
			parts = append(parts, s.HintLabel.Render(h.Label))
		case h.Label == "":
			parts = append(parts, s.HintKey.Render(h.Key))
		default:
			parts = append(parts, s.HintKey.Render(h.Key)+" "+s.HintLabel.Render(h.Label))
		}
	}
	return strings.Join(parts, "  ")
}
