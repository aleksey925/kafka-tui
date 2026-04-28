package components

import (
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// HelpSection groups related key hints under a title (specification §7.9).
type HelpSection struct {
	Title string
	Keys  []layout.KeyHint
}

// Help is a full-screen, sectioned help overlay. Sections are rendered in
// the supplied order; the version string sits in the bottom-right corner.
//
// Search inside help is intentionally NOT implemented (§7.9 marks it as
// deferred for a later iteration).
type Help struct {
	Sections []HelpSection
	Version  string

	styles theme.Styles
}

// NewHelp constructs a help overlay.
func NewHelp(sections []HelpSection, version string, opts ...HelpOption) *Help {
	h := &Help{
		Sections: sections,
		Version:  version,
		styles:   theme.DefaultStyles(),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// HelpOption configures Help.
type HelpOption func(*Help)

// WithHelpStyles overrides the theme styles.
func WithHelpStyles(s theme.Styles) HelpOption {
	return func(h *Help) { h.styles = s }
}

// View renders the help overlay sized to width × height. Pass 0 / 0 for an
// untruncated, naturally-sized rendering (used by tests).
func (h *Help) View(width, height int) string {
	body := h.renderBody()
	footer := h.styles.StatusInfo.Render(h.Version)

	if width > 0 && h.Version != "" {
		footer = lipgloss.PlaceHorizontal(width, lipgloss.Right, footer)
	}

	out := body
	if h.Version != "" {
		out = body + "\n\n" + footer
	}
	if height > 0 {
		out = lipgloss.PlaceVertical(height, lipgloss.Top, out)
	}
	return out
}

func (h *Help) renderBody() string {
	parts := []string{h.styles.HelpTitle.Render("Help")}
	for _, sec := range h.Sections {
		parts = append(parts, "")
		if sec.Title != "" {
			parts = append(parts, h.styles.HelpTitle.Render(sec.Title))
		}
		parts = append(parts, layout.KeyHints(h.styles, sec.Keys))
	}
	return strings.Join(parts, "\n")
}
