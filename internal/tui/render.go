// Rendering — the host's Bubble Tea View(), the chrome composition
// (header / command bar / body frame / flash bar), and the help
// overlay. Includes flashTickMsg + promoteFlash that feed the global
// flash bar from each screen's toast queue.

package tui

import (
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

// View implements [tea.Model].
func (m *Model) View() tea.View {
	v := tea.NewView(m.render())
	v.AltScreen = true
	return v
}

// Render returns the model's full content (exported for tests; matches what
// View() embeds).
func (m *Model) Render() string {
	return m.render()
}

func (m *Model) render() string {
	if m.mode == ModeHelp {
		return indentLines(m.renderHelp(), screenSideMargin)
	}

	w := m.screenWidth()
	header := layout.Header(
		m.styles,
		m.header,
		m.statusForRender(),
		m.activeKeyHints(),
		layout.Build{Version: m.build.Version, Commit: m.build.Commit},
		w,
	)

	bar := m.command
	if m.mode == ModeSearch {
		bar = m.search
	}
	cmdBox := layout.CommandLine(m.styles, bar, w)

	body := m.renderBody()
	flash := layout.FlashLine(m.styles, m.flash, w)

	parts := []string{header}
	if cmdBox != "" {
		parts = append(parts, cmdBox)
	}
	parts = append(parts, body, flash)
	return indentLines(strings.Join(parts, "\n"), screenSideMargin)
}

// indentLines prefixes every line of s with `n` spaces. Used to apply
// the outer horizontal screen margin without forcing every chrome
// component to render its own padding.
func indentLines(s string, n int) string {
	if n <= 0 {
		return s
	}
	pad := strings.Repeat(" ", n)
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = pad + l
	}
	return strings.Join(lines, "\n")
}

// flashTickMsg triggers a re-render so a non-sticky toast that has just
// expired clears off the global flash bar without waiting for user input.
type flashTickMsg struct{}

// promoteFlash refreshes the global flash bar from the active screen's
// latest live toast. Returns a tea.Cmd that re-pumps the flash on the
// toast's expiry (so the bar clears automatically), or nil for sticky /
// no-op cases.
func (m *Model) promoteFlash() tea.Cmd {
	if m.active == nil {
		return nil
	}
	t, ok := screenLatestFlash(m.active)
	if !ok {
		// nothing live → clear the bar so a stale message doesn't linger.
		m.flash = layout.Flash{}
		return nil
	}
	if !t.CreatedAt.After(m.flashSeenAt) {
		return nil
	}
	m.flash = flashFromToast(t)
	m.flashSeenAt = t.CreatedAt
	if t.Sticky() {
		return nil
	}
	return tea.Tick(t.Lifetime, func(time.Time) tea.Msg { return flashTickMsg{} })
}

// flashFromToast translates a components.Toast (used by screens) into the
// chrome-side layout.Flash type. layout/ doesn't import components/ to keep
// it dependency-free for theming.
func flashFromToast(t components.Toast) layout.Flash {
	level := layout.FlashInfo
	switch t.Level {
	case components.ToastSuccess:
		level = layout.FlashOK
	case components.ToastWarning:
		level = layout.FlashWarn
	case components.ToastError:
		level = layout.FlashErr
	case components.ToastInfo:
		level = layout.FlashInfo
	}
	return layout.Flash{Text: t.Message, Level: level}
}

// Flash returns the current flash payload (for tests).
func (m *Model) Flash() layout.Flash { return m.flash }

func (m *Model) statusForRender() layout.StatusInfo {
	s := m.status
	// Now is always the live wall clock so the chrome's "X ago" counter
	// advances on every re-render even between refresh ticks.
	s.Now = m.now()
	if m.active != nil {
		s.LastRefresh = screenLastRefresh(m.active)
	}
	return s
}

// renderBody dispatches to the active screen and wraps the result in the
// rounded body frame with the screen's title and breadcrumb in the top
// border. Falls back to a placeholder when no instance is available (test
// path or unwired bootstrap).
func (m *Model) renderBody() string {
	active := m.router.Active()
	if active == "" {
		return m.frameOrRaw(m.styles.StatusInfo.Render("(no screen active)"), "", "")
	}
	if v := m.activeView(); v != "" {
		title, bc := "", ""
		if m.active != nil {
			title, bc = m.active.Title(), m.active.Breadcrumb()
		}
		return m.frameOrRaw(v, title, bc)
	}
	return m.frameOrRaw(
		m.styles.StatusInfo.Render(string(active)+" — coming soon"),
		string(active), "",
	)
}

// frameOrRaw wraps body in the rounded frame when geometry is known; tests
// that don't supply a window size receive the raw body unchanged. The title
// is rendered centered in the top border (k9s-style); breadcrumb context,
// if any, is folded into the title by the screen.
func (m *Model) frameOrRaw(body, title, breadcrumb string) string {
	w := m.screenWidth()
	if w <= 4 || m.bodyHeight() < 1 {
		return body
	}
	combined := title
	if breadcrumb != "" {
		if combined != "" {
			combined += "  ·  " + breadcrumb
		} else {
			combined = breadcrumb
		}
	}
	return layout.Frame(m.styles, layout.FrameOpts{
		Width:  w,
		Height: m.bodyHeight() + frameInset,
		Title:  combined,
	}, body)
}

// renderHelp draws the full-screen `?` overlay. The screen-specific
// sections come first (so the user sees what they care about right
// away); the host-owned General/Navigation/Commands blocks follow.
// The whole overlay is wrapped in [layout.Frame] so it gets the same
// rounded border + side padding as the regular screen body — matches
// k9s' bordered help view (`SetBorder(true)` +
// `SetBorderPadding(0,0,1,1)`) and prevents content from touching the
// terminal edge.
func (m *Model) renderHelp() string {
	var sections []help.Section
	if m.active != nil {
		sections = append(sections, screenHelpSections(m.active)...)
	}
	sections = append(sections, help.GeneralSections()...)

	title := "Help"
	if m.active != nil {
		if name := m.active.Title(); name != "" {
			title = "Help  ·  " + name
		}
	}

	width := m.screenWidth()
	if width <= 0 {
		width = 80
	}
	height := m.height
	if height <= 0 {
		height = 24
	}

	innerW := width - 2 - 2*layout.FrameSidePadding
	innerH := height - 2
	if innerW < 1 {
		innerW = 1
	}
	if innerH < 1 {
		innerH = 1
	}
	body := help.Render(help.Options{
		Width:    innerW,
		Height:   innerH,
		Sections: sections,
		Footer:   m.build.Display(),
		Styles:   m.styles,
	})
	return layout.Frame(m.styles, layout.FrameOpts{
		Width:  width,
		Height: height,
		Title:  title,
	}, body)
}
