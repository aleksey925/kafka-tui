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
		return m.renderHelp()
	}

	header := layout.Header(
		m.styles,
		m.header,
		m.statusForRender(),
		m.activeKeyHints(),
		layout.Build{Version: m.build.Version, Commit: m.build.Commit},
		m.width,
	)

	bar := m.command
	if m.mode == ModeSearch {
		bar = m.search
	}
	cmdBox := layout.CommandLine(m.styles, bar, m.width)

	body := m.renderBody()
	flash := layout.FlashLine(m.styles, m.flash, m.width)

	parts := []string{header}
	if cmdBox != "" {
		parts = append(parts, cmdBox)
	}
	parts = append(parts, body, flash)
	return strings.Join(parts, "\n")
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
	if m.width <= 4 || m.bodyHeight() < 1 {
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
		Width:  m.width,
		Height: m.bodyHeight() + frameInset,
		Title:  combined,
	}, body)
}

// renderHelp draws the full-screen `?` overlay. The screen-specific
// sections come first (so the user sees what they care about right
// away); the host-owned General/Commands/Navigation blocks follow.
func (m *Model) renderHelp() string {
	var sections []help.Section
	if m.active != nil {
		sections = append(sections, screenHelpSections(m.active)...)
	}
	sections = append(sections, help.GeneralSections()...)

	screenName := ""
	if m.active != nil {
		screenName = m.active.Title()
	}

	width := m.width
	if width <= 0 {
		width = 80
	}
	return help.Render(help.Options{
		Width:    width,
		Height:   m.height,
		Screen:   screenName,
		Sections: sections,
		Footer:   m.build.Display(),
		Styles:   m.styles,
	})
}
