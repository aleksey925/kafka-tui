package tui

import (
	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/filterhistory"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

func (m *Model) handleKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch m.mode {
	case ModeCommand:
		return m.handleCommandKey(key)
	case ModeSearch:
		return m.handleSearchKey(key)
	case ModeHelp:
		return m.handleHelpKey(key)
	default:
		return m.handleNormalKey(key)
	}
}

// handleNormalKey runs the default-mode pipeline: ctrl+c always quits,
// raw-input screens get every key as a literal, then global shortcuts,
// then k9s-style filter clearing on esc/ctrl+u, then forward to the
// active screen with q/esc fallback only when nothing claimed the key.
func (m *Model) handleNormalKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	// ctrl+c is always global so the user can quit even from inside a form.
	if key.String() == "ctrl+c" {
		m.quit = true
		return m, tea.Quit
	}
	// raw-input screens (forms) get every key as a literal so global
	// shortcuts don't interfere with typing. See [RawInputs].
	if m.active != nil && screenWantsRawInput(m.active) {
		cmd := m.forwardToActive(key)
		routeCmd := m.routeActiveAction()
		return m, teaBatch(cmd, routeCmd)
	}
	if m.handleGlobalShortcut(key) {
		return m, nil
	}
	// when an overlay is open, esc belongs to it; capture the pre-state so
	// we can suppress the pop after the screen closes its overlay too.
	hadOverlay := m.active != nil && screenHasOverlay(m.active)
	if key.String() == "esc" && !hadOverlay && m.active != nil && screenActiveFilter(m.active) != "" {
		setScreenSearch(m.active, "")
	}
	// ctrl+u on an empty filter falls through (k9s' clearCmd passes the
	// event back when the buffer is inactive).
	if key.String() == "ctrl+u" && !hadOverlay && m.active != nil && screenSupportsSearch(m.active) && screenActiveFilter(m.active) != "" {
		setScreenSearch(m.active, "")
		return m, nil
	}
	cmd := m.forwardToActive(key)
	routeCmd := m.routeActiveAction()
	if cmd == nil && routeCmd == nil {
		if fbCmd, ok := m.handleQuitFallback(key, hadOverlay); ok {
			return m, fbCmd
		}
	}
	return m, teaBatch(cmd, routeCmd)
}

// handleGlobalShortcut runs the screen-agnostic shortcut switch
// (`:` / `/` / `?` / `ctrl+r`). Returns false when the key isn't one of those.
func (m *Model) handleGlobalShortcut(key tea.KeyPressMsg) bool {
	switch key.String() {
	case ":":
		m.mode = ModeCommand
		m.command = layout.CommandBar{Active: true, Prefix: ':'}
		m.applySize()
		return true
	case "/":
		// detail/form views have nothing to filter — swallow `/` instead
		// of opening an inert prompt.
		if m.active != nil && !screenSupportsSearch(m.active) {
			return true
		}
		m.openSearchPrompt()
		return true
	case "?":
		m.mode = ModeHelp
		return true
	case "ctrl+r":
		m.SetAutoRefresh(!m.autoRefresh)
		return true
	}
	return false
}

// handleQuitFallback decides what `q` / `esc` should do when the active
// screen returned no command and no Action. When hadOverlay is true, q/esc
// must NOT pop the screen — the user is inside an overlay/form.
func (m *Model) handleQuitFallback(key tea.KeyPressMsg, hadOverlay bool) (tea.Cmd, bool) {
	switch key.String() {
	case "q":
		if hadOverlay {
			return nil, true
		}
		// `q` quits at the root, otherwise pops a screen.
		if m.router.Depth() <= 1 {
			m.quit = true
			return tea.Quit, true
		}
		m.popScreen()
		return m.activeInit(), true
	case "esc":
		if hadOverlay {
			return nil, true
		}
		// at the root esc is a no-op so users don't quit by accident.
		// ctrl+c remains the unconditional exit.
		if m.router.Depth() > 1 {
			m.popScreen()
			return m.activeInit(), true
		}
		return nil, true
	}
	return nil, false
}

func (m *Model) handleCommandKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		m.mode = ModeNormal
		m.command = layout.CommandBar{}
		m.applySize()
		return m, nil
	case "enter":
		cmd, err := ParseCommand(m.command.Buffer)
		if err != nil {
			m.command.Error = err.Error()
			return m, nil
		}
		m.mode = ModeNormal
		m.command = layout.CommandBar{}
		m.applySize()
		next := m.replaceScreen(cmd.Screen, cmd.Arg)
		return m, next
	case "tab":
		if m.command.Suggestion != "" {
			m.command.Buffer = m.command.Suggestion
			m.command.Suggestion = ""
			m.command.Error = ""
		}
		return m, nil
	case "backspace":
		if n := len(m.command.Buffer); n > 0 {
			m.command.Buffer = m.command.Buffer[:n-1]
			m.command.Error = ""
		}
		m.command.Suggestion = CompletionSuggestion(m.command.Buffer)
		return m, nil
	default:
		if t := key.Text; t != "" {
			m.command.Buffer += t
			m.command.Error = ""
		}
		m.command.Suggestion = CompletionSuggestion(m.command.Buffer)
		return m, nil
	}
}

// openSearchPrompt switches into ModeSearch with an empty buffer; the
// applied filter is left untouched until the user types.
func (m *Model) openSearchPrompt() {
	m.mode = ModeSearch
	m.search = layout.CommandBar{Active: true, Prefix: '/'}
	m.refreshSearchSuggestions()
	m.applySize()
}

const searchHistoryCap = 20

// activeSearchHistory returns the filter-history bucket for the active
// screen, creating one lazily. Returns nil when no screen is active.
func (m *Model) activeSearchHistory() *filterhistory.History {
	if m.active == nil {
		return nil
	}
	id := m.router.Active()
	if id == "" {
		return nil
	}
	h, ok := m.searchHistories[id]
	if !ok {
		h = filterhistory.New(searchHistoryCap)
		m.searchHistories[id] = h
	}
	return h
}

// refreshSearchSuggestions recomputes the ghost suggestion list for the
// current buffer. Index resets to 0 (newest match) on every recompute.
func (m *Model) refreshSearchSuggestions() {
	hist := m.activeSearchHistory()
	if hist == nil {
		m.searchSuggestions = nil
		m.searchSuggestionIdx = -1
		m.search.Suggestion = ""
		return
	}
	m.searchSuggestions = hist.Matches(m.search.Buffer)
	if len(m.searchSuggestions) == 0 {
		m.searchSuggestionIdx = -1
		m.search.Suggestion = ""
		return
	}
	m.searchSuggestionIdx = 0
	m.search.Suggestion = m.searchSuggestions[0]
}

// closeSearchPrompt drops transient prompt state without touching the
// screen's applied filter — Esc/Enter handle that separately.
func (m *Model) closeSearchPrompt() {
	m.mode = ModeNormal
	m.search = layout.CommandBar{}
	m.searchSuggestions = nil
	m.searchSuggestionIdx = -1
	m.applySize()
}

// handleSearchKey runs the live filter prompt:
//   - esc            clears the filter and closes (no history push).
//   - enter / ctrl+e commits the buffer to history; live filter stays applied.
//   - tab / right / ctrl+f promotes the current ghost suggestion.
//   - up / down      cycle through suggestion matches (wraps).
//   - ctrl+u / ctrl+w wipe the buffer.
//   - backspace / delete drop the last rune.
func (m *Model) handleSearchKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		if m.active != nil {
			setScreenSearch(m.active, "")
		}
		m.closeSearchPrompt()
		return m, nil
	case "enter", "ctrl+e":
		if buf := m.search.Buffer; buf != "" {
			if hist := m.activeSearchHistory(); hist != nil {
				hist.Push(buf)
			}
		}
		m.closeSearchPrompt()
		return m, nil
	case "tab", "right", "ctrl+f":
		if m.search.Suggestion == "" {
			return m, nil
		}
		m.search.Buffer = m.search.Suggestion
		m.searchSuggestions = nil
		m.searchSuggestionIdx = -1
		m.search.Suggestion = ""
		if m.active != nil {
			setScreenSearch(m.active, m.search.Buffer)
		}
		return m, nil
	case "up":
		m.cycleSearchSuggestion(1)
		return m, nil
	case "down":
		m.cycleSearchSuggestion(-1)
		return m, nil
	case "ctrl+u", "ctrl+w":
		m.search.Buffer = ""
		if m.active != nil {
			setScreenSearch(m.active, "")
		}
		m.refreshSearchSuggestions()
		return m, nil
	case "backspace", "delete":
		if n := len(m.search.Buffer); n > 0 {
			m.search.Buffer = m.search.Buffer[:n-1]
		}
		if m.active != nil {
			setScreenSearch(m.active, m.search.Buffer)
		}
		m.refreshSearchSuggestions()
		return m, nil
	default:
		if t := key.Text; t != "" {
			m.search.Buffer += t
			if m.active != nil {
				setScreenSearch(m.active, m.search.Buffer)
			}
			m.refreshSearchSuggestions()
		}
		return m, nil
	}
}

// cycleSearchSuggestion advances the suggestion index by step
// (+1 = next older, -1 = next newer, both wrap).
func (m *Model) cycleSearchSuggestion(step int) {
	n := len(m.searchSuggestions)
	if n == 0 {
		return
	}
	idx := m.searchSuggestionIdx + step
	idx %= n
	if idx < 0 {
		idx += n
	}
	m.searchSuggestionIdx = idx
	m.search.Suggestion = m.searchSuggestions[idx]
}

// handleHelpKey closes the overlay on documented exit keys; everything
// else is silently swallowed.
func (m *Model) handleHelpKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc", "q", "?":
		m.mode = ModeNormal
		return m, nil
	}
	return m, nil
}
