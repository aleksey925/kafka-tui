package tui

import (
	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/filterhistory"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/lineedit"
)

func (m *Model) handleKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	// ctrl+c is the unconditional global exit per CLAUDE.md — it must work
	// from any mode, including the `:` / `/` / `?` overlays, otherwise the
	// user has no way to bail out of a prompt without typing esc first.
	if key.String() == "ctrl+c" {
		m.quit = true
		return m, tea.Quit
	}
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

// handlePaste is the central routing point for bracketed-paste events
// (tea.PasteMsg). The active mode owns the payload:
//
//   - ModeCommand: insert into the command-bar buffer (newlines → space).
//   - ModeSearch:  insert into the filter buffer + live-apply to the screen.
//   - ModeHelp:    drop (no text input).
//   - default:     forward to the active screen, which lets the form route
//     it to the focused field (see components.Form.Update).
//
// All sanitisation happens in [lineedit.InsertText]; this function only
// dispatches.
func (m *Model) handlePaste(msg tea.PasteMsg) (tea.Model, tea.Cmd) {
	switch m.mode {
	case ModeCommand:
		state := lineedit.InsertText(lineedit.State{
			Runes:  []rune(m.command.Buffer),
			Cursor: m.command.Cursor,
		}, msg.Content)
		m.command.Buffer = state.String()
		m.command.Cursor = state.Cursor
		m.command.Error = ""
		m.command.Suggestion = CompletionSuggestion(m.command.Buffer)
		return m, nil
	case ModeSearch:
		state := lineedit.InsertText(lineedit.State{
			Runes:  []rune(m.search.Buffer),
			Cursor: m.search.Cursor,
		}, msg.Content)
		m.search.Buffer = state.String()
		m.search.Cursor = state.Cursor
		if m.active != nil {
			setScreenSearch(m.active, m.search.Buffer)
		}
		m.refreshSearchSuggestions()
		return m, nil
	case ModeHelp:
		return m, nil
	default:
		cmd := m.forwardToActive(msg)
		routeCmd := m.routeActiveAction()
		return m, teaBatch(cmd, routeCmd)
	}
}

// handleNormalKey runs the default-mode pipeline: raw-input screens get
// every key as a literal, then global shortcuts, then k9s-style filter
// clearing on esc/ctrl+u, then forward to the active screen with q/esc
// fallback only when nothing claimed the key. ctrl+c is short-circuited
// in [Model.handleKey] before any mode-specific dispatch.
func (m *Model) handleNormalKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
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
	// k9s parity: esc with an applied filter wipes the filter AND falls
	// through to the screen's esc binding, so a single press both clears
	// and pops. ctrl+u below is the readline-style alternative that only
	// wipes and stays put.
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
//
// Globals whose semantics don't apply inside an overlay (currently `ctrl+r` —
// a form has nothing to refresh) yield to the overlay by returning false, so
// the key reaches the active screen instead of silently firing a no-op global.
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
		if m.active != nil && screenHasOverlay(m.active) {
			return false
		}
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
		// guard cluster-bound screens so `:topics` / `:groups` from the
		// clusters picker don't leave the user on a blank placeholder.
		// the toast is pushed to the active screen so promoteFlash surfaces
		// it through the global flash bar — k9s-style — instead of an
		// inline error glued to the command prompt. when the active screen
		// has no toast queue (e.g. configsrc), bounce to the clusters
		// picker so the warning has somewhere to land and the next obvious
		// action is right there.
		if requiresClient(cmd.Screen) && m.client == nil {
			if q, ok := activeToastQueue(m.active); ok {
				q.Push(components.ToastWarning, "connect to a cluster first")
				return m, nil
			}
			next := m.replaceScreen(ScreenClusters, "")
			if q, ok := activeToastQueue(m.active); ok {
				q.Push(components.ToastWarning, "connect to a cluster first")
			}
			return m, next
		}
		next := m.replaceScreen(cmd.Screen, cmd.Arg)
		return m, next
	}
	// tab and right-at-end promote the ghost suggestion. ctrl+f is an alias
	// for right here.
	if promoted := m.promoteCommandSuggestion(key); promoted {
		return m, nil
	}
	if applied, changed := applyLineEdit(&m.command, key); applied {
		if changed {
			m.command.Error = ""
			m.command.Suggestion = CompletionSuggestion(m.command.Buffer)
		}
	}
	return m, nil
}

// promoteCommandSuggestion accepts the current ghost suggestion when the user
// presses tab, or right/ctrl+f at end of buffer. Returns true when consumed.
func (m *Model) promoteCommandSuggestion(key tea.KeyPressMsg) bool {
	if m.command.Suggestion == "" {
		return false
	}
	switch key.String() {
	case "tab":
	case "right", "ctrl+f":
		if m.command.Cursor < lineedit.RuneLen(m.command.Buffer) {
			return false
		}
	default:
		return false
	}
	m.command.Buffer = m.command.Suggestion
	m.command.Cursor = lineedit.RuneLen(m.command.Buffer)
	m.command.Suggestion = ""
	m.command.Error = ""
	return true
}

// applyLineEdit feeds key through lineedit using bar as the buffer/cursor.
// Returns (applied, changed) — applied is whether lineedit handled the key,
// changed is whether the buffer differs after the operation.
func applyLineEdit(bar *layout.CommandBar, key tea.KeyPressMsg) (bool, bool) {
	state, ok := lineedit.Apply(lineedit.State{
		Runes:  []rune(bar.Buffer),
		Cursor: bar.Cursor,
	}, key)
	if !ok {
		return false, false
	}
	newBuf := state.String()
	changed := newBuf != bar.Buffer
	bar.Buffer = newBuf
	bar.Cursor = state.Cursor
	return true, changed
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
//   - esc        clears the filter and closes (no history push).
//   - enter      commits the buffer to history; live filter stays applied.
//   - tab        promotes the ghost suggestion; right / ctrl+f do the same
//     when the cursor is at the end of the buffer, otherwise they move it.
//   - up / down  cycle through suggestion matches (wraps).
//
// All other edit keys (ctrl+a/e/u/k/w, alt+b/f/backspace, arrows, home/end,
// backspace/delete, text) are delegated to [lineedit.Apply].
func (m *Model) handleSearchKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		if m.active != nil {
			setScreenSearch(m.active, "")
		}
		m.closeSearchPrompt()
		return m, nil
	case "enter":
		if buf := m.search.Buffer; buf != "" {
			if hist := m.activeSearchHistory(); hist != nil {
				hist.Push(buf)
			}
		}
		m.closeSearchPrompt()
		return m, nil
	case "up":
		m.cycleSearchSuggestion(1)
		return m, nil
	case "down":
		m.cycleSearchSuggestion(-1)
		return m, nil
	}
	if m.promoteSearchSuggestion(key) {
		if m.active != nil {
			setScreenSearch(m.active, m.search.Buffer)
		}
		return m, nil
	}
	if applied, changed := applyLineEdit(&m.search, key); applied && changed {
		if m.active != nil {
			setScreenSearch(m.active, m.search.Buffer)
		}
		m.refreshSearchSuggestions()
	}
	return m, nil
}

// promoteSearchSuggestion mirrors promoteCommandSuggestion for the `/` prompt.
func (m *Model) promoteSearchSuggestion(key tea.KeyPressMsg) bool {
	if m.search.Suggestion == "" {
		return false
	}
	switch key.String() {
	case "tab":
	case "right", "ctrl+f":
		if m.search.Cursor < lineedit.RuneLen(m.search.Buffer) {
			return false
		}
	default:
		return false
	}
	m.search.Buffer = m.search.Suggestion
	m.search.Cursor = lineedit.RuneLen(m.search.Buffer)
	m.searchSuggestions = nil
	m.searchSuggestionIdx = -1
	m.search.Suggestion = ""
	return true
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
