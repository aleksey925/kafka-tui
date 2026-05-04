package tui

import (
	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

// handleKey is the top-level keystroke dispatcher. It routes every key
// to the handler for the model's current mode (normal, command bar,
// search prompt, help overlay), each of which knows whether to
// consume the key, forward it to the active screen, or change the
// mode itself.
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
// raw-input screens get every key as a literal, then the global
// shortcuts (`:` / `/` / `?` / `ctrl+r`) get a chance, then the esc
// filter-clear cascade, then we forward to the active screen and use
// the q/esc fallback only when nothing claimed the key.
func (m *Model) handleNormalKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	// ctrl+c is always global so the user can quit even from inside a form.
	if key.String() == "ctrl+c" {
		m.quit = true
		return m, tea.Quit
	}
	// when the active screen is editing free-form text (produce form, topic
	// create/clone, reset params), route every key to it as a literal so
	// `:`, `/`, `?`, `ctrl+r` reach the form instead of triggering global
	// shortcuts.
	if m.active != nil && screenWantsRawInput(m.active) {
		cmd := m.forwardToActive(key)
		routeCmd := m.routeActiveAction()
		return m, teaBatch(cmd, routeCmd)
	}
	if m.handleGlobalShortcut(key) {
		return m, nil
	}
	// esc cascade: if the screen has a modal overlay open, esc belongs
	// to it (close confirm/chooser/etc.), not to the filter-clear or pop
	// path below. Capture the pre-state so we can also suppress the pop
	// after the screen closes its overlay.
	hadOverlay := m.active != nil && screenHasOverlay(m.active)
	if key.String() == "esc" && !hadOverlay && m.active != nil && screenActiveFilter(m.active) != "" {
		// no overlay in the way — clear the screen-level filter first;
		// next esc will pop. Mirrors k9s behavior.
		setScreenSearch(m.active, "")
		return m, nil
	}
	// forward to active screen first; it may consume q/esc itself
	// (e.g. close an overlay). After the screen handles it we look at the
	// resulting Action; if no screen wants to keep us, q/esc pops the stack.
	cmd := m.forwardToActive(key)
	routeCmd := m.routeActiveAction()
	if cmd == nil && routeCmd == nil {
		if fbCmd, ok := m.handleQuitFallback(key, hadOverlay); ok {
			return m, fbCmd
		}
	}
	return m, teaBatch(cmd, routeCmd)
}

// handleGlobalShortcut runs the screen-agnostic shortcut switch (`:` /
// `/` / `?` / `ctrl+r`). Returns false when the key isn't one of those
// so the caller falls through to the screen-aware path.
func (m *Model) handleGlobalShortcut(key tea.KeyPressMsg) bool {
	switch key.String() {
	case ":":
		m.mode = ModeCommand
		m.command = layout.CommandBar{Active: true, Prefix: ':'}
		m.applySize()
		return true
	case "/":
		// only open the prompt for screens that can actually filter — on
		// detail/form views the prompt would just swallow keystrokes.
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
// screen returned no command and no Action — i.e. it didn't claim the
// key for an overlay or transition. Returns ok=false for keys outside
// q/esc, leaving the caller to teaBatch the screen's nil cmds.
func (m *Model) handleQuitFallback(key tea.KeyPressMsg, hadOverlay bool) (tea.Cmd, bool) {
	switch key.String() {
	case "q":
		// `q` quits at the root, otherwise pops a screen.
		if m.router.Depth() <= 1 {
			m.quit = true
			return tea.Quit, true
		}
		m.popScreen()
		return m.activeInit(), true
	case "esc":
		if hadOverlay {
			// the screen just closed its overlay via the forwarded esc —
			// don't double-act by also popping.
			return nil, true
		}
		// at the root esc is a no-op so users don't quit the app by
		// accident. ctrl+c remains the unconditional exit.
		if m.router.Depth() > 1 {
			m.popScreen()
			return m.activeInit(), true
		}
		return nil, true
	}
	return nil, false
}

// handleCommandKey runs the `:` command-bar prompt: typing edits the
// buffer (with live tab-completion suggestions), enter parses and
// dispatches the command, esc dismisses the bar.
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

// openSearchPrompt switches into ModeSearch and pre-fills the buffer with
// the screen's currently-applied filter so `/` re-opens an existing filter
// for editing instead of discarding it. esc on an empty edit restores
// whatever was there before.
func (m *Model) openSearchPrompt() {
	m.mode = ModeSearch
	m.searchOriginal = ""
	if m.active != nil {
		m.searchOriginal = screenActiveFilter(m.active)
	}
	m.search = layout.CommandBar{Active: true, Prefix: '/', Buffer: m.searchOriginal}
	m.applySize()
}

// handleSearchKey runs the host's k9s-style filter prompt: each keystroke
// updates the buffer AND pushes the live query into the active screen so
// rows filter as the user types. esc cancels the edit and restores the
// previous filter (or clears it when there was none). Enter commits the
// current buffer and dismisses the prompt.
func (m *Model) handleSearchKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		// restore the filter that was active before the prompt opened —
		// "/" then esc must be a no-op when used to inspect/edit, not a
		// silent way to drop an applied filter.
		if m.active != nil {
			setScreenSearch(m.active, m.searchOriginal)
		}
		m.mode = ModeNormal
		m.search = layout.CommandBar{}
		m.searchOriginal = ""
		m.applySize()
		return m, nil
	case "enter":
		m.mode = ModeNormal
		m.search = layout.CommandBar{}
		m.searchOriginal = ""
		m.applySize()
		// filter stays applied — the active screen's table already has it.
		return m, nil
	case "backspace":
		if n := len(m.search.Buffer); n > 0 {
			m.search.Buffer = m.search.Buffer[:n-1]
		}
		if m.active != nil {
			setScreenSearch(m.active, m.search.Buffer)
		}
		return m, nil
	default:
		if t := key.Text; t != "" {
			m.search.Buffer += t
		}
		if m.active != nil {
			setScreenSearch(m.active, m.search.Buffer)
		}
		return m, nil
	}
}

// handleHelpKey closes the help overlay on any of the documented exit
// keys; everything else is silently swallowed so the user can't trigger
// an unrelated action while the help is up.
func (m *Model) handleHelpKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc", "q", "?":
		m.mode = ModeNormal
		return m, nil
	}
	return m, nil
}
