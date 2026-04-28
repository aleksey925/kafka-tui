// Package tui hosts the Bubble Tea v2 root model that wires the global
// chrome (header, command bar, status bar, key hints) to the screen router.
//
// Individual screens (clusters, topics, messages, etc.) are added in later
// tasks; this package only provides the shell.
package tui

import (
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
	"github.com/aleksey925/kafka-tui/internal/version"
)

// Mode is the root model's current input mode.
type Mode int

const (
	// ModeNormal: ordinary screen interaction.
	ModeNormal Mode = iota
	// ModeCommand: a `:` command bar is open.
	ModeCommand
	// ModeSearch: a `/` search bar is open. Routed to the active screen on
	// submit; the root model only owns rendering.
	ModeSearch
	// ModeHelp: full-screen help overlay is visible.
	ModeHelp
)

// Options configure the root model at construction.
type Options struct {
	// Cluster is the currently selected cluster's display name (may be empty
	// before a cluster is chosen).
	Cluster      string
	ClusterColor string
	ReadOnly     bool
	FromCLI      bool

	// Initial places the router on a starting screen. Empty leaves the router
	// empty (the bootstrap process pushes the appropriate first screen).
	Initial ScreenID

	// Width / Height seed the model with a known terminal size. Real values
	// arrive via tea.WindowSizeMsg.
	Width, Height int

	// Version / Commit are shown in the help overlay footer.
	Version, Commit string

	// Now is an injectable clock for deterministic tests.
	Now func() time.Time

	// Styles overrides the default palette (mostly for tests).
	Styles theme.Styles

	// KeyHints lists the small set of hotkeys shown at the bottom of every
	// screen. Each screen will eventually override these via a message.
	KeyHints []layout.KeyHint
}

// Model is the Bubble Tea root model. It is exported so cmd/kafka-tui can
// own program lifecycle.
type Model struct {
	router *Router
	mode   Mode

	header  layout.HeaderInfo
	status  layout.StatusInfo
	command layout.CommandBar
	search  layout.CommandBar
	hints   []layout.KeyHint

	width, height int
	autoRefresh   bool

	styles  theme.Styles
	now     func() time.Time
	version string
	commit  string

	quit bool
}

// New creates a root model populated with the given options.
func New(opts Options) *Model {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	hints := opts.KeyHints
	if hints == nil {
		hints = DefaultKeyHints()
	}
	router := NewRouter()
	if opts.Initial != "" {
		router.Push(opts.Initial)
	}
	return &Model{
		router: router,
		mode:   ModeNormal,
		header: layout.HeaderInfo{
			Cluster:      opts.Cluster,
			ClusterColor: opts.ClusterColor,
			ReadOnly:     opts.ReadOnly,
			FromCLI:      opts.FromCLI,
		},
		status: layout.StatusInfo{
			Mode: layout.RefreshOff,
			Now:  now(),
		},
		hints:       hints,
		width:       opts.Width,
		height:      opts.Height,
		autoRefresh: true,
		styles:      styles,
		now:         now,
		version:     opts.Version,
		commit:      opts.Commit,
	}
}

// DefaultKeyHints returns the key hints shown by every screen until a screen
// installs its own.
func DefaultKeyHints() []layout.KeyHint {
	return []layout.KeyHint{
		{Key: ":", Label: "command"},
		{Key: "/", Label: "search"},
		{Key: "?", Label: "help"},
		{Key: "Ctrl+R", Label: "refresh"},
		{Key: "q", Label: "back/quit"},
	}
}

// Init implements tea.Model.
func (m *Model) Init() tea.Cmd {
	return nil
}

// Router exposes the underlying router (for tests and bootstrap).
func (m *Model) Router() *Router { return m.router }

// Mode returns the current input mode (for tests).
func (m *Model) Mode() Mode { return m.mode }

// CommandBuffer returns the current command-bar buffer (for tests).
func (m *Model) CommandBuffer() string { return m.command.Buffer }

// SearchBuffer returns the current search buffer (for tests).
func (m *Model) SearchBuffer() string { return m.search.Buffer }

// AutoRefresh reports whether auto-refresh is enabled (for tests).
func (m *Model) AutoRefresh() bool { return m.autoRefresh }

// Status returns the current status snapshot (for tests).
func (m *Model) Status() layout.StatusInfo { return m.status }

// Quit reports whether the model has signaled program termination.
func (m *Model) Quit() bool { return m.quit }

// SetAutoRefresh forces a refresh state (used at bootstrap).
func (m *Model) SetAutoRefresh(on bool) {
	m.autoRefresh = on
	if on && m.status.Mode == layout.RefreshManual {
		m.status.Mode = layout.RefreshAuto
	} else if !on && m.status.Mode == layout.RefreshAuto {
		m.status.Mode = layout.RefreshManual
	}
}

// SetStatus replaces the status snapshot.
func (m *Model) SetStatus(s layout.StatusInfo) {
	m.status = s
	if m.autoRefresh && s.Mode == layout.RefreshManual {
		m.autoRefresh = false
	}
	if !m.autoRefresh && s.Mode == layout.RefreshAuto {
		m.autoRefresh = true
	}
}

// SetKeyHints replaces the bottom hints.
func (m *Model) SetKeyHints(hints []layout.KeyHint) {
	m.hints = hints
}

// Update implements tea.Model.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

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

func (m *Model) handleNormalKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case ":":
		m.mode = ModeCommand
		m.command = layout.CommandBar{Active: true, Prefix: ':'}
		return m, nil
	case "/":
		m.mode = ModeSearch
		m.search = layout.CommandBar{Active: true, Prefix: '/'}
		return m, nil
	case "?":
		m.mode = ModeHelp
		return m, nil
	case "ctrl+r":
		m.SetAutoRefresh(!m.autoRefresh)
		return m, nil
	case "ctrl+c":
		m.quit = true
		return m, tea.Quit
	case "q", "esc":
		// q/esc pops the screen stack; if nothing remains, quit.
		if m.router.Depth() <= 1 {
			m.quit = true
			return m, tea.Quit
		}
		m.router.Pop()
		return m, nil
	}
	return m, nil
}

func (m *Model) handleCommandKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		m.mode = ModeNormal
		m.command = layout.CommandBar{}
		return m, nil
	case "enter":
		cmd, err := ParseCommand(m.command.Buffer)
		if err != nil {
			m.command.Error = err.Error()
			return m, nil
		}
		m.mode = ModeNormal
		m.command = layout.CommandBar{}
		m.router.Replace(cmd.Screen)
		return m, nil
	case "backspace":
		if n := len(m.command.Buffer); n > 0 {
			m.command.Buffer = m.command.Buffer[:n-1]
			m.command.Error = ""
		}
		return m, nil
	default:
		if t := key.Text; t != "" {
			m.command.Buffer += t
			m.command.Error = ""
		}
		return m, nil
	}
}

func (m *Model) handleSearchKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		m.mode = ModeNormal
		m.search = layout.CommandBar{}
		return m, nil
	case "enter":
		// search delegation lives on individual screens (Task 11+); the root
		// model just collects input and returns to normal mode.
		m.mode = ModeNormal
		return m, nil
	case "backspace":
		if n := len(m.search.Buffer); n > 0 {
			m.search.Buffer = m.search.Buffer[:n-1]
		}
		return m, nil
	default:
		if t := key.Text; t != "" {
			m.search.Buffer += t
		}
		return m, nil
	}
}

func (m *Model) handleHelpKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc", "q", "?":
		m.mode = ModeNormal
		return m, nil
	}
	return m, nil
}

// View implements tea.Model.
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

	header := layout.Header(m.styles, m.header)
	status := layout.Status(m.styles, m.statusForRender())
	topRow := joinRow(m.width, header, status)

	body := m.renderBody()

	cmd := layout.CommandLine(m.styles, m.command)
	if m.mode == ModeSearch {
		cmd = layout.CommandLine(m.styles, m.search)
	}

	hints := layout.KeyHints(m.styles, m.hints)

	parts := []string{topRow}
	if body != "" {
		parts = append(parts, body)
	}
	if cmd != "" {
		parts = append(parts, cmd)
	}
	if hints != "" {
		parts = append(parts, hints)
	}
	return strings.Join(parts, "\n")
}

func (m *Model) statusForRender() layout.StatusInfo {
	s := m.status
	if s.Now.IsZero() {
		s.Now = m.now()
	}
	return s
}

// renderBody returns a placeholder body until real screens land in later
// tasks. The active screen's name is shown so users see something meaningful
// during bootstrap.
func (m *Model) renderBody() string {
	active := m.router.Active()
	if active == "" {
		return m.styles.StatusInfo.Render("(no screen active)")
	}
	return m.styles.StatusInfo.Render(string(active) + " — coming soon")
}

func (m *Model) renderHelp() string {
	title := m.styles.HelpTitle.Render("Help")
	versionLine := m.styles.StatusInfo.Render(version.Format(m.version, m.commit))

	globalHints := []layout.KeyHint{
		{Key: ":", Label: "open command bar"},
		{Key: "/", Label: "open search"},
		{Key: "?", Label: "toggle help"},
		{Key: "Ctrl+R", Label: "toggle auto-refresh"},
		{Key: "Esc/q", Label: "back / quit"},
		{Key: "Ctrl+C", Label: "quit"},
	}
	commands := []layout.KeyHint{
		{Key: ":topics", Label: "topics list"},
		{Key: ":groups", Label: "consumer groups"},
		{Key: ":clusters", Label: "cluster list"},
		{Key: ":cluster <name>", Label: "switch cluster"},
		{Key: ":logs", Label: "log viewer"},
		{Key: ":config sources", Label: "config provenance"},
	}

	body := strings.Join([]string{
		title,
		"",
		m.styles.HelpTitle.Render("Global"),
		layout.KeyHints(m.styles, globalHints),
		"",
		m.styles.HelpTitle.Render("Commands"),
		layout.KeyHints(m.styles, commands),
	}, "\n")

	if m.width > 0 {
		footer := lipgloss.PlaceHorizontal(m.width, lipgloss.Right, versionLine)
		body += "\n\n" + footer
	} else {
		body += "\n\n" + versionLine
	}
	return body
}

// joinRow places left and right strings on a single line, padding so right is
// pinned to the rightmost column. width=0 falls back to a simple join.
func joinRow(width int, left, right string) string {
	if right == "" {
		return left
	}
	if width <= 0 {
		return left + "  " + right
	}
	gap := max(1, width-lipgloss.Width(left)-lipgloss.Width(right))
	return left + strings.Repeat(" ", gap) + right
}
