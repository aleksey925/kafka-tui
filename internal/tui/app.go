// Package tui hosts the Bubble Tea v2 root model that wires the global
// chrome (header, command bar, status bar, key hints) to the screen router.
//
// The host owns:
//   - the active *kafka.Client (re-dialed when the user switches cluster);
//   - one instance of each screen, only one of which is non-nil at a time;
//   - the Bootstrap struct that supplies dialer, pinger, history, paths.
//
// Screens never touch the host directly. They populate an Action; the host
// reads it after every Update and routes via the Router.
package tui

import (
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/configsrc"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/groups"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/logs"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
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
	// ModeSearch: a `/` filter prompt is open. Same chrome slot as the
	// command bar; each keystroke is forwarded to the active screen as a
	// live filter via [screen.SetSearch].
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

	// Build describes the binary's version and VCS commit hash.
	Build version.BuildInfo

	// Now is an injectable clock for deterministic tests.
	Now func() time.Time

	// Styles overrides the default palette (mostly for tests).
	Styles theme.Styles

	// KeyHints lists the small set of hotkeys shown at the bottom of every
	// screen. Each screen will eventually override these via a message.
	KeyHints []layout.KeyHint

	// Bootstrap supplies the wiring needed to construct real screens (config,
	// dialer, pinger, history). When nil the host falls back to a placeholder
	// body — that path is exercised by unit tests that don't drive screens.
	Bootstrap *Bootstrap
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
	// searchOriginal is the screen's filter at the moment the prompt was
	// (re)opened. Esc restores it; enter keeps whatever the buffer holds.
	// Mirrors k9s — opening `/` over an existing filter is "edit", not
	// "discard and re-enter".
	searchOriginal string
	hints          []layout.KeyHint

	width, height int
	autoRefresh   bool

	styles theme.Styles
	now    func() time.Time
	build  version.BuildInfo

	quit bool

	// boot holds production wiring (nil in tests that don't drive screens).
	boot *Bootstrap

	// active kafka client (set when user connects to a cluster).
	client     *kafka.Client
	activeClu  string
	clusterClr string
	clusterRO  bool
	fromCLI    bool

	// active screen instance (nil when no screen is wired — placeholder body).
	active screen

	// nav seeds — populated when a screen requests navigation, consumed when
	// the next screen is instantiated.
	navTopic        string
	navTopicsFilter []string
	navPrefill      *kafka.Message
	navGroupFilter  string

	// lastTopic remembers which topic was selected when navigating away from
	// the topics screen, so the cursor can be restored on return.
	lastTopic string

	// flash holds the latest screen-emitted toast promoted to the global
	// bottom bar. flashSeenAt tracks which toast (by CreatedAt) we last
	// promoted, so an older or repeated push isn't re-shown.
	flash       layout.Flash
	flashSeenAt time.Time
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
	m := &Model{
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
		build:       opts.Build,
		boot:        opts.Bootstrap,
		activeClu:   opts.Cluster,
		clusterClr:  opts.ClusterColor,
		clusterRO:   opts.ReadOnly,
		fromCLI:     opts.FromCLI,
	}
	if opts.Initial != "" {
		m.pushScreen(opts.Initial)
	}
	return m
}

// DefaultKeyHints returns the key hints shown by every screen until a screen
// installs its own.
func DefaultKeyHints() []layout.KeyHint {
	return []layout.KeyHint{
		{Key: ":", Label: "command"},
		{Key: "/", Label: "search"},
		{Key: "?", Label: "help"},
		{Key: "ctrl+r", Label: "auto-refresh"},
		{Key: "q", Label: "back/quit"},
	}
}

// Init implements tea.Model. Returns the active screen's Init cmd. When the
// clusters screen reports a [clusters.Model.SkipTarget] (because there's
// exactly one cluster or --brokers was supplied), the host connects to it
// immediately and starts on the topics screen instead.
func (m *Model) Init() tea.Cmd {
	if cs, ok := m.active.(*clustersScreen); ok {
		if name, skip := cs.m.SkipTarget(); skip {
			if connectCmd := m.connectCluster(name); connectCmd != nil {
				return teaBatch(connectCmd, m.watchConfigSnapshots())
			}
		}
	}
	return teaBatch(m.activeInit(), m.watchConfigSnapshots())
}

// configSnapshotMsg carries a fresh config snapshot from the file watcher.
// One Snapshot per tea.Cmd; after handling, the host re-arms the listener
// so subsequent file edits keep arriving.
type configSnapshotMsg struct {
	snapshot config.Snapshot
}

// watchConfigSnapshots returns a tea.Cmd that blocks on the watcher's
// Snapshots channel until the next event, then surfaces it as a typed msg.
// Returns nil when no watcher is wired (e.g. tests without bootstrap).
func (m *Model) watchConfigSnapshots() tea.Cmd {
	if m.boot == nil || m.boot.ConfigSnapshots == nil {
		return nil
	}
	ch := m.boot.ConfigSnapshots
	return func() tea.Msg {
		s, ok := <-ch
		if !ok {
			// channel closed (watcher.Close() called or fsnotify died) —
			// log so future "config didn't reload" reports have a trail.
			slog.Warn("config watcher snapshot channel closed; auto-reload disabled")
			return nil
		}
		return configSnapshotMsg{snapshot: s}
	}
}

// Router exposes the underlying router (for tests and bootstrap).
func (m *Model) Router() *Router { return m.router }

// Mode returns the current input mode (for tests).
func (m *Model) Mode() Mode { return m.mode }

// CommandBuffer returns the current command-bar buffer (for tests).
func (m *Model) CommandBuffer() string { return m.command.Buffer }

// CommandSuggestion returns the current command-bar suggestion (for tests).
func (m *Model) CommandSuggestion() string { return m.command.Suggestion }

// AutoRefresh reports whether auto-refresh is enabled (for tests).
func (m *Model) AutoRefresh() bool { return m.autoRefresh }

// Status returns the current status snapshot (for tests).
func (m *Model) Status() layout.StatusInfo { return m.status }

// Quit reports whether the model has signaled program termination.
func (m *Model) Quit() bool { return m.quit }

// ActiveClient returns the currently connected *kafka.Client (nil before any
// cluster has been selected). Exposed so cmd/kafka-tui can close it on exit.
func (m *Model) ActiveClient() *kafka.Client { return m.client }

// SetAutoRefresh toggles the global auto-refresh state. When the active
// screen has its own refresh ticker, the change is propagated via
// SetRefreshPaused so the ticker stops loading data (without stopping the
// ticker itself). The chrome's Refresh: indicator always reflects the new
// state.
func (m *Model) SetAutoRefresh(on bool) {
	m.autoRefresh = on
	if m.active != nil {
		m.active.SetRefreshPaused(!on)
	}
	m.syncRefreshStatus()
}

// syncRefreshStatus keeps the chrome's Refresh: indicator in sync with the
// active screen's RefreshInterval and the global auto-refresh flag. Called
// when the active screen changes and when ctrl+r flips m.autoRefresh.
//
// Special case: the clusters screen has no periodic poll — it reloads on
// filesystem events via config.Watcher. We reflect that as RefreshOnEdit
// instead of falling through to "off".
func (m *Model) syncRefreshStatus() {
	if _, ok := m.active.(*clustersScreen); ok && m.boot != nil && m.boot.ConfigSnapshots != nil {
		m.status.Mode = layout.RefreshOnEdit
		m.status.Interval = 0
		return
	}
	if m.active != nil && !m.active.SupportsRefresh() {
		// e.g. message detail, produce form, configsrc snapshot — show
		// a dash so the user understands the row isn't applicable.
		m.status.Mode = layout.RefreshNotApplicable
		m.status.Interval = 0
		return
	}
	interval := time.Duration(0)
	if m.active != nil {
		interval = m.active.RefreshInterval()
	}
	if interval <= 0 {
		m.status.Mode = layout.RefreshOff
		m.status.Interval = 0
		return
	}
	m.status.Interval = interval
	if m.autoRefresh {
		m.status.Mode = layout.RefreshAuto
	} else {
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
		m.applySize()
		return m, nil
	case tea.KeyPressMsg:
		newM, cmd := m.handleKey(msg)
		flashCmd := m.promoteFlash()
		return newM, teaBatch(cmd, flashCmd)
	case flashTickMsg:
		// re-evaluate the flash so an expired toast clears off the bar.
		flashCmd := m.promoteFlash()
		return m, flashCmd
	case configSnapshotMsg:
		m.handleConfigSnapshot(msg.snapshot)
		// re-arm the listener for the next snapshot.
		return m, teaBatch(m.watchConfigSnapshots(), m.promoteFlash())
	}
	// non-key, non-resize message → forward to active screen so async cmds
	// (e.g. topic-loaded, fetch-result) reach their owner.
	cmd := m.forwardToActive(msg)
	cmd = teaBatch(cmd, m.routeActiveAction(), m.promoteFlash())
	return m, cmd
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
	// ctrl+c is always global so the user can quit even from inside a form.
	if key.String() == "ctrl+c" {
		m.quit = true
		return m, tea.Quit
	}
	// when the active screen is editing free-form text (produce form, topic
	// create/clone, reset params), route every key to it as a literal so
	// `:`, `/`, `?`, `ctrl+r` reach the form instead of triggering global
	// shortcuts.
	if m.active != nil && m.active.WantsRawInput() {
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
	hadOverlay := m.active != nil && m.active.HasOverlay()
	if key.String() == "esc" && !hadOverlay && m.active != nil && m.active.ActiveFilter() != "" {
		// no overlay in the way — clear the screen-level filter first;
		// next esc will pop. Mirrors k9s behavior.
		m.active.SetSearch("")
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
		if m.active != nil && !m.active.SupportsSearch() {
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
		m.searchOriginal = m.active.ActiveFilter()
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
			m.active.SetSearch(m.searchOriginal)
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
			m.active.SetSearch(m.search.Buffer)
		}
		return m, nil
	default:
		if t := key.Text; t != "" {
			m.search.Buffer += t
		}
		if m.active != nil {
			m.active.SetSearch(m.search.Buffer)
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
	t, ok := m.active.LatestFlash()
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
		s.LastRefresh = m.active.LastRefresh()
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

func (m *Model) renderHelp() string {
	title := m.styles.HelpTitle.Render("Help")
	versionLine := m.styles.StatusInfo.Render(m.build.Display())

	globalHints := []layout.KeyHint{
		{Key: ":", Label: "open command bar"},
		{Key: "/", Label: "open search"},
		{Key: "?", Label: "toggle help"},
		{Key: "ctrl+r", Label: "toggle auto-refresh"},
		{Key: "esc/q", Label: "back / quit"},
		{Key: "ctrl+c", Label: "quit"},
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

// teaBatch returns a command that runs cmds in any order. nils are filtered.
func teaBatch(cmds ...tea.Cmd) tea.Cmd {
	out := make([]tea.Cmd, 0, len(cmds))
	for _, c := range cmds {
		if c != nil {
			out = append(out, c)
		}
	}
	switch len(out) {
	case 0:
		return nil
	case 1:
		return out[0]
	default:
		return tea.Batch(out...)
	}
}

// updateHeaderForActive refreshes the header chrome when the active cluster
// changes (typically after [Connect]).
func (m *Model) updateHeaderForActive(name, color string, readOnly, fromCLI bool) {
	m.activeClu = name
	m.clusterClr = color
	m.clusterRO = readOnly
	m.fromCLI = fromCLI
	m.header = layout.HeaderInfo{
		Cluster:      name,
		ClusterColor: color,
		ReadOnly:     readOnly,
		FromCLI:      fromCLI,
	}
}

// connectCluster dials the named cluster and replaces the topics screen on
// the stack. Closes the previous *kafka.Client, if any.
func (m *Model) connectCluster(name string) tea.Cmd {
	if m.boot == nil || m.boot.Dialer == nil {
		return nil
	}
	clu := findCluster(m.boot.Clusters, name)
	if clu == nil {
		return nil
	}
	client, err := m.boot.Dialer.Dial(*clu)
	if err != nil {
		// surface dial errors via the clusters screen toast queue when we
		// can; otherwise log and stay on the clusters screen.
		if cs, ok := m.active.(*clustersScreen); ok {
			cs.m.Toasts().Push(components.ToastError, fmt.Sprintf("connect %q failed: %v", name, err))
		}
		return nil
	}
	if m.client != nil {
		m.client.Close()
	}
	m.client = client
	m.updateHeaderForActive(clu.Name, clu.Color, clu.ReadOnly || (m.boot != nil && m.boot.ReadOnly), name == m.boot.CLIName)
	return m.replaceScreen(ScreenTopics, "")
}

// findCluster returns a pointer to the cluster with the given name, or nil.
func findCluster(list []config.Cluster, name string) *config.Cluster {
	for i := range list {
		if list[i].Name == name {
			return &list[i]
		}
	}
	return nil
}

// applySize forwards the new geometry to the active screen — the inner
// area inside the body frame (terminal minus chrome and border).
func (m *Model) applySize() {
	if m.active == nil {
		return
	}
	w, h := m.bodyWidth(), m.bodyHeight()
	if w <= 0 || h <= 0 {
		return
	}
	m.active.SetSize(w, h)
}

// frameInset is the height contributed by the body frame's top+bottom
// border (2 rows). Width contribution is computed via [frameWidthInset]
// because the frame also reserves [layout.FrameSidePadding] columns on
// each side.
const frameInset = 2

// frameWidthInset is the total horizontal space the frame consumes:
// left border + left padding + right padding + right border.
const frameWidthInset = 2 + 2*layout.FrameSidePadding

// bodyHeight returns the inner height left for the active screen after the
// chrome and the body frame border are subtracted. The command-prompt rows
// only count when the bar is active — when closed, the body uses the full
// screen below the header.
func (m *Model) bodyHeight() int {
	chrome := layout.HeaderRows + 1 // header + flash bar
	if m.mode == ModeCommand || m.mode == ModeSearch {
		chrome += layout.CommandRows
	}
	h := m.height - chrome - frameInset
	if h < 1 {
		return 0
	}
	return h
}

// bodyWidth returns the inner width inside the body frame (terminal width
// minus the frame's left/right borders and side padding).
func (m *Model) bodyWidth() int {
	w := m.width - frameWidthInset
	if w < 1 {
		return 0
	}
	return w
}

// activeKeyHints returns the hints for the active screen, falling back to
// the model's default when no screen is wired.
func (m *Model) activeKeyHints() []layout.KeyHint {
	if m.active == nil {
		return m.hints
	}
	return m.active.KeyHints()
}

// activeView returns the active screen's body, or "" if no instance exists.
func (m *Model) activeView() string {
	if m.active == nil {
		return ""
	}
	return m.active.View()
}

// activeInit returns the active screen's Init cmd, or nil.
func (m *Model) activeInit() tea.Cmd {
	if m.active == nil {
		return nil
	}
	return m.active.Init()
}

// forwardToActive sends msg to the active screen and returns its tea.Cmd.
func (m *Model) forwardToActive(msg tea.Msg) tea.Cmd {
	if m.active == nil {
		return nil
	}
	return m.active.Update(msg)
}

// routeActiveAction reads the active screen's pending Action and reacts to
// it (push/pop/replace/connect/quit). Returns any tea.Cmd produced.
func (m *Model) routeActiveAction() tea.Cmd {
	switch s := m.active.(type) {
	case *clustersScreen:
		return m.routeClustersAction(s)
	case *topicsScreen:
		return m.routeTopicsAction(s)
	case *messagesScreen:
		return m.routeMessagesAction(s)
	case *produceScreen:
		return m.routeProduceAction(s)
	case *groupsScreen:
		return m.routeGroupsAction(s)
	case *logsScreen:
		return m.routeLogsAction(s)
	case *configsrcScreen:
		return m.routeConfigSrcAction(s)
	case *topicConfigsScreen:
		return m.routeTopicConfigsAction(s)
	}
	return nil
}

func (m *Model) routeClustersAction(s *clustersScreen) tea.Cmd {
	a := s.m.ConsumeAction()
	if a.Quit {
		m.quit = true
		return tea.Quit
	}
	if a.Connect != "" {
		return m.connectCluster(a.Connect)
	}
	if a.Reload {
		m.reloadClusters(s)
	}
	return nil
}

// handleConfigSnapshot applies a fresh config snapshot delivered by the
// file watcher. Updates Bootstrap state for everyone; if the user is
// currently looking at the clusters screen, also pushes the fresh list
// into the model and a toast that honestly distinguishes "the cluster
// list changed" from "some other config field changed". Other screens
// stay silent (their data isn't config-derived in a way that re-render
// makes sense without a re-instantiate).
func (m *Model) handleConfigSnapshot(snap config.Snapshot) {
	if m.boot == nil {
		return
	}
	if snap.Err != nil {
		// surface parse errors; without this a syntax error in
		// clusters.yaml would silently keep the stale config and the
		// user would only notice on the next reconnect.
		slog.Error("config watcher: reload failed", "err", snap.Err)
		if cs, ok := m.active.(*clustersScreen); ok {
			cs.m.Toasts().Push(components.ToastError, "config reload failed: "+snap.Err.Error())
		}
		return
	}
	if snap.Loaded == nil {
		return
	}
	list := snap.Loaded.Clusters
	cli := ""
	if m.boot.BuildClusterList != nil {
		list, cli = m.boot.BuildClusterList(snap.Loaded.Clusters)
	}
	clustersChanged := !reflect.DeepEqual(m.boot.Clusters, list) || m.boot.CLIName != cli
	m.boot.Loaded = snap.Loaded
	m.boot.Clusters = list
	m.boot.CLIName = cli
	cs, onClusters := m.active.(*clustersScreen)
	if onClusters {
		cs.m.SetClusters(list, cli)
		if clustersChanged {
			cs.m.Toasts().Push(components.ToastSuccess, fmt.Sprintf("clusters reloaded · %d", len(list)))
		} else {
			cs.m.Toasts().Push(components.ToastInfo, "config reloaded")
		}
	}
	// active cluster's fields changed under us — the live *kafka.Client is
	// still wired to the previous broker/auth values, so warn the user
	// that a reconnect is required.
	if snap.ActiveClusterChanged {
		warning := "active cluster changed in config — reconnect to apply"
		// always log: even if the chrome can't surface it (e.g. on the
		// configsrc screen which has no toast queue), the warning sits
		// in the file log for troubleshooting.
		slog.Warn(warning)
		// route through the active screen's toast queue when possible —
		// promoteFlash will pick it up on the next render. Direct
		// assignment to m.flash would be wiped by promoteFlash because
		// the host-side flash slot is not a separate source.
		if q, ok := activeToastQueue(m.active); ok {
			q.Push(components.ToastWarning, warning)
		}
	}
}

// activeToastQueue exposes the active screen's toast queue when the
// concrete model has one. Returns ok=false for screens without queues
// (currently only configsrc).
func activeToastQueue(s screen) (*components.Toasts, bool) {
	switch a := s.(type) {
	case *clustersScreen:
		return a.m.Toasts(), true
	case *topicsScreen:
		return a.m.Toasts(), true
	case *messagesScreen:
		return a.m.Toasts(), true
	case *produceScreen:
		return a.m.Toasts(), true
	case *groupsScreen:
		return a.m.Toasts(), true
	case *logsScreen:
		return a.m.Toasts(), true
	case *topicConfigsScreen:
		return a.m.Toasts(), true
	}
	return nil, false
}

// reloadClusters re-reads config files via Bootstrap.ConfigReloader and
// pushes the fresh list into the clusters screen. Errors are surfaced
// through the screen's toast queue (which the global flash bar promotes).
func (m *Model) reloadClusters(s *clustersScreen) {
	if m.boot == nil || m.boot.ConfigReloader == nil {
		s.m.Toasts().Push(components.ToastWarning, "reload not configured")
		return
	}
	loaded, list, cli, err := m.boot.ConfigReloader()
	if err != nil {
		s.m.Toasts().Push(components.ToastError, "reload: "+err.Error())
		return
	}
	m.boot.Loaded = loaded
	m.boot.Clusters = list
	m.boot.CLIName = cli
	s.m.SetClusters(list, cli)
	s.m.Toasts().Push(components.ToastSuccess, fmt.Sprintf("reloaded %d clusters", len(list)))
}

func (m *Model) routeTopicsAction(s *topicsScreen) tea.Cmd {
	a := s.m.ConsumeAction()
	switch {
	case a.Quit:
		return m.popOrReplaceToClusters()
	case a.Messages != "":
		m.lastTopic = a.Messages
		m.navTopic = a.Messages
		return m.pushScreenCmd(ScreenMessages)
	case a.Configs != "":
		m.lastTopic = a.Configs
		m.navTopic = a.Configs
		return m.pushScreenCmd(ScreenTopicConfigs)
	case a.Groups != "":
		m.lastTopic = a.Groups
		m.navGroupFilter = a.Groups
		return m.pushScreenCmd(ScreenGroups)
	case a.Produce != "":
		m.lastTopic = a.Produce
		m.navTopic = a.Produce
		m.navPrefill = nil
		return m.pushScreenCmd(ScreenProduce)
	}
	return nil
}

func (m *Model) routeMessagesAction(s *messagesScreen) tea.Cmd {
	a := s.m.ConsumeAction()
	switch {
	case a.Back:
		m.popScreen()
		return m.activeInit()
	case a.Produce != "":
		m.lastTopic = a.Produce
		m.navTopic = a.Produce
		m.navPrefill = a.PrefillFromMessage
		return m.pushScreenCmd(ScreenProduce)
	}
	return nil
}

func (m *Model) routeProduceAction(s *produceScreen) tea.Cmd {
	a := s.m.ConsumeAction()
	if a.Back {
		m.popScreen()
		return m.activeInit()
	}
	return nil
}

func (m *Model) routeGroupsAction(s *groupsScreen) tea.Cmd {
	a := s.m.ConsumeAction()
	switch {
	case a.Back:
		m.popScreen()
		return m.activeInit()
	case a.Topic != "":
		m.lastTopic = a.Topic
		m.navTopic = a.Topic
		return m.pushScreenCmd(ScreenMessages)
	case len(a.TopicsForGroup) > 0:
		m.navTopicsFilter = a.TopicsForGroup
		return m.pushScreenCmd(ScreenTopics)
	}
	return nil
}

func (m *Model) routeLogsAction(s *logsScreen) tea.Cmd {
	a := s.m.ConsumeAction()
	if a.Back {
		m.popScreen()
		return m.activeInit()
	}
	return nil
}

func (m *Model) routeConfigSrcAction(s *configsrcScreen) tea.Cmd {
	a := s.m.ConsumeAction()
	if a.Back {
		m.popScreen()
		return m.activeInit()
	}
	return nil
}

func (m *Model) routeTopicConfigsAction(s *topicConfigsScreen) tea.Cmd {
	a := s.m.ConsumeAction()
	if a.Back {
		m.popScreen()
		return m.activeInit()
	}
	return nil
}

// popOrReplaceToClusters pops the topics screen to expose the clusters list
// underneath; if there's nothing below, it replaces with the clusters screen.
func (m *Model) popOrReplaceToClusters() tea.Cmd {
	if m.router.Depth() > 1 {
		m.popScreen()
		return m.activeInit()
	}
	return m.replaceScreen(ScreenClusters, "")
}

// closeActive releases any background resources held by the current
// active screen (clone goroutine, follow session, etc.) and clears the
// pointer so a fresh instance can be wired in.
func (m *Model) closeActive() {
	if m.active != nil {
		m.active.Close()
		m.active = nil
	}
	m.flash = layout.Flash{}
}

// pushScreen pushes id onto the router stack and constructs the screen
// instance. Used at startup. Discards the resulting Init cmd because the
// caller is expected to invoke [Init] separately.
func (m *Model) pushScreen(id ScreenID) {
	m.closeActive()
	m.router.Push(id)
	m.instantiate(id)
	m.applySize()
	m.syncRefreshStatus()
}

// pushScreenCmd is the runtime variant: pushes a screen, instantiates it, and
// returns its Init cmd to be batched with the host's reply.
func (m *Model) pushScreenCmd(id ScreenID) tea.Cmd {
	m.closeActive()
	m.router.Push(id)
	m.instantiate(id)
	m.applySize()
	m.syncRefreshStatus()
	return m.activeInit()
}

// replaceScreen swaps the active screen for a new id, freeing the previous
// instance.
func (m *Model) replaceScreen(id ScreenID, arg string) tea.Cmd {
	if id == ScreenClusters && arg != "" {
		// `:cluster <name>` connects to a known cluster directly.
		return m.connectCluster(arg)
	}
	m.closeActive()
	m.router.Replace(id)
	m.instantiate(id)
	m.applySize()
	m.syncRefreshStatus()
	return m.activeInit()
}

// popScreen pops the router stack and frees the previously active screen.
// The new active screen (one level up) is re-instantiated from the bootstrap
// data, since we drop instances when leaving them to release Kafka follow
// sessions, table state, etc.
func (m *Model) popScreen() {
	m.closeActive()
	m.router.Pop()
	if id := m.router.Active(); id != "" {
		m.instantiate(id)
	}
	m.applySize()
	m.syncRefreshStatus()
}

// instantiate constructs the screen for id using the current bootstrap. When
// boot is nil or required deps (e.g. *kafka.Client for topic screens) are
// absent, the active screen is left nil and [renderBody] falls back to the
// placeholder.
func (m *Model) instantiate(id ScreenID) {
	if m.boot == nil {
		return
	}
	// when re-instantiating after a pop the nav seeds are empty; fall back to
	// lastTopic so topic-bound screens (messages, produce) recreate against the
	// correct topic instead of an empty one.
	if m.navTopic == "" {
		m.navTopic = m.lastTopic
	}
	defer m.clearNavSeeds()
	switch id {
	case ScreenClusters:
		m.active = &clustersScreen{m: m.newClusters()}
	case ScreenTopics:
		if m.client != nil {
			m.active = &topicsScreen{m: m.newTopics()}
		}
	case ScreenTopicConfigs:
		if m.client != nil {
			m.active = &topicConfigsScreen{m: m.newTopicConfigs()}
		}
	case ScreenMessages:
		if m.client != nil {
			m.active = &messagesScreen{m: m.newMessages()}
		}
	case ScreenProduce:
		if m.client != nil {
			m.active = &produceScreen{m: m.newProduce()}
		}
	case ScreenGroups:
		if m.client != nil {
			m.active = &groupsScreen{m: m.newGroups()}
		}
	case ScreenLogs:
		m.active = &logsScreen{m: m.newLogs()}
	case ScreenConfigSrc:
		m.active = &configsrcScreen{m: m.newConfigSrc()}
	case ScreenHelpOverlay:
		// help is a chrome overlay, not a routed screen.
	}
}

func (m *Model) clearNavSeeds() {
	m.navTopic = ""
	m.navTopicsFilter = nil
	m.navPrefill = nil
	m.navGroupFilter = ""
}

func (m *Model) newClusters() *clusters.Model {
	b := m.boot
	return clusters.New(clusters.Options{
		Clusters:        b.Clusters,
		CLIName:         b.CLIName,
		GlobalPath:      b.GlobalPath,
		ProjectPath:     b.ProjectPath,
		Pinger:          b.Pinger,
		Editor:          b.Editor,
		StartupWarnings: b.StartupWarnings,
		Now:             m.now,
		Styles:          m.styles,
	})
}

func (m *Model) newTopics() *topics.Model {
	cfg := m.boot.Loaded.Config
	return topics.New(topics.Options{
		Service:         m.client,
		ReadOnly:        m.clusterRO,
		Columns:         cfg.Topics.Columns,
		FilterTopics:    m.navTopicsFilter,
		FocusTopic:      m.lastTopic,
		RefreshInterval: parseRefresh(cfg.Refresh.TopicsList),
		Now:             m.now,
		Styles:          m.styles,
	})
}

func (m *Model) newTopicConfigs() *topics.ConfigsModel {
	return topics.NewConfigsModel(topics.ConfigsOptions{
		Service: m.client,
		Topic:   m.navTopic,
		Now:     m.now,
		Styles:  m.styles,
	})
}

func (m *Model) newMessages() *messages.Model {
	cfg := m.boot.Loaded.Config
	return messages.New(messages.Options{
		Service:   m.client,
		Topic:     m.navTopic,
		ReadOnly:  m.clusterRO,
		Columns:   cfg.Messages.Columns,
		Clipboard: m.boot.Clipboard,
		Now:       m.now,
		Styles:    m.styles,
	})
}

func (m *Model) newProduce() *produce.Model {
	cfg := m.boot.Loaded.Config
	return produce.New(produce.Options{
		Service:            m.client,
		Cluster:            m.activeClu,
		Topic:              m.navTopic,
		ReadOnly:           m.clusterRO,
		HistorySize:        cfg.Produce.HistorySize,
		History:            m.boot.History,
		Pager:              m.boot.Pager,
		PrefillFromMessage: m.navPrefill,
		Now:                m.now,
		Styles:             m.styles,
	})
}

func (m *Model) newGroups() *groups.Model {
	cfg := m.boot.Loaded.Config
	return groups.New(groups.Options{
		Service:               m.client,
		ReadOnly:              m.clusterRO,
		FilterTopic:           m.navGroupFilter,
		ListRefreshInterval:   parseRefresh(cfg.Refresh.GroupsList),
		DetailRefreshInterval: parseRefresh(cfg.Refresh.GroupDetail),
		Now:                   m.now,
		Styles:                m.styles,
	})
}

func (m *Model) newLogs() *logs.Model {
	return logs.New(logs.Options{
		Path:   m.boot.LogPath,
		Now:    m.now,
		Styles: m.styles,
	})
}

func (m *Model) newConfigSrc() *configsrc.Model {
	return configsrc.New(configsrc.Options{
		Sources: m.boot.Loaded.Sources,
		Styles:  m.styles,
	})
}

// parseRefresh converts the config string ("off", "5s", …) into a duration.
// "off" / unparseable / zero map to 0 (auto-refresh disabled).
func parseRefresh(s string) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "off") {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	if d < 0 {
		return 0
	}
	return d
}
