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
	hints   []layout.KeyHint

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
				return connectCmd
			}
		}
	}
	return m.activeInit()
}

// Router exposes the underlying router (for tests and bootstrap).
func (m *Model) Router() *Router { return m.router }

// Mode returns the current input mode (for tests).
func (m *Model) Mode() Mode { return m.mode }

// CommandBuffer returns the current command-bar buffer (for tests).
func (m *Model) CommandBuffer() string { return m.command.Buffer }

// CommandSuggestion returns the current command-bar suggestion (for tests).
func (m *Model) CommandSuggestion() string { return m.command.Suggestion }

// SearchBuffer returns the current search buffer (for tests).
func (m *Model) SearchBuffer() string { return m.search.Buffer }

// AutoRefresh reports whether auto-refresh is enabled (for tests).
func (m *Model) AutoRefresh() bool { return m.autoRefresh }

// Status returns the current status snapshot (for tests).
func (m *Model) Status() layout.StatusInfo { return m.status }

// Quit reports whether the model has signaled program termination.
func (m *Model) Quit() bool { return m.quit }

// ActiveClient returns the currently connected *kafka.Client (nil before any
// cluster has been selected). Exposed so cmd/kafka-tui can close it on exit.
func (m *Model) ActiveClient() *kafka.Client { return m.client }

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
	switch key.String() {
	case ":":
		m.mode = ModeCommand
		m.command = layout.CommandBar{Active: true, Prefix: ':'}
		m.applySize()
		return m, nil
	case "/":
		m.mode = ModeSearch
		m.search = layout.CommandBar{Active: true, Prefix: '/'}
		m.applySize()
		return m, nil
	case "?":
		m.mode = ModeHelp
		return m, nil
	case "ctrl+r":
		m.SetAutoRefresh(!m.autoRefresh)
		return m, nil
	}
	// forward to active screen first; it may consume q/esc itself
	// (e.g. close an overlay). After the screen handles it we look at the
	// resulting Action; if no screen wants to keep us, q/esc pops the stack.
	cmd := m.forwardToActive(key)
	routeCmd := m.routeActiveAction()
	if cmd == nil && routeCmd == nil {
		switch key.String() {
		case "q":
			// `q` quits at the root, otherwise pops a screen.
			if m.router.Depth() <= 1 {
				m.quit = true
				return m, tea.Quit
			}
			m.popScreen()
			next := m.activeInit()
			return m, next
		case "esc":
			// esc only pops; at the root it's a no-op so users don't quit
			// the app by accident. ctrl+c remains the unconditional exit.
			if m.router.Depth() > 1 {
				m.popScreen()
				next := m.activeInit()
				return m, next
			}
			return m, nil
		}
	}
	return m, teaBatch(cmd, routeCmd)
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

func (m *Model) handleSearchKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		m.mode = ModeNormal
		m.search = layout.CommandBar{}
		m.applySize()
		return m, nil
	case "enter":
		// search delegation lives on individual screens (Task 11+); the root
		// model just collects input and returns to normal mode.
		m.mode = ModeNormal
		m.applySize()
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

	header := layout.Header(
		m.styles,
		m.header,
		m.statusForRender(),
		m.activeKeyHints(),
		layout.Build{Version: m.build.Version, Commit: m.build.Commit},
		m.width,
	)

	bar := m.command
	switch m.mode {
	case ModeSearch:
		bar = m.search
	case ModeCommand, ModeNormal, ModeHelp:
		// keep `m.command` (already empty in normal/help modes).
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
	if s.Now.IsZero() {
		s.Now = m.now()
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

// pushScreen pushes id onto the router stack and constructs the screen
// instance. Used at startup. Discards the resulting Init cmd because the
// caller is expected to invoke [Init] separately.
func (m *Model) pushScreen(id ScreenID) {
	m.active = nil
	m.flash = layout.Flash{}
	m.router.Push(id)
	m.instantiate(id)
	m.applySize()
}

// pushScreenCmd is the runtime variant: pushes a screen, instantiates it, and
// returns its Init cmd to be batched with the host's reply.
func (m *Model) pushScreenCmd(id ScreenID) tea.Cmd {
	m.active = nil
	m.flash = layout.Flash{}
	m.router.Push(id)
	m.instantiate(id)
	m.applySize()
	return m.activeInit()
}

// replaceScreen swaps the active screen for a new id, freeing the previous
// instance.
func (m *Model) replaceScreen(id ScreenID, arg string) tea.Cmd {
	if id == ScreenClusters && arg != "" {
		// `:cluster <name>` connects to a known cluster directly.
		return m.connectCluster(arg)
	}
	m.active = nil
	m.flash = layout.Flash{}
	m.router.Replace(id)
	m.instantiate(id)
	m.applySize()
	return m.activeInit()
}

// popScreen pops the router stack and frees the previously active screen.
// The new active screen (one level up) is re-instantiated from the bootstrap
// data, since we drop instances when leaving them to release Kafka follow
// sessions, table state, etc.
func (m *Model) popScreen() {
	m.active = nil
	m.flash = layout.Flash{}
	m.router.Pop()
	if id := m.router.Active(); id != "" {
		m.instantiate(id)
	}
	m.applySize()
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
