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
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/filterhistory"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
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
	// live filter via [Searchable.SetSearch].
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
	// per-screen filter history. Entries are pushed when the user commits
	// a non-empty query via Enter or Ctrl-E; opening `/` again surfaces
	// the newest match as a ghost suggestion (Tab/Right/Ctrl-F to accept).
	// Up/Down cycle through the matches.
	searchHistories     map[ScreenID]*filterhistory.History
	searchSuggestions   []string
	searchSuggestionIdx int
	hints               []layout.KeyHint

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
	active Screen

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
		router:              router,
		mode:                ModeNormal,
		searchHistories:     make(map[ScreenID]*filterhistory.History),
		searchSuggestionIdx: -1,
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
	if cs, ok := m.active.(*clusters.Model); ok {
		if name, skip := cs.SkipTarget(); skip {
			if connectCmd := m.connectCluster(name); connectCmd != nil {
				return teaBatch(connectCmd, m.watchConfigSnapshots())
			}
		}
	}
	return teaBatch(m.activeInit(), m.watchConfigSnapshots())
}

// Router exposes the underlying router (for tests and bootstrap).
func (m *Model) Router() *Router { return m.router }

// Mode returns the current input mode (for tests).
func (m *Model) Mode() Mode { return m.mode }

// CommandBuffer returns the current command-bar buffer (for tests).
func (m *Model) CommandBuffer() string { return m.command.Buffer }

// CommandSuggestion returns the current command-bar suggestion (for tests).
func (m *Model) CommandSuggestion() string { return m.command.Suggestion }

// SearchBuffer returns the current `/`-prompt buffer (for tests).
func (m *Model) SearchBuffer() string { return m.search.Buffer }

// SearchSuggestion returns the current `/`-prompt ghost suggestion (for tests).
func (m *Model) SearchSuggestion() string { return m.search.Suggestion }

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
		setScreenRefreshPaused(m.active, !on)
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
	if _, ok := m.active.(*clusters.Model); ok && m.boot != nil && m.boot.ConfigSnapshots != nil {
		m.status.Mode = layout.RefreshOnEdit
		m.status.Interval = 0
		return
	}
	if m.active != nil && !screenSupportsRefresh(m.active) {
		// e.g. message detail, produce form, configsrc snapshot — show
		// a dash so the user understands the row isn't applicable.
		m.status.Mode = layout.RefreshNotApplicable
		m.status.Interval = 0
		return
	}
	interval := time.Duration(0)
	if m.active != nil {
		interval = screenRefreshInterval(m.active)
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

// screenSideMargin is the number of blank columns kept between the
// terminal edges and the rendered chrome (header, body frame, command
// line, flash). Without this margin the right border of the frame
// (and the right-aligned build identity in the header) sit flush
// against the terminal edge — visually cramped on wide terminals.
const screenSideMargin = 1

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
// minus the outer side margin, the frame's left/right borders, and side
// padding).
func (m *Model) bodyWidth() int {
	w := m.screenWidth() - frameWidthInset
	if w < 1 {
		return 0
	}
	return w
}

// screenWidth returns the width available to the chrome (header, body
// frame, command line, flash) after the outer [screenSideMargin] is
// subtracted from each side.
func (m *Model) screenWidth() int {
	w := m.width - 2*screenSideMargin
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
