// Package tui hosts the Bubble Tea v2 root model that wires global chrome
// (header, command bar, status bar, key hints) to the screen router.
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
	ModeNormal Mode = iota
	ModeCommand
	ModeSearch
	ModeHelp
)

// Options configure the root model at construction.
type Options struct {
	Cluster      string
	ClusterColor string
	FromCLI      bool

	Initial ScreenID

	Width, Height int

	Build version.BuildInfo

	Now func() time.Time

	Styles theme.Styles

	KeyHints []layout.KeyHint

	// Bootstrap supplies the wiring needed to construct real screens. When
	// nil the host falls back to a placeholder body — used by tests that
	// don't drive screens.
	Bootstrap *Bootstrap
}

// Model is the Bubble Tea root model.
type Model struct {
	router *Router
	mode   Mode

	header              layout.HeaderInfo
	status              layout.StatusInfo
	command             layout.CommandBar
	search              layout.CommandBar
	searchHistories     map[ScreenID]*filterhistory.History
	searchSuggestions   []string
	searchSuggestionIdx int
	hints               []layout.KeyHint

	width, height int

	styles theme.Styles
	now    func() time.Time
	build  version.BuildInfo

	quit bool

	boot *Bootstrap

	client     *kafka.Client
	activeClu  string
	clusterClr string
	clusterRO  bool
	fromCLI    bool

	// connectGen bumps on every connectCluster dispatch and is stamped on
	// the connectResultMsg, so a result that arrives after a newer connect
	// (or after the user navigated away) is dropped instead of swapping the
	// client out from under the active session.
	connectGen uint64
	// connectName is the target of the latest dispatch. A stale result only
	// clears its row when the superseding connect targeted a different
	// cluster — otherwise it would wipe the live "checking…" that a re-issued
	// connect for the same row just set.
	connectName string

	active Screen

	// nav seeds — populated when a screen requests navigation, consumed when
	// the next screen is instantiated.
	navTopic       string
	navPrefill     *kafka.Message
	navGroupFilter string
	// navConfigKey + navConfigValue seed the topic-config edit screen
	// with the key under the cursor and the broker-reported current value.
	navConfigKey   string
	navConfigValue string

	// lastTopic restores the topics screen cursor when navigating back.
	lastTopic string
	// lastConfigKey restores the configs screen cursor after the user
	// returns from the edit screen.
	lastConfigKey string
	// sessionState holds each screen's Stateful snapshot across
	// re-instantiation (push, pop, replace). Cleared on cluster switch —
	// see connectCluster.
	sessionState map[ScreenID]any

	// flashSeenAt tracks the CreatedAt of the last promoted toast so an
	// older or repeated push isn't re-shown.
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
		sessionState:        make(map[ScreenID]any),
		header: layout.HeaderInfo{
			Cluster:      opts.Cluster,
			ClusterColor: opts.ClusterColor,
			FromCLI:      opts.FromCLI,
		},
		status: layout.StatusInfo{
			// no-screen sentinel — [pushScreen] flips this via
			// [syncRefreshStatus] as soon as the first screen mounts.
			Mode: layout.RefreshNotApplicable,
			Now:  now(),
		},
		hints:      hints,
		width:      opts.Width,
		height:     opts.Height,
		styles:     styles,
		now:        now,
		build:      opts.Build,
		boot:       opts.Bootstrap,
		activeClu:  opts.Cluster,
		clusterClr: opts.ClusterColor,
		fromCLI:    opts.FromCLI,
	}
	if opts.Initial != "" {
		m.pushScreen(opts.Initial)
	}
	return m
}

// DefaultKeyHints returns the key hints shown until a screen installs its own.
func DefaultKeyHints() []layout.KeyHint {
	return []layout.KeyHint{
		{Key: ":", Label: "command"},
		{Key: "/", Label: "search"},
		{Key: "?", Label: "help"},
		{Key: "ctrl+r", Label: "refresh interval"},
		{Key: "q", Label: "back/quit"},
	}
}

// Init implements tea.Model. When the clusters screen reports a
// [clusters.Model.SkipTarget] (single cluster or --brokers supplied), the
// host kicks off an async connect to it while the picker stays mounted. The
// topics screen is reached only once connectivity is verified
// (see [Model.handleConnectResult]); a broker that can't be reached leaves
// the user on the picker with the failure, never on an empty topics screen.
func (m *Model) Init() tea.Cmd {
	if cs, ok := m.active.(*clusters.Model); ok {
		if name, skip := cs.SkipTarget(); skip {
			return teaBatch(m.activeInit(), m.connectCluster(name), m.watchConfigSnapshots())
		}
	}
	return teaBatch(m.activeInit(), m.watchConfigSnapshots())
}

func (m *Model) Router() *Router { return m.router }

func (m *Model) Mode() Mode { return m.mode }

func (m *Model) CommandBuffer() string { return m.command.Buffer }

func (m *Model) CommandSuggestion() string { return m.command.Suggestion }

func (m *Model) SearchBuffer() string { return m.search.Buffer }

func (m *Model) SearchSuggestion() string { return m.search.Suggestion }

func (m *Model) Status() layout.StatusInfo { return m.status }

func (m *Model) Quit() bool { return m.quit }

// ActiveClient returns the connected *kafka.Client (nil before any cluster
// has been selected). Exposed so cmd/kafka-tui can close it on exit.
func (m *Model) ActiveClient() *kafka.Client { return m.client }

// syncRefreshStatus mirrors the active screen's refresher state into the
// chrome's Refresh: indicator. The clusters screen is a special case — it
// reloads on filesystem events (config.Watcher) instead of a periodic tick.
func (m *Model) syncRefreshStatus() {
	if _, ok := m.active.(*clusters.Model); ok && m.boot != nil && m.boot.ConfigSnapshots != nil {
		m.status.Mode = layout.RefreshOnEdit
		m.status.Interval = 0
		return
	}
	if m.active == nil || !screenSupportsRefresh(m.active) {
		m.status.Mode = layout.RefreshNotApplicable
		m.status.Interval = 0
		return
	}
	interval := screenRefreshInterval(m.active)
	m.status.Interval = interval
	if interval <= 0 {
		m.status.Mode = layout.RefreshManual
		return
	}
	m.status.Mode = layout.RefreshAuto
}

func (m *Model) SetStatus(s layout.StatusInfo) { m.status = s }

func (m *Model) SetKeyHints(hints []layout.KeyHint) {
	m.hints = hints
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.applySize()
		return m, nil
	case tea.KeyPressMsg:
		newM, cmd := m.handleKey(msg)
		// re-sync chrome after every key so screen-level mutations to the
		// refresher (e.g. the picker confirming a new interval) flow into
		// the status bar without waiting for the next screen transition.
		m.syncRefreshStatus()
		flashCmd := m.promoteFlash()
		return newM, teaBatch(cmd, flashCmd)
	case tea.PasteMsg:
		newM, cmd := m.handlePaste(msg)
		flashCmd := m.promoteFlash()
		return newM, teaBatch(cmd, flashCmd)
	case tea.PasteStartMsg, tea.PasteEndMsg:
		// the full content arrives in tea.PasteMsg; the start/end markers
		// are advisory and ignored.
		return m, nil
	case flashTickMsg:
		flashCmd := m.promoteFlash()
		return m, flashCmd
	case configSnapshotMsg:
		m.handleConfigSnapshot(msg.snapshot)
		return m, teaBatch(m.watchConfigSnapshots(), m.promoteFlash())
	case connectResultMsg:
		cmd := m.handleConnectResult(msg)
		return m, teaBatch(cmd, m.promoteFlash())
	}
	// async screen messages (topic-loaded, fetch-result, …) reach the active
	// screen here; route any resulting Action and re-promote the flash.
	cmd := m.forwardToActive(msg)
	cmd = teaBatch(cmd, m.routeActiveAction(), m.promoteFlash())
	return m, cmd
}

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

// applySize forwards the new geometry (inner area inside the body frame)
// to the active screen.
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
// border (2 rows). Horizontal contribution is in [frameWidthInset].
const frameInset = 2

// frameWidthInset is left border + left padding + right padding + right border.
const frameWidthInset = 2 + 2*layout.FrameSidePadding

// screenSideMargin keeps blank columns between terminal edges and the
// rendered chrome so frame borders don't sit flush against the edge.
const screenSideMargin = 1

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

func (m *Model) bodyWidth() int {
	w := m.screenWidth() - frameWidthInset
	if w < 1 {
		return 0
	}
	return w
}

func (m *Model) screenWidth() int {
	w := m.width - 2*screenSideMargin
	if w < 1 {
		return 0
	}
	return w
}

func (m *Model) activeKeyHints() []layout.KeyHint {
	if m.active == nil {
		return m.hints
	}
	return m.active.KeyHints()
}

func (m *Model) activeView() string {
	if m.active == nil {
		return ""
	}
	return m.active.View()
}

func (m *Model) activeInit() tea.Cmd {
	if m.active == nil {
		return nil
	}
	return m.active.Init()
}

func (m *Model) forwardToActive(msg tea.Msg) tea.Cmd {
	if m.active == nil {
		return nil
	}
	return m.active.Update(msg)
}
