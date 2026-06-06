package tui_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
)

// TestModel_BootstrapRendersClustersBody verifies the host wires the real
// clusters screen instead of the "coming soon" placeholder.
func TestModel_BootstrapRendersClustersBody(t *testing.T) {
	// arrange
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "local", Brokers: []string{"localhost:9092"}}},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: loaded.Clusters,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     100,
		Height:    24,
		Bootstrap: boot,
	})

	// act
	out := m.Render()

	// assert
	assert.NotContains(t, out, "coming soon")
	assert.Contains(t, out, "local")
	assert.Contains(t, out, "localhost:9092")
}

// TestModel_NoBootstrapFallsBackToPlaceholder pins the test path: tests that
// don't supply a Bootstrap continue to see the legacy placeholder body.
func TestModel_NoBootstrapFallsBackToPlaceholder(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics, Width: 80, Height: 24})

	out := m.Render()

	assert.Contains(t, out, "topics — coming soon")
}

// TestSearch_LiveFilterAndCommit drives the full k9s-style filter flow on
// the clusters screen end-to-end: open the prompt with `/`, live-narrow
// rows by typing, commit with enter, and confirm both the title and the
// table reflect the filter.
func TestSearch_LiveFilterAndCommit(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
		{Name: "gamma", Brokers: []string{"g:9092"}},
	})

	// before search — full title and all rows visible.
	out := m.Render()
	assert.Contains(t, out, "Clusters [3]")
	for _, name := range []string{"alpha", "beta", "gamma"} {
		assert.Contains(t, out, name)
	}

	// open prompt and type "be" — live filter narrows to "beta".
	feed(m, "/", 'b', 'e')
	out = m.Render()
	assert.Equal(t, tui.ModeSearch, m.Mode(), "prompt is open")
	assert.Contains(t, out, "Clusters [1/3] </be>", "title shows match count + query")
	assert.Contains(t, out, "beta")
	assert.NotContains(t, out, "alpha")
	assert.NotContains(t, out, "gamma")

	// enter commits, prompt closes, filter stays applied.
	_, _ = m.Update(keyPress("enter"))
	out = m.Render()
	assert.Equal(t, tui.ModeNormal, m.Mode())
	assert.Contains(t, out, "Clusters [1/3] </be>")
	assert.Contains(t, out, "beta")
	assert.NotContains(t, out, "gamma")
}

// TestSearch_ReopenShowsHistoryGhostAndEscClearsFilter pins the k9s flow:
// reopening `/` over an applied filter starts with an empty buffer but
// surfaces the previous query as a ghost suggestion (Tab to accept), and
// esc inside the prompt always drops the applied filter rather than
// restoring it.
func TestSearch_ReopenShowsHistoryGhostAndEscClearsFilter(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
		{Name: "gamma", Brokers: []string{"g:9092"}},
	})

	// apply a filter
	feed(m, "/", 'b', 'e')
	_, _ = m.Update(keyPress("enter"))
	require.Contains(t, m.Render(), "Clusters [1/3] </be>")

	// reopen the prompt — buffer is empty, ghost holds the last query.
	feed(m, "/")
	require.Equal(t, tui.ModeSearch, m.Mode())
	assert.Empty(t, m.SearchBuffer())
	assert.Equal(t, "be", m.SearchSuggestion())

	// esc clears the applied filter wholesale (no restore).
	_, _ = m.Update(keyPress("esc"))
	out := m.Render()
	assert.Equal(t, tui.ModeNormal, m.Mode())
	assert.Contains(t, out, "Clusters [3]")
	assert.NotContains(t, out, "</be>")
	for _, name := range []string{"alpha", "beta", "gamma"} {
		assert.Contains(t, out, name)
	}
}

// TestSearch_TabPromotesGhostAndAppliesLive verifies that after a prior
// committed query lands in history, Tab on the freshly-opened prompt
// promotes the ghost to the buffer and applies the filter live.
func TestSearch_TabPromotesGhostAndAppliesLive(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
		{Name: "gamma", Brokers: []string{"g:9092"}},
	})

	// seed history with "be", then drop the applied filter.
	feed(m, "/", 'b', 'e')
	_, _ = m.Update(keyPress("enter"))
	_, _ = m.Update(keyPress("esc")) // clears the applied filter

	// reopen — ghost surfaces from history; tab promotes it.
	feed(m, "/")
	require.Equal(t, "be", m.SearchSuggestion())
	_, _ = m.Update(keyPress("tab"))

	assert.Equal(t, "be", m.SearchBuffer())
	assert.Empty(t, m.SearchSuggestion())
	out := m.Render()
	assert.Contains(t, out, "Clusters [1/3] </be>", "tab applies the suggestion live")
}

// TestSearch_UpDownCycleHistory confirms Up/Down walk through the
// suggestion list (newest ↔ oldest, wrapping).
func TestSearch_UpDownCycleHistory(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	// seed history with three queries.
	for _, q := range []string{"alpha", "beta", "alp"} {
		feed(m, "/")
		for _, r := range q {
			feed(m, r)
		}
		_, _ = m.Update(keyPress("enter"))
		_, _ = m.Update(keyPress("esc"))
	}

	// reopen — newest ("alp") leads.
	feed(m, "/")
	require.Equal(t, "alp", m.SearchSuggestion())

	// up → next older ("beta")
	_, _ = m.Update(keyPress("up"))
	assert.Equal(t, "beta", m.SearchSuggestion())

	// up → "alpha"
	_, _ = m.Update(keyPress("up"))
	assert.Equal(t, "alpha", m.SearchSuggestion())

	// up → wraps back to newest
	_, _ = m.Update(keyPress("up"))
	assert.Equal(t, "alp", m.SearchSuggestion())

	// down → wraps to oldest
	_, _ = m.Update(keyPress("down"))
	assert.Equal(t, "alpha", m.SearchSuggestion())
}

// TestSearch_RightAndCtrlFPromoteLikeTab pins the Tab/Right/Ctrl-F parity
// from k9s — all three accept the current ghost suggestion identically.
func TestSearch_RightAndCtrlFPromoteLikeTab(t *testing.T) {
	for _, accept := range []string{"right", "ctrl+f"} {
		t.Run(accept, func(t *testing.T) {
			m := newClustersHostWith(t, []config.Cluster{
				{Name: "alpha", Brokers: []string{"a:9092"}},
				{Name: "beta", Brokers: []string{"b:9092"}},
				{Name: "gamma", Brokers: []string{"g:9092"}},
			})

			// seed history with "be"
			feed(m, "/", 'b', 'e')
			_, _ = m.Update(keyPress("enter"))
			_, _ = m.Update(keyPress("esc"))

			// reopen, accept ghost via the alias.
			feed(m, "/")
			require.Equal(t, "be", m.SearchSuggestion())
			_, _ = m.Update(keyPress(accept))

			assert.Equal(t, "be", m.SearchBuffer())
			assert.Empty(t, m.SearchSuggestion())
			assert.Contains(t, m.Render(), "Clusters [1/3] </be>")
		})
	}
}

// TestSearch_CtrlE_MovesToEnd verifies that ctrl+e is now a readline
// move-to-end shortcut (and no longer an enter alias). It should be a no-op
// here since the cursor already sits at the end of the buffer.
func TestSearch_CtrlE_MovesToEnd(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	feed(m, "/", 'b', 'e')
	_, _ = m.Update(keyPress("ctrl+e"))

	// prompt stays open, buffer untouched.
	assert.Equal(t, tui.ModeSearch, m.Mode())
	assert.Equal(t, "be", m.SearchBuffer())
}

// TestSearch_DeleteIsForwardOnly verifies the cursor-aware delete semantics:
// delete with the cursor at end-of-buffer is a no-op (it deletes the rune
// under the cursor, not the one before — that's backspace's job).
func TestSearch_DeleteIsForwardOnly(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	feed(m, "/", 'b', 'e', 'x')
	_, _ = m.Update(keyPress("delete"))

	assert.Equal(t, "bex", m.SearchBuffer(), "delete at end is a no-op")
}

// TestSearch_CtrlA_MovesToStart_DeleteRemovesForward verifies forward delete
// when the cursor is positioned earlier in the buffer.
func TestSearch_CtrlA_MovesToStart_DeleteRemovesForward(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	feed(m, "/", 'b', 'e', 'x')
	_, _ = m.Update(keyPress("ctrl+a"))
	_, _ = m.Update(keyPress("delete"))

	assert.Equal(t, "ex", m.SearchBuffer(), "ctrl+a then delete removes first rune")
}

// TestSearch_CtrlUAndCtrlWClearBuffer pins the readline-style line-wipe
// shortcuts. ctrl+u kills from the start of the line to the cursor; ctrl+w
// kills the word back. Both clear a single-word buffer like "be" — the
// distinction surfaces on multi-word buffers (covered separately).
func TestSearch_CtrlUAndCtrlWClearBuffer(t *testing.T) {
	for _, wipe := range []string{"ctrl+u", "ctrl+w"} {
		t.Run(wipe, func(t *testing.T) {
			m := newClustersHostWith(t, []config.Cluster{
				{Name: "alpha", Brokers: []string{"a:9092"}},
				{Name: "beta", Brokers: []string{"b:9092"}},
			})

			feed(m, "/", 'b', 'e')
			require.Contains(t, m.Render(), "Clusters [1/2] </be>")

			_, _ = m.Update(keyPress(wipe))

			assert.Empty(t, m.SearchBuffer())
			// filter applied live → table back to full count.
			assert.Contains(t, m.Render(), "Clusters [2]")
		})
	}
}

// TestSearch_PasteInsertsAndAppliesFilter verifies the bracketed-paste path:
// a tea.PasteMsg arriving while the filter prompt is open must land in the
// buffer, sanitize newlines, refresh the suggestion, and live-apply the filter.
func TestSearch_PasteInsertsAndAppliesFilter(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	feed(m, "/")
	_, _ = m.Update(tea.PasteMsg{Content: "be\ntail"})

	assert.Equal(t, "be tail", m.SearchBuffer(),
		"paste lands in the buffer and newlines collapse to spaces")
	// the live-applied filter renders the sanitized buffer in the header
	// regardless of whether any row matches.
	assert.Contains(t, m.Render(), "</be tail>")
}

// TestCommand_PasteInsertsAndRefreshesSuggestion verifies paste into the
// command bar.
func TestCommand_PasteInsertsAndRefreshesSuggestion(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
	})

	feed(m, ":")
	_, _ = m.Update(tea.PasteMsg{Content: "topi\x1b[31m"})

	// escape sequences must be filtered out before they reach the buffer
	// (otherwise rendering would inject them and corrupt terminal state).
	assert.Equal(t, "topi[31m", m.CommandBuffer())
}

// TestSearch_CtrlW_KillsOnlyLastWord verifies the readline word-boundary
// semantics: with a multi-word buffer, ctrl+w trims only the trailing word
// rather than wiping everything (which is ctrl+u's job).
func TestSearch_CtrlW_KillsOnlyLastWord(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha-prod", Brokers: []string{"a:9092"}},
		{Name: "alpha-staging", Brokers: []string{"s:9092"}},
	})

	feed(m, "/", 'a', 'l', 'p', 'h', 'a', ' ', 'p', 'r', 'o', 'd')
	_, _ = m.Update(keyPress("ctrl+w"))

	assert.Equal(t, "alpha ", m.SearchBuffer(),
		"ctrl+w must kill only the trailing word, leaving the preceding whitespace")
}

// TestSearch_BackspaceRecomputesSuggestion verifies the live suggestion
// recompute path: shrinking the buffer must surface a different
// (broader-prefix) match from history.
func TestSearch_BackspaceRecomputesSuggestion(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	// seed two history entries.
	for _, q := range []string{"alpha", "beta"} {
		feed(m, "/")
		for _, r := range q {
			feed(m, r)
		}
		_, _ = m.Update(keyPress("enter"))
		_, _ = m.Update(keyPress("esc"))
	}

	// reopen, type "a" — only "alpha" matches.
	feed(m, "/", 'a')
	assert.Equal(t, "alpha", m.SearchSuggestion())

	// type "x" — no match.
	feed(m, 'x')
	assert.Empty(t, m.SearchSuggestion())

	// backspace removes "x" — "alpha" surfaces again.
	_, _ = m.Update(keyPress("backspace"))
	assert.Equal(t, "alpha", m.SearchSuggestion())
}

// TestSearch_EscDoesNotPushToHistory pins the asymmetric semantics:
// only Enter (and Ctrl-E) commit to history; Esc walks away cleanly.
func TestSearch_EscDoesNotPushToHistory(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	// type and abandon via esc — must not enter history.
	feed(m, "/", 'b', 'e')
	_, _ = m.Update(keyPress("esc"))

	// reopen — no ghost, nothing was pushed.
	feed(m, "/")
	assert.Empty(t, m.SearchSuggestion(), "esc must not commit to history")
}

// TestSearch_HistoryIsPerScreen ensures history buckets don't bleed across
// unrelated screens — a query committed on clusters must not surface on
// topics, and vice versa.
func TestSearch_HistoryIsPerScreen(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")
	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool { return strings.Contains(m.Render(), "orders") })

	// commit a query on the topics screen.
	feed(m, "/", 'o', 'r', 'd')
	_, _ = m.Update(keyPress("enter"))
	_, _ = m.Update(keyPress("esc"))

	// pop back to the clusters screen.
	_, cmd := m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Clusters")

	// open `/` on clusters — no ghost from the topics history.
	feed(m, "/")
	assert.Empty(t, m.SearchSuggestion(),
		"clusters history is independent from topics history")
}

// TestEsc_AtRootClearsFilterWithoutQuitting verifies the root-depth
// behavior after the k9s parity change: at the cluster picker, esc
// with an active filter wipes the filter; the would-be pop is a no-op
// because the cluster screen is the root, so the app must not quit.
func TestEsc_AtRootClearsFilterWithoutQuitting(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	feed(m, "/", 'b')
	_, _ = m.Update(keyPress("enter"))
	require.Contains(t, m.Render(), "Clusters [1/2] </b>")
	require.False(t, m.Quit())

	_, _ = m.Update(keyPress("esc"))
	out := m.Render()
	assert.NotContains(t, out, "</b>")
	assert.Contains(t, out, "Clusters [2]")
	assert.False(t, m.Quit(), "esc at root must never quit the app")
}

// TestCtrlU_ClearsActiveFilter pins the k9s clearCmd contract: with a
// filter applied, ctrl+u wipes it and stays on the screen — never pops.
func TestCtrlU_ClearsActiveFilter(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	feed(m, "/", 'b')
	_, _ = m.Update(keyPress("enter"))
	require.Contains(t, m.Render(), "Clusters [1/2] </b>")

	_, _ = m.Update(keyPress("ctrl+u"))
	out := m.Render()
	assert.NotContains(t, out, "</b>")
	assert.Contains(t, out, "Clusters [2]")
}

// TestConfigSnapshot_UpdatesBootAndToastsOnClusters drives an end-to-end
// reload via the file watcher: a Snapshot delivered through the bootstrap
// channel must update the host's cluster list, refresh the on-screen
// table, and surface a success toast in the global flash bar.
func TestConfigSnapshot_UpdatesBootAndToastsOnClusters(t *testing.T) {
	snapshots := make(chan config.Snapshot, 1)
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
	}
	boot := &tui.Bootstrap{
		Loaded:          loaded,
		Clusters:        loaded.Clusters,
		ConfigSnapshots: snapshots,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     100,
		Height:    24,
		Bootstrap: boot,
	})

	// before — only "alpha" is visible.
	require.Contains(t, m.Render(), "alpha")
	require.NotContains(t, m.Render(), "delta")

	// push the fresh snapshot, then close so the re-armed listener cmd
	// sees ok=false and returns nil instead of blocking forever in the
	// drain loop.
	freshLoaded := &config.Loaded{
		Config: config.Defaults(),
		Clusters: []config.Cluster{
			{Name: "alpha", Brokers: []string{"a:9092"}},
			{Name: "delta", Brokers: []string{"d:9092"}},
		},
	}
	snapshots <- config.Snapshot{Loaded: freshLoaded}
	close(snapshots)

	// drain Init's batched cmds — the watch-snapshots cmd will surface
	// the configSnapshotMsg, which Update will apply.
	drainCmd(t, m, m.Init())

	out := m.Render()
	assert.Contains(t, out, "alpha")
	assert.Contains(t, out, "delta", "new cluster from snapshot must appear")
	assert.Contains(t, out, "clusters refreshed · 2", "success toast must surface in the flash bar")
}

// ----- helpers -----

// newClustersHostWith builds a host wired to the real clusters screen
// with the supplied list and a no-op pinger. Sized large enough that the
// frame and chrome render normally.
func newClustersHostWith(t *testing.T, list []config.Cluster) *tui.Model {
	t.Helper()
	loaded := &config.Loaded{Config: config.Defaults(), Clusters: list}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: list,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	}
	return tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})
}

// feed sends each key in order. Single-rune strings become text presses;
// named keys (`enter`, `esc`, `/`, …) are routed through keyPress.
func feed(m *tui.Model, keys ...any) {
	for _, k := range keys {
		switch v := k.(type) {
		case rune:
			_, _ = m.Update(keyPressRune(v))
		case string:
			_, _ = m.Update(keyPress(v))
		}
	}
}

// drainCmd executes the cmd, feeds any resulting tea.Msg back into
// m.Update, and repeats until the queue is empty. tea.Batch results are
// unpacked into their constituent cmds. Bounded to avoid an accidental
// infinite loop in case a screen schedules a self-perpetuating tick
// (auto-refresh, follow, etc.).
//
// Each cmd is invoked in a goroutine with a short deadline so timer-based
// cmds (tea.Tick for refresh ticks) don't stall the test for 30s. Anything
// that doesn't return a msg promptly is treated as a deferred tick and
// dropped — production code keeps the original tick alive in the runtime,
// but tests only need the immediate, data-bearing cmds.
func drainCmd(t *testing.T, m *tui.Model, cmd tea.Cmd) {
	t.Helper()
	const (
		maxSteps    = 64
		stepTimeout = 500 * time.Millisecond
	)
	queue := []tea.Cmd{cmd}
	for steps := 0; steps < maxSteps && len(queue) > 0; steps++ {
		c := queue[0]
		queue = queue[1:]
		if c == nil {
			continue
		}
		msgCh := make(chan tea.Msg, 1)
		go func() { msgCh <- c() }()
		var msg tea.Msg
		select {
		case msg = <-msgCh:
		case <-time.After(stepTimeout):
			// cmd is a deferred timer (tea.Tick); skip — its msg would
			// arrive too late to matter for an integration assertion.
			continue
		}
		if msg == nil {
			continue
		}
		// auto-refresh ticks are noise once we've already pumped the data
		// fetch; drop them so we don't loop on the rescheduled tick.
		if isRefreshTick(msg) {
			continue
		}
		// tea.Batch encodes its parts as a BatchMsg; unpack instead of
		// feeding it back to Update (which doesn't know about it).
		if batch, ok := msg.(tea.BatchMsg); ok {
			queue = append(queue, batch...)
			continue
		}
		_, next := m.Update(msg)
		if next != nil {
			queue = append(queue, next)
		}
	}
}

// isRefreshTick reports whether msg is one of the recurring auto-refresh
// ticks. Drained tests can't usefully process them and would otherwise
// spin forever (the tick reschedules itself).
func isRefreshTick(msg tea.Msg) bool {
	name := strings.ToLower(fmt.Sprintf("%T", msg))
	return strings.Contains(name, "refreshtick") ||
		strings.Contains(name, "followtick") ||
		strings.Contains(name, "flashtick")
}

// startKfake spins up an in-process fake Kafka cluster and returns a
// config.Cluster pointing at it. The cluster is closed at test end.
func startKfake(t *testing.T) config.Cluster {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return config.Cluster{
		Name:    "kfake",
		Brokers: cluster.ListenAddrs(),
	}
}

// newConnectedHost builds a host wired to a real kfake-backed Dialer with
// the given cluster as the only entry. The host is sized large and starts
// on the clusters screen — callers drive enter/connect via key events.
func newConnectedHost(t *testing.T, cluster config.Cluster) *tui.Model {
	t.Helper()
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{cluster},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: []config.Cluster{cluster},
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
		Connector: tui.NewKafkaConnector(tui.DialerFunc(func(c config.Cluster) (*kafka.Client, error) {
			return kafka.Dial(c, kafka.DialOptions{ClientID: "kafka-tui-test"})
		}), 5*time.Second),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})
	t.Cleanup(func() {
		if c := m.ActiveClient(); c != nil {
			c.Close()
		}
	})
	return m
}

// connectActive drives enter on the cluster row and pumps the resulting
// connect cmd. After return the host is on the topics screen with a live
// *kafka.Client.
func connectActive(t *testing.T, m *tui.Model) {
	t.Helper()
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.NotNil(t, m.ActiveClient(), "connect must produce a live client")
}

// TestCommand_LogsAndConfigSourcesPushScreens drives `:logs` and
// `:config sources` through the command bar so the host instantiates the
// corresponding screens. Exercises replaceScreen, newLogs, newConfigSrc,
// and the matching dispatch wrappers — none of which need a Kafka client.
func TestCommand_LogsAndConfigSourcesPushScreens(t *testing.T) {
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "kafka-tui.log")
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
		Sources: config.Sources{
			Config:   map[string]config.Source{},
			Clusters: map[string]map[string]config.Source{},
		},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: loaded.Clusters,
		LogPath:  logPath,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})

	// `:logs` → replaceScreen(ScreenLogs).
	feed(m, ":", 'l', 'o', 'g', 's')
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	out := m.Render()
	assert.Contains(t, out, "Logs", "logs screen title must surface")

	// `:config sources` → replaceScreen(ScreenConfigSrc).
	feed(m, ":", 'c', 'o', 'n', 'f', 'i', 'g', ' ', 's', 'o', 'u', 'r', 'c', 'e', 's')
	_, cmd = m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	out = m.Render()
	assert.Contains(t, out, "Config Sources")
}

func TestCommand_RequiresClientGuardsClusterScreens(t *testing.T) {
	for _, name := range []string{"topics", "groups"} {
		t.Run(name, func(t *testing.T) {
			// arrange — fresh model per case so a leftover toast from an
			// earlier run can't satisfy this run's assertion via flash echo.
			m := newGuardHost(t)

			// act
			feed(m, ":")
			for _, r := range name {
				feed(m, r)
			}
			_, cmd := m.Update(keyPress("enter"))
			drainCmd(t, m, cmd)

			// assert
			assert.Equal(t, tui.ModeNormal, m.Mode(), "command prompt must close")
			assert.Equal(t, tui.ScreenClusters, m.Router().Active(), "must not navigate away from clusters")
			out := m.Render()
			assert.Contains(t, out, "connect to a cluster first", "guard toast must surface in the flash bar")
			assert.NotContains(t, out, "coming soon", "must not leave the user on a placeholder")
		})
	}
}

// TestCommand_RequiresClientGuardsFallbackFromConfigSources pins the fallback
// path for screens without a toast queue: typing `:topics` from the configsrc
// screen (which can't surface toasts) must redirect the user back to the
// clusters picker with the guard warning attached, instead of swallowing it
// silently.
func TestCommand_RequiresClientGuardsFallbackFromConfigSources(t *testing.T) {
	// arrange — start on clusters, then jump to configsrc via `:config sources`.
	m := newGuardHost(t)
	feed(m, ":", 'c', 'o', 'n', 'f', 'i', 'g', ' ', 's', 'o', 'u', 'r', 'c', 'e', 's')
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.Equal(t, tui.ScreenConfigSrc, m.Router().Active(), "precondition: must reach configsrc")

	// act — type `:topics` from configsrc with no client connected.
	feed(m, ":", 't', 'o', 'p', 'i', 'c', 's')
	_, cmd = m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)

	// assert — guard must bounce us back to clusters and surface the warning.
	assert.Equal(t, tui.ModeNormal, m.Mode())
	assert.Equal(t, tui.ScreenClusters, m.Router().Active(), "guard must redirect to clusters when active screen has no toast queue")
	assert.Contains(t, m.Render(), "connect to a cluster first")
}

func newGuardHost(t *testing.T) *tui.Model {
	t.Helper()
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
		Sources: config.Sources{
			Config:   map[string]config.Source{},
			Clusters: map[string]map[string]config.Source{},
		},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: loaded.Clusters,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	}
	return tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})
}

// TestConnect_DialsAndPushesTopics drives the cluster→topics connect path
// against a real kfake broker, exercising connectCluster, replaceScreen,
// instantiate(topics), updateHeaderForActive, findCluster, newTopics, and
// the topics screen lifecycle.
func TestConnect_DialsAndPushesTopics(t *testing.T) {
	cluster := startKfake(t)
	m := newConnectedHost(t, cluster)
	connectActive(t, m)

	out := m.Render()
	// the topics screen is now active — title carries the count badge,
	// and the cluster name is in the header chrome.
	assert.Contains(t, out, "Topics")
	assert.Contains(t, out, cluster.Name)
	// status snapshot reflects the topics screen's 30s default refresh.
	assert.NotZero(t, m.Status().Interval)
}

// TestConnect_UnknownClusterIsNoop pins findCluster's nil branch — when
// the requested name isn't in the bootstrap list, connectCluster returns
// nil without touching the connector.
func TestConnect_UnknownClusterIsNoop(t *testing.T) {
	connected := false
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: loaded.Clusters,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
		Connector: tui.ConnectorFunc(func(_ config.Cluster) (*kafka.Client, error) {
			connected = true
			return nil, errors.New("should not be called")
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})

	// `:cluster missing` resolves to ScreenClusters with arg "missing"
	// → replaceScreen short-circuits to connectCluster, which fails to
	// find the cluster and returns nil.
	feed(m, ":", 'c', 'l', 'u', 's', 't', 'e', 'r', ' ', 'm', 'i', 's', 's', 'i', 'n', 'g')
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)

	assert.False(t, connected, "connector must not run for unknown cluster")
	assert.Nil(t, m.ActiveClient())
}

// TestConnect_FailureLandsOnPickerWithToast pins the connect-failure branch:
// a broker that fails the connectivity gate leaves the user on the clusters
// picker with the reason, never on an empty topics screen.
func TestConnect_FailureLandsOnPickerWithToast(t *testing.T) {
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: loaded.Clusters,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
		Connector: tui.ConnectorFunc(func(_ config.Cluster) (*kafka.Client, error) {
			return nil, errors.New("ping: broker unreachable")
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})

	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)

	out := m.Render()
	assert.Contains(t, out, "broker unreachable")
	assert.Contains(t, out, "Clusters", "must stay on the picker, not a topics screen")
	assert.Nil(t, m.ActiveClient())
}

// TestConnect_AutoSelectFailureLandsOnPicker pins the startup auto-connect
// path: an unreachable AutoSelectCluster must leave the user on the picker
// with the failure, not on an empty topics list.
func TestConnect_AutoSelectFailureLandsOnPicker(t *testing.T) {
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
	}
	boot := &tui.Bootstrap{
		Loaded:            loaded,
		Clusters:          loaded.Clusters,
		AutoSelectCluster: "alpha",
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
		Connector: tui.ConnectorFunc(func(_ config.Cluster) (*kafka.Client, error) {
			return nil, errors.New("ping: broker unreachable")
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})

	drainCmd(t, m, m.Init())

	out := m.Render()
	assert.Contains(t, out, "broker unreachable")
	assert.Contains(t, out, "Clusters", "auto-connect failure must land on the picker")
	assert.Nil(t, m.ActiveClient())
}

// TestConnect_AutoSelectSuccessReachesTopics pins the happy startup path:
// a reachable AutoSelectCluster connects and mounts the topics screen
// without the user touching the picker.
func TestConnect_AutoSelectSuccessReachesTopics(t *testing.T) {
	cluster := startKfake(t)
	cluster.Name = "alpha"
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{cluster},
	}
	boot := &tui.Bootstrap{
		Loaded:            loaded,
		Clusters:          loaded.Clusters,
		AutoSelectCluster: "alpha",
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
		Connector: tui.NewKafkaConnector(tui.DialerFunc(func(c config.Cluster) (*kafka.Client, error) {
			return kafka.Dial(c, kafka.DialOptions{ClientID: "kafka-tui-test"})
		}), 5*time.Second),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})
	t.Cleanup(func() {
		if c := m.ActiveClient(); c != nil {
			c.Close()
		}
	})

	drainCmd(t, m, m.Init())

	assert.Contains(t, m.Render(), "Topics")
	assert.NotNil(t, m.ActiveClient())
}

// TestRoute_TopicsToMessagesAndBack drives the host's topics→messages→back
// chain. Hits routeTopicsAction, pushScreenCmd(messages), newMessages,
// messages screen and routeMessagesAction.
func TestRoute_TopicsToMessagesAndBack(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)

	// wait for the topics list to load — pump non-tick async messages
	// until the table actually has the topic row.
	settleUntil(t, m, func() bool { return strings.Contains(m.Render(), "orders") })

	// enter on the selected row → routeTopicsAction sets navTopic and
	// pushes the messages screen.
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	out := m.Render()
	assert.Contains(t, out, "orders", "messages screen title must include topic")

	// q/esc on messages list → routeMessagesAction.Back → popScreen.
	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	out = m.Render()
	assert.Contains(t, out, "Topics", "popping must restore topics screen")
}

// TestRoute_TopicsFilterSurvivesMessagesRoundTrip pins the user-facing
// contract: a `/` filter applied on the topics screen must remain applied
// after the user drills into a message and pops back. Without the
// lastFilters round-trip (closeActive saves, instantiate restores) the
// new topics instance lands unfiltered and the user has to re-type.
func TestRoute_TopicsFilterSurvivesMessagesRoundTrip(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")
	mustCreateTopic(t, cluster, "events")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool {
		out := m.Render()
		return strings.Contains(out, "orders") && strings.Contains(out, "events")
	})

	// apply filter "ord" — narrows to a single row.
	feed(m, "/", 'o', 'r', 'd')
	_, _ = m.Update(keyPress("enter"))
	require.Contains(t, m.Render(), "Topics [1/2] </ord>")

	// push the messages screen via enter on the filtered row.
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Messages")

	// pop back to topics; filter must still be applied once the refresh
	// from the re-instantiated screen lands its data.
	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	settleUntil(t, m, func() bool {
		return strings.Contains(m.Render(), "Topics [1/2] </ord>")
	})
	out := m.Render()
	assert.Contains(t, out, "Topics [1/2] </ord>", "filter must survive push/pop")
	assert.NotContains(t, out, "events", "non-matching rows must remain hidden")
}

// TestRoute_ClearedFilterStaysClearedAcrossRoundTrip pins the inverse of
// the survive-round-trip contract: an explicit clear (ctrl+u) must not
// be undone by a stale sessionState entry from an earlier save. The
// asymmetric "skip writes on empty filter" logic in screenSnapshot
// would otherwise leave the previously-saved query in the map, and the
// next pop would resurrect it.
func TestRoute_ClearedFilterStaysClearedAcrossRoundTrip(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")
	mustCreateTopic(t, cluster, "events")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool {
		out := m.Render()
		return strings.Contains(out, "orders") && strings.Contains(out, "events")
	})

	// apply filter "ord", push messages, pop back — filter is now stored
	// in sessionState[ScreenTopics].
	feed(m, "/", 'o', 'r', 'd')
	_, _ = m.Update(keyPress("enter"))
	require.Contains(t, m.Render(), "Topics [1/2] </ord>")
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Messages")
	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	settleUntil(t, m, func() bool {
		return strings.Contains(m.Render(), "Topics [1/2] </ord>")
	})

	// user explicitly clears the filter with ctrl+u (k9s "wipe but stay").
	_, _ = m.Update(keyPress("ctrl+u"))
	require.Contains(t, m.Render(), "Topics [2]", "ctrl+u must clear the filter inline")

	// push the messages screen again and pop back. The expectation is the
	// cleared state survives; without delete-on-empty in closeActive the
	// previously-saved "ord" would resurrect here.
	_, cmd = m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Messages")
	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	settleUntil(t, m, func() bool {
		out := m.Render()
		return strings.Contains(out, "orders") && strings.Contains(out, "events")
	})

	out := m.Render()
	assert.Contains(t, out, "Topics [2]", "filter must remain cleared after round-trip")
	assert.NotContains(t, out, "</ord>", "no stale filter must resurrect")
	assert.Contains(t, out, "events", "non-matching rows visible again")
}

// TestConnect_DialClearsFilters pins the cluster-boundary rule: each
// cluster is its own session, so a filter saved against the previous
// cluster's topic set must NOT bleed into the next cluster — otherwise
// the user lands on what looks like an empty list when nothing in the
// new cluster matches the stale query.
func TestConnect_DialClearsFilters(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")
	mustCreateTopic(t, cluster, "events")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool {
		out := m.Render()
		return strings.Contains(out, "orders") && strings.Contains(out, "events")
	})

	// filter topics to a single match.
	feed(m, "/", 'o', 'r', 'd')
	_, _ = m.Update(keyPress("enter"))
	require.Contains(t, m.Render(), "Topics [1/2] </ord>")

	// jump back to the clusters list and re-connect — connectCluster's
	// clear(lastFilters) wipes the stale filter so the re-instantiated
	// topics screen renders unfiltered.
	feed(m, ":", 'c', 'l', 'u', 's', 't', 'e', 'r', 's')
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Clusters")

	connectActive(t, m)
	settleUntil(t, m, func() bool {
		return strings.Contains(m.Render(), "Topics [2]")
	})
	out := m.Render()
	assert.Contains(t, out, "Topics [2]", "filter must reset on cluster (re)connect")
	assert.Contains(t, out, "events", "all rows visible again")
}

// TestConnect_DirectClusterCommandClearsFilters covers the `:cluster <name>`
// shortcut path. That path bypasses the clusters screen entirely — the
// active topics screen is still mounted when connectCluster runs — so any
// filter applied to it would otherwise leak into the new cluster's topics
// list when closeActive saved it AFTER clear(lastFilters) ran.
func TestConnect_DirectClusterCommandClearsFilters(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")
	mustCreateTopic(t, cluster, "events")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool {
		out := m.Render()
		return strings.Contains(out, "orders") && strings.Contains(out, "events")
	})

	feed(m, "/", 'o', 'r', 'd')
	_, _ = m.Update(keyPress("enter"))
	require.Contains(t, m.Render(), "Topics [1/2] </ord>")

	// `:cluster kfake` reconnects directly — never visits the clusters
	// screen. The filter "ord" must be wiped before the new topics screen
	// mounts.
	feed(m, ":", 'c', 'l', 'u', 's', 't', 'e', 'r', ' ', 'k', 'f', 'a', 'k', 'e')
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)

	settleUntil(t, m, func() bool {
		return strings.Contains(m.Render(), "Topics [2]")
	})
	out := m.Render()
	assert.Contains(t, out, "Topics [2]", "filter must reset on direct :cluster <name>")
	assert.Contains(t, out, "events", "all rows visible again")
}

// TestRoute_MessagesToGroupsAndBack drives the messages→groups action via
// the `g` hotkey, exercising routeMessagesAction.Groups, newGroups, and
// the groups screen (filtered by topic). q pops back to messages.
func TestRoute_MessagesToGroupsAndBack(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool { return strings.Contains(m.Render(), "orders") })

	// drill into messages
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)

	// `g` pushes the consumer-groups screen filtered by the current topic.
	_, cmd = m.Update(keyPressRune('g'))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Consumer Groups")

	// q → routeGroupsAction.Back → pop back to messages.
	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	out := m.Render()
	assert.Contains(t, out, "Messages", "popping must restore messages screen")
	assert.NotContains(t, out, "Consumer Groups", "groups screen must not still be active")
}

// TestRoute_MessagesToProduce drives the messages→produce action via the
// `p` hotkey, exercising routeMessagesAction.Produce, newProduce, and
// the produce screen.
func TestRoute_MessagesToProduce(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "events")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool { return strings.Contains(m.Render(), "events") })

	// drill into messages
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)

	// `p` opens the produce form; after the push the host is on the
	// produce screen, which is WantsRawInput=true.
	_, cmd = m.Update(keyPressRune('p'))
	drainCmd(t, m, cmd)
	out := m.Render()
	assert.Contains(t, strings.ToLower(out), "produce", "produce form must render")
}

// TestReloadClusters_PushesFreshList drives `r` on the clusters screen
// against a Bootstrap whose ConfigReloader returns an extended list.
// Exercises routeClustersAction's Reload branch and reloadClusters.
func TestReloadClusters_PushesFreshList(t *testing.T) {
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
	}
	freshLoaded := &config.Loaded{
		Config: config.Defaults(),
		Clusters: []config.Cluster{
			{Name: "alpha", Brokers: []string{"a:9092"}},
			{Name: "beta", Brokers: []string{"b:9092"}},
		},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: loaded.Clusters,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
		ConfigReloader: func() (*config.Loaded, error) {
			return freshLoaded, nil
		},
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})

	_, cmd := m.Update(keyPressRune('r'))
	drainCmd(t, m, cmd)

	out := m.Render()
	assert.Contains(t, out, "beta", "freshly-loaded cluster must surface")
	assert.Contains(t, out, "refreshed · 2 clusters", "success toast must surface")
}

// TestReloadClusters_NilReloaderWarnsUser pins the "no reloader wired"
// branch — the user gets an explicit toast instead of silent failure.
func TestReloadClusters_NilReloaderWarnsUser(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
	})

	_, cmd := m.Update(keyPressRune('r'))
	drainCmd(t, m, cmd)

	assert.Contains(t, m.Render(), "reload not configured")
}

// TestReloadClusters_ErrorSurfacesToast pins the reloader-failure branch.
func TestReloadClusters_ErrorSurfacesToast(t *testing.T) {
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: loaded.Clusters,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
		ConfigReloader: func() (*config.Loaded, error) {
			return nil, errors.New("yaml: bad indent")
		},
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})

	_, cmd := m.Update(keyPressRune('r'))
	drainCmd(t, m, cmd)

	assert.Contains(t, m.Render(), "yaml: bad indent")
}

// TestConfigSnapshot_ActiveClusterChangedWarns pins the warning-toast
// path through activeToastQueue. With ActiveClusterChanged=true and the
// cluster list otherwise unchanged, the host must surface the
// "reconnect to apply" warning even though no clusters were added or
// removed.
func TestConfigSnapshot_ActiveClusterChangedWarns(t *testing.T) {
	snapshots := make(chan config.Snapshot, 1)
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
	}
	boot := &tui.Bootstrap{
		Loaded:          loaded,
		Clusters:        loaded.Clusters,
		ConfigSnapshots: snapshots,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})

	freshLoaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
	}
	snapshots <- config.Snapshot{Loaded: freshLoaded, ActiveClusterChanged: true}
	close(snapshots)

	drainCmd(t, m, m.Init())

	out := m.Render()
	assert.Contains(t, out, "reconnect to apply",
		"ActiveClusterChanged must produce a warning toast")
}

// TestConfigSnapshot_ParseErrorSurfacesToast pins the snap.Err branch.
func TestConfigSnapshot_ParseErrorSurfacesToast(t *testing.T) {
	snapshots := make(chan config.Snapshot, 1)
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
	}
	boot := &tui.Bootstrap{
		Loaded:          loaded,
		Clusters:        loaded.Clusters,
		ConfigSnapshots: snapshots,
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})

	snapshots <- config.Snapshot{Err: errors.New("yaml: malformed")}
	close(snapshots)

	drainCmd(t, m, m.Init())

	assert.Contains(t, m.Render(), "yaml: malformed")
}

// TestRoute_TopicsToGroupsConfigsAndProduce drives all four navigation
// branches out of routeTopicsAction in a single connected session, plus
// the corresponding back-pop branches in routeGroupsAction,
// routeTopicConfigsAction, and routeProduceAction.
func TestRoute_TopicsToGroupsConfigsAndProduce(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool { return strings.Contains(m.Render(), "orders") })

	// `g` → push consumer-groups (filtered by topic). Exercises
	// routeTopicsAction.Groups, newGroups, groups screen.
	_, cmd := m.Update(keyPressRune('g'))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Consumer Groups")

	// q → routeGroupsAction.Back → pop back to topics.
	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Topics")

	// `c` → push topic-configs screen. Exercises routeTopicsAction.Configs,
	// newTopicConfigs, topic configs screen.
	_, cmd = m.Update(keyPressRune('c'))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Configs")

	// q → routeTopicConfigsAction.Back → pop.
	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Topics")

	// `p` → push produce form. Exercises routeTopicsAction.Produce,
	// newProduce, produce screen.
	_, cmd = m.Update(keyPressRune('p'))
	drainCmd(t, m, cmd)
	require.Contains(t, strings.ToLower(m.Render()), "produce")

	// produce form is WantsRawInput=true, so esc reaches it as a literal;
	// in NORMAL field mode it sets Action.Back → routeProduceAction → pop.
	_, cmd = m.Update(keyPress("esc"))
	drainCmd(t, m, cmd)
	assert.Contains(t, m.Render(), "Topics")
}

// TestRoute_ProduceSuccessForwardsToastToTopics pins the send & close path:
// the produce screen's success toast is captured before the screen is
// popped and re-pushed onto the underlying topics screen, so the user sees
// a confirmation in the global flash bar even though the form is gone.
func TestRoute_ProduceSuccessForwardsToastToTopics(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool { return strings.Contains(m.Render(), "orders") })

	// `p` opens the produce form bound to the focused topic.
	_, cmd := m.Update(keyPressRune('p'))
	drainCmd(t, m, cmd)
	require.Contains(t, strings.ToLower(m.Render()), "produce")

	// `s` opens the send confirm; `y` commits and closes — async produce
	// result drains through the host, the screen pops, and the success
	// toast must land on the topics screen's queue.
	_, cmd = m.Update(keyPressRune('s'))
	drainCmd(t, m, cmd)
	_, cmd = m.Update(keyPressRune('y'))
	drainCmd(t, m, cmd)

	out := m.Render()
	assert.Contains(t, out, "Topics", "topics screen must be active after pop")
	assert.Contains(t, out, "Sent to orders", "success toast must surface on the underlying screen")
}

// TestRoute_LogsAndConfigSrcBackPops drives back/quit on the logs and
// config-sources screens to exercise the Back branches of
// routeLogsAction and routeConfigSrcAction.
func TestRoute_LogsAndConfigSrcBackPops(t *testing.T) {
	tmp := t.TempDir()
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "alpha", Brokers: []string{"a:9092"}}},
		Sources: config.Sources{
			Config:   map[string]config.Source{},
			Clusters: map[string]map[string]config.Source{},
		},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: loaded.Clusters,
		LogPath:  filepath.Join(tmp, "kafka-tui.log"),
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     120,
		Height:    30,
		Bootstrap: boot,
	})

	feed(m, ":", 'l', 'o', 'g', 's')
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Logs")
	require.Equal(t, 1, m.Router().Depth())

	// q on logs at depth=1 with no connected cluster → routeLogsAction.Back
	// → popOrReplaceToHome → replace with clusters. Without that fallback
	// the screen would pop into nothing and render "(no screen active)".
	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	assert.Contains(t, m.Render(), "Clusters")
	assert.NotContains(t, m.Render(), "no screen active")

	// reopen on `:config sources` and exercise the same Back path.
	feed(m, ":", 'c', 'o', 'n', 'f', 'i', 'g', ' ', 's', 'o', 'u', 'r', 'c', 'e', 's')
	_, cmd = m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Config Sources")

	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
	assert.Contains(t, m.Render(), "Clusters")
	assert.NotContains(t, m.Render(), "no screen active")
}

// TestRoute_GroupsBackAtDepth1ReplacesWithTopics pins the regression where
// `:groups` from a connected session left the router with a depth-1 stack
// containing only the groups screen. Pressing esc/q on that stack popped
// to nothing and rendered "(no screen active)" — popOrReplaceToHome must
// instead drop the user back onto topics so the flow stays usable.
func TestRoute_GroupsBackAtDepth1ReplacesWithTopics(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")

	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool { return strings.Contains(m.Render(), "orders") })
	require.Equal(t, 1, m.Router().Depth())

	feed(m, ":", 'g', 'r', 'o', 'u', 'p', 's')
	_, cmd := m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Consumer Groups")
	require.Equal(t, 1, m.Router().Depth())

	_, cmd = m.Update(keyPress("esc"))
	drainCmd(t, m, cmd)
	out := m.Render()
	assert.Contains(t, out, "Topics")
	assert.NotContains(t, out, "no screen active")
}

// TestRoute_TopicsQuitAtRootReplacesWithClusters pins
// popOrReplaceToClusters: when topics is the only screen on the stack,
// `q` must replace it with the clusters list rather than pop into nothing.
func TestRoute_TopicsQuitAtRootReplacesWithClusters(t *testing.T) {
	cluster := startKfake(t)
	mustCreateTopic(t, cluster, "orders")
	m := newConnectedHost(t, cluster)
	connectActive(t, m)
	settleUntil(t, m, func() bool { return strings.Contains(m.Render(), "orders") })
	require.Equal(t, 1, m.Router().Depth())

	_, cmd := m.Update(keyPress("q"))
	drainCmd(t, m, cmd)

	// after the replace the active screen is clusters.
	assert.Contains(t, m.Render(), "Clusters")
}

// TestNewKafkaDialer_AndPinger exercises the production constructors so
// their bodies are covered. The pinger is invoked against kfake; the
// dialer body is exercised indirectly by the pinger.
func TestNewKafkaDialer_AndPinger(t *testing.T) {
	cluster := startKfake(t)
	dialer := tui.NewKafkaDialer("kafka-tui-test")
	c, err := dialer.Dial(cluster)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	pinger := tui.NewClusterPinger(dialer, time.Second)
	require.NoError(t, pinger.Ping(context.Background(), cluster))
}

// TestActiveClient_NilBeforeConnect pins the smoke getters: ActiveClient
// is nil before any cluster has been connected, and Render/View don't
// panic on the freshly-constructed host.
func TestActiveClient_NilBeforeConnect(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
	})

	assert.Nil(t, m.ActiveClient())
	assert.NotEmpty(t, m.Render())
	// View() proxies to render(); just exercise the path.
	_ = m.View()
}

// mustCreateTopic creates a single-partition topic on the kfake cluster
// via an admin call so the topics screen sees it on the first metadata
// fetch.
func mustCreateTopic(t *testing.T, c config.Cluster, topic string) {
	t.Helper()
	cl, err := kafka.Dial(c, kafka.DialOptions{ClientID: "kafka-tui-test-setup"})
	require.NoError(t, err)
	defer cl.Close()
	require.NoError(t, cl.CreateTopic(context.Background(), kafka.CreateTopicSpec{
		Name: topic, Partitions: 1, ReplicationFactor: 1,
	}))
}

// settleUntil pumps async screen messages (via Init + chained cmds) until
// cond returns true or the bound is hit. This is how integration tests
// wait for a screen's first data fetch to land without sleeping.
func settleUntil(t *testing.T, m *tui.Model, cond func() bool) {
	t.Helper()
	const maxRounds = 32
	for range maxRounds {
		if cond() {
			return
		}
		// re-arm whatever the screen is waiting on.
		drainCmd(t, m, m.Init())
	}
	t.Fatalf("condition never became true; last render:\n%s", m.Render())
}
