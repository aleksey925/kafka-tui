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
	assert.Contains(t, out, "Clusters[3]")
	for _, name := range []string{"alpha", "beta", "gamma"} {
		assert.Contains(t, out, name)
	}

	// open prompt and type "be" — live filter narrows to "beta".
	feed(m, "/", 'b', 'e')
	out = m.Render()
	assert.Equal(t, tui.ModeSearch, m.Mode(), "prompt is open")
	assert.Contains(t, out, "Clusters[1/3] /be", "title shows match count + query")
	assert.Contains(t, out, "beta")
	assert.NotContains(t, out, "alpha")
	assert.NotContains(t, out, "gamma")

	// enter commits, prompt closes, filter stays applied.
	_, _ = m.Update(keyPress("enter"))
	out = m.Render()
	assert.Equal(t, tui.ModeNormal, m.Mode())
	assert.Contains(t, out, "Clusters[1/3] /be")
	assert.Contains(t, out, "beta")
	assert.NotContains(t, out, "gamma")
}

// TestSearch_EscRestoresPreviousFilter pins the k9s behavior reported by
// the user: opening `/` over an existing filter pre-fills the buffer for
// editing, and esc restores the original filter rather than dropping it.
func TestSearch_EscRestoresPreviousFilter(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
		{Name: "gamma", Brokers: []string{"g:9092"}},
	})

	// apply a filter
	feed(m, "/", 'b', 'e')
	_, _ = m.Update(keyPress("enter"))
	require.Contains(t, m.Render(), "Clusters[1/3] /be")

	// reopen the prompt — buffer should already hold "be" — and esc.
	feed(m, "/")
	require.Equal(t, tui.ModeSearch, m.Mode())
	_, _ = m.Update(keyPress("esc"))

	out := m.Render()
	assert.Equal(t, tui.ModeNormal, m.Mode())
	// filter is preserved verbatim.
	assert.Contains(t, out, "Clusters[1/3] /be")
	assert.Contains(t, out, "beta")
	assert.NotContains(t, out, "alpha")
}

// TestEsc_FilterClearedBeforePop verifies the esc cascade: with a filter
// applied, esc only clears the filter; a second esc then runs the regular
// pop logic (here: a no-op at root depth).
func TestEsc_FilterClearedBeforePop(t *testing.T) {
	m := newClustersHostWith(t, []config.Cluster{
		{Name: "alpha", Brokers: []string{"a:9092"}},
		{Name: "beta", Brokers: []string{"b:9092"}},
	})

	// apply a filter
	feed(m, "/", 'b')
	_, _ = m.Update(keyPress("enter"))
	require.Contains(t, m.Render(), "Clusters[1/2] /b")
	require.False(t, m.Quit())

	// first esc clears filter
	_, _ = m.Update(keyPress("esc"))
	out := m.Render()
	assert.NotContains(t, out, "/b")
	assert.Contains(t, out, "Clusters[2]")
	assert.False(t, m.Quit(), "esc must not quit while filter was active")

	// at root depth a second esc is a no-op (ctrl+c remains the exit).
	_, _ = m.Update(keyPress("esc"))
	assert.False(t, m.Quit(), "esc at root never quits the app")
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
		BuildClusterList: func(c []config.Cluster) ([]config.Cluster, string) {
			return c, ""
		},
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
	assert.Contains(t, out, "clusters reloaded · 2", "success toast must surface in the flash bar")
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
		Dialer: tui.DialerFunc(func(c config.Cluster) (*kafka.Client, error) {
			return kafka.Dial(c, kafka.DialOptions{ClientID: "kafka-tui-test"})
		}),
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
// nil without touching the dialer.
func TestConnect_UnknownClusterIsNoop(t *testing.T) {
	dialed := false
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
		Dialer: tui.DialerFunc(func(_ config.Cluster) (*kafka.Client, error) {
			dialed = true
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

	assert.False(t, dialed, "dialer must not run for unknown cluster")
	assert.Nil(t, m.ActiveClient())
}

// TestConnect_DialErrorSurfacesToast pins the dial-failure branch:
// connectCluster pushes an error toast onto the clusters screen instead
// of leaving the user without feedback.
func TestConnect_DialErrorSurfacesToast(t *testing.T) {
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
		Dialer: tui.DialerFunc(func(_ config.Cluster) (*kafka.Client, error) {
			return nil, errors.New("dial refused")
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

	assert.Contains(t, m.Render(), "dial refused")
	assert.Nil(t, m.ActiveClient())
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
		ConfigReloader: func() (*config.Loaded, []config.Cluster, string, error) {
			return freshLoaded, freshLoaded.Clusters, "", nil
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
	assert.Contains(t, out, "reloaded 2 clusters", "success toast must surface")
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
		ConfigReloader: func() (*config.Loaded, []config.Cluster, string, error) {
			return nil, nil, "", errors.New("yaml: bad indent")
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
		BuildClusterList: func(c []config.Cluster) ([]config.Cluster, string) {
			return c, ""
		},
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
		BuildClusterList: func(c []config.Cluster) ([]config.Cluster, string) {
			return c, ""
		},
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

	// q on logs → Action.Back. The host's popScreen on a depth-1 stack is
	// effectively a no-op, but the Action consumption itself runs through
	// routeLogsAction's Back branch.
	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)

	// reopen on `:config sources` and exercise the same Back path.
	feed(m, ":", 'c', 'o', 'n', 'f', 'i', 'g', ' ', 's', 'o', 'u', 'r', 'c', 'e', 's')
	_, cmd = m.Update(keyPress("enter"))
	drainCmd(t, m, cmd)
	require.Contains(t, m.Render(), "Config Sources")

	_, cmd = m.Update(keyPress("q"))
	drainCmd(t, m, cmd)
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
