package clusters_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
)

// drive runs cmd to completion synchronously and routes any resulting
// messages back through the Model, mirroring how the Bubble Tea program
// dispatches cmds in production. tea.BatchMsg / sequenceMsg are unfolded into
// the queue so async fan-outs (e.g. testAll) are fully drained.
func drive(t *testing.T, m *clusters.Model, cmd tea.Cmd) {
	t.Helper()
	queue := []tea.Cmd{cmd}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		if next == nil {
			continue
		}
		msg := next()
		if msg == nil {
			continue
		}
		if batch, ok := msg.(tea.BatchMsg); ok {
			queue = append(queue, batch...)
			continue
		}
		follow := m.Update(msg)
		queue = append(queue, follow)
	}
}

func TestStatusLabels(t *testing.T) {
	tests := []struct {
		s    clusters.ConnectionStatus
		want string
	}{
		{clusters.StatusUnknown, "? unknown"},
		{clusters.StatusChecking, "◐ checking…"},
		{clusters.StatusOK, "✓ ok"},
		{clusters.StatusFailed, "✗ failed"},
		{clusters.StatusInvalid, "! invalid"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, tc.s.Label())
	}
}

func TestNew_InitializesUnknownStatusForEveryCluster(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "prod", Brokers: []string{"a:9092"}},
			{Name: "stage", Brokers: []string{"b:9092"}},
		},
	})
	assert.Equal(t, clusters.StatusUnknown, m.Status("prod"))
	assert.Equal(t, clusters.StatusUnknown, m.Status("stage"))
}

func TestClusters_BreadcrumbIsEmpty(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "prod", Brokers: []string{"a:9092"}},
		},
	})
	assert.Empty(t, m.Breadcrumb())
}

func TestSkipTarget_SingleClusterAutoSkips(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "only", Brokers: []string{"a:9092"}}},
	})
	name, ok := m.SkipTarget()
	assert.True(t, ok)
	assert.Equal(t, "only", name)
}

func TestSkipTarget_AutoSelectMatchingClusterWins(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		AutoSelectCluster: "b",
	})
	name, ok := m.SkipTarget()
	assert.True(t, ok)
	assert.Equal(t, "b", name)
}

func TestSkipTarget_AutoSelectUnknownCluster_LandsOnPickerWithToast(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		AutoSelectCluster: "nonexistent",
	})

	_, ok := m.SkipTarget()
	assert.False(t, ok, "unknown autoSelect must not auto-connect")

	// Init drains the SkipTarget miss into a toast so the user sees what
	// went wrong instead of silently landing on the picker.
	_ = m.Init()
	require.Equal(t, 1, m.Toasts().Len())
	assert.Contains(t, m.Toasts().Items()[0].Message, "nonexistent")
	assert.Contains(t, m.Toasts().Items()[0].Message, "not found")
}

func TestSkipTarget_AutoSelectInvalidCluster_LandsOnPickerWithToast(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "ok"}},
		InvalidClusters: []config.InvalidCluster{
			{Cluster: config.Cluster{Name: "broken"}, Reason: errors.New("vault")},
		},
		AutoSelectCluster: "broken",
	})

	_, ok := m.SkipTarget()
	assert.False(t, ok, "invalid autoSelect must not auto-connect")

	_ = m.Init()
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	// first toast should explain the invalid autoSelect — the generic
	// "N cluster(s) failed to load" summary comes after.
	assert.Contains(t, m.Toasts().Items()[0].Message, "broken")
	assert.Contains(t, m.Toasts().Items()[0].Message, "invalid")
}

func TestSkipTarget_NoSkipWhenMultipleClusters(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
	})
	_, ok := m.SkipTarget()
	assert.False(t, ok)
}

func TestInit_StartupWarningsBecomeToasts(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters:        []config.Cluster{{Name: "a", Brokers: []string{"x"}}, {Name: "b", Brokers: []string{"y"}}},
		StartupWarnings: []string{"cluster \"a\" is overridden by --brokers"},
	})
	_ = m.Init()
	assert.Equal(t, 1, m.Toasts().Len())
}

func TestInit_DoesNotPingClusters(t *testing.T) {
	pings := 0
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "a", Brokers: []string{"x"}}, {Name: "b", Brokers: []string{"y"}}},
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			pings++
			return nil
		}),
	})

	drive(t, m, m.Init())

	assert.Equal(t, 0, pings)
	assert.Equal(t, clusters.StatusUnknown, m.Status("a"))
	assert.Equal(t, clusters.StatusUnknown, m.Status("b"))
}

func TestEnter_OptimisticConnectWhenNoPinger(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
	})
	_ = m.Update(keyPress("enter"))
	a := m.ConsumeAction()
	assert.Equal(t, "a", a.Connect)
	assert.Equal(t, clusters.StatusOK, m.Status("a"))
}

func TestEnter_PingerSuccessSetsOKAndConnects(t *testing.T) {
	calls := 0
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			calls++
			return nil
		}),
	})
	cmd := m.Update(keyPress("enter"))
	require.NotNil(t, cmd)
	assert.Equal(t, clusters.StatusChecking, m.Status("a"))

	drive(t, m, cmd)

	assert.Equal(t, clusters.StatusOK, m.Status("a"))
	assert.Equal(t, "a", m.ConsumeAction().Connect)
	assert.Equal(t, 1, calls)
}

func TestPing_StaleResultIsDropped(t *testing.T) {
	// arrange
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "a", Brokers: []string{"x"}}},
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	})
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	_ = m.ConsumeAction()
	require.Equal(t, clusters.StatusOK, m.Status("a"))

	// act — a late result from a superseded ping (Gen 0) must not
	// downgrade the live OK status or trigger a phantom connect.
	m.Update(clusters.PingResultMsg{
		Name: "a",
		Err:  errors.New("stale broker error"),
		Gen:  0,
	})

	// assert
	assert.Equal(t, clusters.StatusOK, m.Status("a"))
	assert.Empty(t, m.ConsumeAction().Connect)
	assert.Equal(t, 0, m.Toasts().Len(), "stale result must not push a toast")
}

func TestPing_StaleResultForRemovedClusterIsDropped(t *testing.T) {
	// arrange — connect to `a` so pingGen[a] is bumped, then config-reload
	// removes `a` entirely.
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "a", Brokers: []string{"x"}}},
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	})
	cmd := m.Update(keyPress("enter"))
	require.NotNil(t, cmd)
	gen := uint64(1) // first ping dispatch bumps from 0 to 1
	_ = m.ConsumeAction()
	m.SetClusters([]config.Cluster{{Name: "b", Brokers: []string{"y"}}}, nil, nil, "")

	// act — the in-flight ping for the removed `a` finally returns
	m.Update(clusters.PingResultMsg{Name: "a", Err: nil, Gen: gen})

	// assert — must not resurrect status or fire a phantom Connect
	assert.Empty(t, m.ConsumeAction().Connect, "removed cluster must not auto-connect")
	assert.Equal(t, clusters.StatusUnknown, m.Status("a"))
}

func TestPing_StaleResultForNewlyInvalidClusterIsDropped(t *testing.T) {
	// arrange — connect to `a` so pingGen[a]=1 and a ping is in flight.
	// Then reload makes `a` invalid (e.g. its vault placeholder broke).
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "a", Brokers: []string{"x"}}},
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return nil
		}),
	})
	cmd := m.Update(keyPress("enter"))
	require.NotNil(t, cmd)
	gen := uint64(1)
	_ = m.ConsumeAction()
	m.SetClusters(nil, []config.InvalidCluster{{
		Cluster: config.Cluster{Name: "a", Brokers: []string{"x"}},
		Reason:  errors.New("vault placeholder broke"),
	}}, nil, "")
	require.Equal(t, clusters.StatusInvalid, m.Status("a"))

	// act — late ping result must not overwrite StatusInvalid with OK
	// nor trigger a phantom Connect to a cluster with broken config.
	m.Update(clusters.PingResultMsg{Name: "a", Err: nil, Gen: gen})

	// assert
	assert.Equal(t, clusters.StatusInvalid, m.Status("a"))
	assert.Empty(t, m.ConsumeAction().Connect, "invalid cluster must not auto-connect")
}

func TestEnter_PingerFailureSetsFailedAndRaisesToast(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			return errors.New("dial timeout")
		}),
	})
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)

	assert.Equal(t, clusters.StatusFailed, m.Status("a"))
	assert.Empty(t, m.ConsumeAction().Connect, "must not connect on failure")
	require.Equal(t, 1, m.Toasts().Len())
	assert.Contains(t, m.Toasts().Items()[0].Message, "dial timeout")
}

func TestT_TestSelectedClusterOnly(t *testing.T) {
	probed := []string{}
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		Pinger: clusters.PingerFunc(func(_ context.Context, c config.Cluster) error {
			probed = append(probed, c.Name)
			return nil
		}),
	})
	cmd := m.Update(keyPress("t"))
	drive(t, m, cmd)
	assert.Equal(t, []string{"a"}, probed)
	assert.Equal(t, clusters.StatusOK, m.Status("a"))
	// no connect intent for plain test.
	assert.Empty(t, m.ConsumeAction().Connect)
}

func TestShiftT_TestAllClusters(t *testing.T) {
	probed := map[string]int{}
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		Pinger: clusters.PingerFunc(func(_ context.Context, c config.Cluster) error {
			probed[c.Name]++
			return nil
		}),
	})
	cmd := m.Update(keyPress("T"))
	drive(t, m, cmd)
	assert.Equal(t, 1, probed["a"])
	assert.Equal(t, 1, probed["b"])
	assert.Equal(t, clusters.StatusOK, m.Status("a"))
	assert.Equal(t, clusters.StatusOK, m.Status("b"))
}

func TestT_TestsAllStatuses(t *testing.T) {
	probed := 0
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			probed++
			return nil
		}),
	})
	cmd := m.Update(keyPress("T"))
	drive(t, m, cmd)
	assert.Equal(t, 2, probed)
}

func TestR_RaisesReloadAction(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
		},
	})
	_ = m.Update(keyPress("r"))
	assert.True(t, m.ConsumeAction().Reload, "r must request a config reload")
}

func TestE_OpensChooserWhenBothPathsExist(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		GlobalPath:  "/home/u/.kafka-tui/clusters.yaml",
		ProjectPath: "/proj/.kafka-tui/clusters.yaml",
	})
	_ = m.Update(keyPress("e"))
	assert.True(t, m.EditingChooser())
	assert.Equal(t, []string{"global", "project"}, m.EditChoices())
}

func TestEditChooser_NavigatesAndSelects(t *testing.T) {
	called := []string{}
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		GlobalPath:  "/global/clusters.yaml",
		ProjectPath: "/project/clusters.yaml",
		Editor: clusters.EditorFunc(func(p string) tea.Cmd {
			called = append(called, p)
			return func() tea.Msg { return clusters.EditCompletedMsg{Path: p} }
		}),
	})
	_ = m.Update(keyPress("e"))
	_ = m.Update(keyPress("j")) // move to project
	assert.Equal(t, 1, m.EditCursor())

	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)

	assert.False(t, m.EditingChooser())
	assert.Equal(t, []string{"/project/clusters.yaml"}, called)
}

func TestE_NoChoicesEmitsWarning(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
	})
	_ = m.Update(keyPress("e"))
	require.Equal(t, 1, m.Toasts().Len())
	assert.Contains(t, m.Toasts().Items()[0].Message, "no clusters.yaml")
}

func TestE_SinglePathSkipsChooser(t *testing.T) {
	called := []string{}
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		GlobalPath: "/g/clusters.yaml",
		Editor: clusters.EditorFunc(func(p string) tea.Cmd {
			called = append(called, p)
			return func() tea.Msg { return clusters.EditCompletedMsg{Path: p} }
		}),
	})
	cmd := m.Update(keyPress("e"))
	drive(t, m, cmd)
	assert.False(t, m.EditingChooser())
	assert.Equal(t, []string{"/g/clusters.yaml"}, called)
}

func TestEditChooser_EscapeCancels(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		GlobalPath:  "/g/clusters.yaml",
		ProjectPath: "/p/clusters.yaml",
	})
	_ = m.Update(keyPress("e"))
	_ = m.Update(keyPress("esc"))
	assert.False(t, m.EditingChooser())
}

func TestQ_RaisesQuitAction(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
	})
	_ = m.Update(keyPress("q"))
	assert.True(t, m.ConsumeAction().Quit)
}

func TestEsc_DoesNotQuitFromRoot(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
	})
	_ = m.Update(keyPress("esc"))
	assert.False(t, m.ConsumeAction().Quit, "esc on the root screen must be a no-op")
}

func TestView_RendersTableWithMarkers(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "prod", Brokers: []string{"a:9092", "b:9092"}, Color: "red", ReadOnly: true},
			{Name: "stage", Brokers: []string{"c:9092"}, Color: "yellow"},
			{Name: "cli", Brokers: []string{"d:9092"}},
		},
		CLIName: "cli",
	})
	out := m.View()
	assert.Contains(t, out, "prod")
	assert.Contains(t, out, "stage")
	assert.Contains(t, out, "cli")
	assert.Contains(t, out, "[RO]")
	assert.Contains(t, out, "(cli)")
	// status column shows initial unknown.
	assert.Contains(t, out, "? unknown")
}

func TestView_GoldenWithStatuses(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "prod", Brokers: []string{"a:9092"}, Color: "red", ReadOnly: true},
			{Name: "stage", Brokers: []string{"b:9092"}, Color: "green"},
		},
		Pinger: clusters.PingerFunc(func(_ context.Context, c config.Cluster) error {
			if c.Name == "prod" {
				return errors.New("connection refused")
			}
			return nil
		}),
		Now: func() time.Time { return time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC) },
	})
	cmd := m.Update(keyPress("T"))
	drive(t, m, cmd)

	out := m.View()
	assert.Contains(t, out, "✓ ok")
	assert.Contains(t, out, "✗ failed")
	assert.Contains(t, out, "[RO]")
	// error is reported via toast (now surfaced through the global flash bar,
	// so check the queue rather than the screen body).
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "connection refused")
}

func TestView_EditChooserModalRendered(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		GlobalPath:  "/g/clusters.yaml",
		ProjectPath: "/p/clusters.yaml",
	})
	_ = m.Update(keyPress("e"))

	out := m.View()
	assert.Contains(t, out, "Edit clusters.yaml")
	assert.Contains(t, out, "global")
	assert.Contains(t, out, "project")
	assert.Contains(t, out, "/g/clusters.yaml")

	// hints for the open chooser live in the screen's KeyHints (which the
	// host renders into the global hint bar), not inside the popup itself —
	// same as the copy/seek menus.
	labels := []string{}
	for _, h := range m.KeyHints() {
		labels = append(labels, h.Label)
	}
	joined := strings.Join(labels, ",")
	assert.Contains(t, joined, "select")
	assert.Contains(t, joined, "cancel")
}

func TestKeyHints_ContainExpectedLabels(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
	})
	hints := m.KeyHints()
	labels := make([]string, 0, len(hints))
	for _, h := range hints {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	assert.Contains(t, got, "connect")
	assert.Contains(t, got, "test")
	assert.Contains(t, got, "reload")
	assert.Contains(t, got, "edit")
	assert.Contains(t, got, "filter")
}

func TestSearch_FiltersTable(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "prod-east", Brokers: []string{"a"}},
			{Name: "stage", Brokers: []string{"b"}},
			{Name: "prod-west", Brokers: []string{"c"}},
		},
	})
	// the host owns the `/` prompt now and pushes each keystroke into
	// SetSearch. Drive the screen through that public API to verify
	// rows filter as expected.
	m.SetSearch("prod")
	out := m.View()
	assert.Contains(t, out, "prod-east")
	assert.Contains(t, out, "prod-west")
	assert.NotContains(t, out, "stage")
	assert.Contains(t, m.Title(), "Clusters [2/3] </prod>")
}

func TestEditCompleted_ErrorRaisesToast(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		GlobalPath: "/g/clusters.yaml",
		Editor: clusters.EditorFunc(func(p string) tea.Cmd {
			return func() tea.Msg {
				return clusters.EditCompletedMsg{Path: p, Err: errors.New("editor crashed")}
			}
		}),
	})
	cmd := m.Update(keyPress("e"))
	drive(t, m, cmd)
	require.Equal(t, 1, m.Toasts().Len())
	assert.Contains(t, m.Toasts().Items()[0].Message, "editor crashed")
}

func TestNew_InvalidClusters_SeedStatusAndErrors(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "ok", Brokers: []string{"a:9092"}}},
		InvalidClusters: []config.InvalidCluster{
			{Cluster: config.Cluster{Name: "broken", Brokers: []string{"b:9092"}}, Reason: errors.New("vault is not configured")},
		},
	})
	assert.Equal(t, clusters.StatusUnknown, m.Status("ok"))
	assert.Equal(t, clusters.StatusInvalid, m.Status("broken"))
}

func TestInit_InvalidClustersRaiseSummaryToast(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "ok", Brokers: []string{"a:9092"}}},
		InvalidClusters: []config.InvalidCluster{
			{Cluster: config.Cluster{Name: "broken1"}, Reason: errors.New("x")},
			{Cluster: config.Cluster{Name: "broken2"}, Reason: errors.New("y")},
		},
	})
	_ = m.Init()
	require.Equal(t, 1, m.Toasts().Len())
	assert.Contains(t, m.Toasts().Items()[0].Message, "2 cluster(s) failed to load")
}

func TestEnter_InvalidClusterShowsReasonAndDoesNotConnect(t *testing.T) {
	pings := 0
	m := clusters.New(clusters.Options{
		InvalidClusters: []config.InvalidCluster{
			{Cluster: config.Cluster{Name: "broken", Brokers: []string{"b:9092"}}, Reason: errors.New("vault is not configured")},
		},
		Pinger: clusters.PingerFunc(func(_ context.Context, _ config.Cluster) error {
			pings++
			return nil
		}),
	})

	_ = m.Update(keyPress("enter"))

	assert.Equal(t, 0, pings, "must not dial an invalid cluster")
	assert.Empty(t, m.ConsumeAction().Connect)
	// summary toast from Init was not triggered here; first toast is the reason
	require.Equal(t, 1, m.Toasts().Len())
	assert.Contains(t, m.Toasts().Items()[0].Message, "vault is not configured")
}

func TestShiftT_TestAllSkipsInvalidClusters(t *testing.T) {
	probed := []string{}
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "ok", Brokers: []string{"a:9092"}}},
		InvalidClusters: []config.InvalidCluster{
			{Cluster: config.Cluster{Name: "broken"}, Reason: errors.New("x")},
		},
		Pinger: clusters.PingerFunc(func(_ context.Context, c config.Cluster) error {
			probed = append(probed, c.Name)
			return nil
		}),
	})

	cmd := m.Update(keyPress("T"))
	drive(t, m, cmd)

	assert.Equal(t, []string{"ok"}, probed)
	assert.Equal(t, clusters.StatusInvalid, m.Status("broken"))
}

func TestSkipTarget_InvalidLoneClusterDoesNotAutoSkip(t *testing.T) {
	m := clusters.New(clusters.Options{
		InvalidClusters: []config.InvalidCluster{
			{Cluster: config.Cluster{Name: "broken"}, Reason: errors.New("x")},
		},
	})
	_, ok := m.SkipTarget()
	assert.False(t, ok, "lone-cluster auto-skip must not run when the only cluster is invalid")
}

func TestSetClusters_NewlyInvalidClusterRaisesToast(t *testing.T) {
	// arrange — clean state, then a reload introduces a new invalid cluster.
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "a"}, {Name: "b"}},
	})

	// act
	m.SetClusters(
		[]config.Cluster{{Name: "a"}},
		[]config.InvalidCluster{{Cluster: config.Cluster{Name: "b"}, Reason: errors.New("vault")}},
		nil,
		"",
	)

	// assert
	require.Equal(t, 1, m.Toasts().Len())
	assert.Contains(t, m.Toasts().Items()[0].Message, "now invalid")
	assert.Contains(t, m.Toasts().Items()[0].Message, "b")
}

func TestSetClusters_AlreadyInvalidClusterDoesNotReToast(t *testing.T) {
	// arrange — "b" is invalid from the start; a reload that keeps "b"
	// invalid (no change) must not re-toast.
	m := clusters.New(clusters.Options{
		Clusters:        []config.Cluster{{Name: "a"}},
		InvalidClusters: []config.InvalidCluster{{Cluster: config.Cluster{Name: "b"}, Reason: errors.New("vault")}},
	})

	// act
	m.SetClusters(
		[]config.Cluster{{Name: "a"}},
		[]config.InvalidCluster{{Cluster: config.Cluster{Name: "b"}, Reason: errors.New("vault still")}},
		nil,
		"",
	)

	// assert
	assert.Equal(t, 0, m.Toasts().Len(), "persistent invalid must not re-toast on every reload")
}

func TestSetClusters_NewWarningRaisesToast(t *testing.T) {
	// arrange — clean startup, no warnings.
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "a"}},
	})

	// act — reload introduces a new soft-fallback warning.
	m.SetClusters(
		[]config.Cluster{{Name: "a"}},
		nil,
		[]string{"clipboard.method: invalid value \"xclip\"; using default"},
		"",
	)

	// assert
	require.Equal(t, 1, m.Toasts().Len())
	assert.Contains(t, m.Toasts().Items()[0].Message, "clipboard.method")
}

func TestSetClusters_PersistentWarningDoesNotReToast(t *testing.T) {
	// arrange — same warning was already shown at startup.
	startup := []string{"clipboard.method: invalid value \"xclip\"; using default"}
	m := clusters.New(clusters.Options{
		Clusters:        []config.Cluster{{Name: "a"}},
		StartupWarnings: startup,
	})
	_ = m.Init() // drain startupWarn
	startupCount := m.Toasts().Len()

	// act — reload reports the same warning (user did not fix the YAML).
	m.SetClusters([]config.Cluster{{Name: "a"}}, nil, startup, "")

	// assert
	assert.Equal(t, startupCount, m.Toasts().Len(), "persistent warning must not re-toast")
}

func TestSetClusters_ClusterFixedTransitionsInvalidToUnknown(t *testing.T) {
	m := clusters.New(clusters.Options{
		InvalidClusters: []config.InvalidCluster{
			{Cluster: config.Cluster{Name: "c", Brokers: []string{"a:9092"}}, Reason: errors.New("vault")},
		},
	})
	require.Equal(t, clusters.StatusInvalid, m.Status("c"))

	m.SetClusters([]config.Cluster{{Name: "c", Brokers: []string{"a:9092"}}}, nil, nil, "")

	assert.Equal(t, clusters.StatusUnknown, m.Status("c"))
}

func TestSetClusters_ClusterBrokenTransitionsValidToInvalid(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "c", Brokers: []string{"a:9092"}}},
	})
	require.Equal(t, clusters.StatusUnknown, m.Status("c"))

	m.SetClusters(nil, []config.InvalidCluster{
		{Cluster: config.Cluster{Name: "c"}, Reason: errors.New("vault")},
	}, nil, "")

	assert.Equal(t, clusters.StatusInvalid, m.Status("c"))
}

// ----- helpers -----

func keyPress(name string) tea.KeyPressMsg {
	switch name {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "backspace":
		return tea.KeyPressMsg{Code: tea.KeyBackspace}
	case "tab":
		return tea.KeyPressMsg{Code: tea.KeyTab}
	case "down":
		return tea.KeyPressMsg{Code: tea.KeyDown}
	case "up":
		return tea.KeyPressMsg{Code: tea.KeyUp}
	}
	if len(name) == 1 {
		r := rune(name[0])
		return tea.KeyPressMsg{Code: r, Text: string(r)}
	}
	return tea.KeyPressMsg{}
}
