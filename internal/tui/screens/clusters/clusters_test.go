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
		_, follow := m.Update(msg)
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

func TestSkipTarget_SingleClusterAutoSkips(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{{Name: "only", Brokers: []string{"a:9092"}}},
	})
	name, ok := m.SkipTarget()
	assert.True(t, ok)
	assert.Equal(t, "only", name)
}

func TestSkipTarget_CLINameAlwaysWins(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		CLIName: "cli",
	})
	name, ok := m.SkipTarget()
	assert.True(t, ok)
	assert.Equal(t, "cli", name)
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

func TestEnter_OptimisticConnectWhenNoPinger(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
	})
	updated, _ := m.Update(keyPress("enter"))
	m = updated
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
	_, cmd := m.Update(keyPress("enter"))
	require.NotNil(t, cmd)
	assert.Equal(t, clusters.StatusChecking, m.Status("a"))

	drive(t, m, cmd)

	assert.Equal(t, clusters.StatusOK, m.Status("a"))
	assert.Equal(t, "a", m.ConsumeAction().Connect)
	assert.Equal(t, 1, calls)
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
	_, cmd := m.Update(keyPress("enter"))
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
	_, cmd := m.Update(keyPress("t"))
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
	_, cmd := m.Update(keyPress("T"))
	drive(t, m, cmd)
	assert.Equal(t, 1, probed["a"])
	assert.Equal(t, 1, probed["b"])
	assert.Equal(t, clusters.StatusOK, m.Status("a"))
	assert.Equal(t, clusters.StatusOK, m.Status("b"))
}

func TestR_RefreshesAllStatuses(t *testing.T) {
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
	_, cmd := m.Update(keyPress("r"))
	drive(t, m, cmd)
	assert.Equal(t, 2, probed)
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
	updated, _ := m.Update(keyPress("e"))
	m = updated
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
		Editor: clusters.EditorFunc(func(p string) error {
			called = append(called, p)
			return nil
		}),
	})
	_, _ = m.Update(keyPress("e"))
	_, _ = m.Update(keyPress("j")) // move to project
	assert.Equal(t, 1, m.EditCursor())

	_, cmd := m.Update(keyPress("enter"))
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
	_, _ = m.Update(keyPress("e"))
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
		Editor: clusters.EditorFunc(func(p string) error {
			called = append(called, p)
			return nil
		}),
	})
	_, cmd := m.Update(keyPress("e"))
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
	_, _ = m.Update(keyPress("e"))
	_, _ = m.Update(keyPress("esc"))
	assert.False(t, m.EditingChooser())
}

func TestEsc_RaisesQuitAction(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
	})
	_, _ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Quit)
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
	_, cmd := m.Update(keyPress("T"))
	drive(t, m, cmd)

	out := m.View()
	assert.Contains(t, out, "✓ ok")
	assert.Contains(t, out, "✗ failed")
	assert.Contains(t, out, "[RO]")
	// error is reported via toast as well.
	assert.Contains(t, out, "connection refused")
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
	_, _ = m.Update(keyPress("e"))

	out := m.View()
	assert.Contains(t, out, "Edit clusters.yaml")
	assert.Contains(t, out, "global")
	assert.Contains(t, out, "project")
	assert.Contains(t, out, "/g/clusters.yaml")
	assert.Contains(t, out, "Esc cancel")
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
	assert.Contains(t, got, "edit")
	assert.Contains(t, got, "refresh")
	assert.Contains(t, got, "search")
}

func TestSearch_FiltersTable(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "prod-east", Brokers: []string{"a"}},
			{Name: "stage", Brokers: []string{"b"}},
			{Name: "prod-west", Brokers: []string{"c"}},
		},
	})
	_, _ = m.Update(keyPress("/"))
	for _, ch := range "prod" {
		_, _ = m.Update(keyPressRune(ch))
	}
	_, _ = m.Update(keyPress("enter"))
	out := m.View()
	assert.Contains(t, out, "prod-east")
	assert.Contains(t, out, "prod-west")
	assert.NotContains(t, out, "stage")
}

func TestEditCompleted_ErrorRaisesToast(t *testing.T) {
	m := clusters.New(clusters.Options{
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x"}},
			{Name: "b", Brokers: []string{"y"}},
		},
		GlobalPath: "/g/clusters.yaml",
		Editor: clusters.EditorFunc(func(_ string) error {
			return errors.New("editor crashed")
		}),
	})
	_, cmd := m.Update(keyPress("e"))
	drive(t, m, cmd)
	require.Equal(t, 1, m.Toasts().Len())
	assert.Contains(t, m.Toasts().Items()[0].Message, "editor crashed")
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

func keyPressRune(r rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: r, Text: string(r)}
}
