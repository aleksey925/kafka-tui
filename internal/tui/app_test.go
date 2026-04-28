package tui_test

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
	"github.com/aleksey925/kafka-tui/internal/version"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    tui.Command
		wantErr bool
	}{
		{
			name:  "topics",
			input: ":topics",
			want:  tui.Command{Screen: tui.ScreenTopics, Raw: "topics"},
		},
		{
			name:  "topics no leading colon",
			input: "topics",
			want:  tui.Command{Screen: tui.ScreenTopics, Raw: "topics"},
		},
		{
			name:  "groups",
			input: ":groups",
			want:  tui.Command{Screen: tui.ScreenGroups, Raw: "groups"},
		},
		{
			name:  "clusters",
			input: ":clusters",
			want:  tui.Command{Screen: tui.ScreenClusters, Raw: "clusters"},
		},
		{
			name:  "cluster with arg",
			input: ":cluster prod-east",
			want:  tui.Command{Screen: tui.ScreenClusters, Arg: "prod-east", Raw: "cluster prod-east"},
		},
		{
			name:  "logs",
			input: ":logs",
			want:  tui.Command{Screen: tui.ScreenLogs, Raw: "logs"},
		},
		{
			name:  "config sources",
			input: ":config sources",
			want:  tui.Command{Screen: tui.ScreenConfigSrc, Raw: "config sources"},
		},
		{
			name:    "empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "whitespace only",
			input:   "   ",
			wantErr: true,
		},
		{
			name:    "topics with stray arg",
			input:   ":topics foo",
			wantErr: true,
		},
		{
			name:    "cluster missing arg",
			input:   ":cluster",
			wantErr: true,
		},
		{
			name:    "config without sources",
			input:   ":config",
			wantErr: true,
		},
		{
			name:    "config with wrong arg",
			input:   ":config dump",
			wantErr: true,
		},
		{
			name:    "unknown",
			input:   ":foobar",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tui.ParseCommand(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRouter_StackOps(t *testing.T) {
	r := tui.NewRouter()
	assert.Equal(t, 0, r.Depth())
	assert.Equal(t, tui.ScreenID(""), r.Active())

	r.Push(tui.ScreenClusters)
	assert.Equal(t, 1, r.Depth())
	assert.Equal(t, tui.ScreenClusters, r.Active())

	r.Push(tui.ScreenTopics)
	r.Push(tui.ScreenGroups)
	assert.Equal(t, []tui.ScreenID{tui.ScreenClusters, tui.ScreenTopics, tui.ScreenGroups}, r.Stack())
	assert.Equal(t, tui.ScreenGroups, r.Active())

	assert.Equal(t, tui.ScreenTopics, r.Pop())
	assert.Equal(t, tui.ScreenTopics, r.Active())

	r.Replace(tui.ScreenLogs)
	assert.Equal(t, tui.ScreenLogs, r.Active())

	r.Pop()
	r.Pop()
	assert.Equal(t, tui.ScreenID(""), r.Pop())
}

func TestRouter_ReplaceOnEmpty(t *testing.T) {
	r := tui.NewRouter()
	r.Replace(tui.ScreenTopics)
	assert.Equal(t, tui.ScreenTopics, r.Active())
	assert.Equal(t, 1, r.Depth())
}

func TestModel_CommandModeRoutesScreen(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenClusters, Width: 80, Height: 24})

	// open command bar
	updated, _ := m.Update(keyPress(":"))
	m = updated.(*tui.Model)
	assert.Equal(t, tui.ModeCommand, m.Mode())

	// type "topics"
	for _, ch := range "topics" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}
	assert.Equal(t, "topics", m.CommandBuffer())

	// submit
	updated, _ = m.Update(keyPress("enter"))
	m = updated.(*tui.Model)

	assert.Equal(t, tui.ModeNormal, m.Mode())
	assert.Empty(t, m.CommandBuffer())
	assert.Equal(t, tui.ScreenTopics, m.Router().Active())
}

func TestModel_CommandUnknownStaysInCommandMode(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenClusters})

	updated, _ := m.Update(keyPress(":"))
	m = updated.(*tui.Model)
	for _, ch := range "foobar" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}
	updated, _ = m.Update(keyPress("enter"))
	m = updated.(*tui.Model)

	assert.Equal(t, tui.ModeCommand, m.Mode())
	assert.Contains(t, m.Render(), "unknown command")
}

func TestModel_CommandBackspace(t *testing.T) {
	m := tui.New(tui.Options{})

	updated, _ := m.Update(keyPress(":"))
	m = updated.(*tui.Model)
	for _, ch := range "topi" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}
	updated, _ = m.Update(keyPress("backspace"))
	m = updated.(*tui.Model)

	assert.Equal(t, "top", m.CommandBuffer())
}

func TestModel_CommandEscapeCancels(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenClusters})

	updated, _ := m.Update(keyPress(":"))
	m = updated.(*tui.Model)
	updated, _ = m.Update(keyPressRune('a'))
	m = updated.(*tui.Model)

	updated, _ = m.Update(keyPress("esc"))
	m = updated.(*tui.Model)

	assert.Equal(t, tui.ModeNormal, m.Mode())
	assert.Empty(t, m.CommandBuffer())
}

func TestModel_SearchMode(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics})

	updated, _ := m.Update(keyPress("/"))
	m = updated.(*tui.Model)
	assert.Equal(t, tui.ModeSearch, m.Mode())

	for _, ch := range "abc" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}
	assert.Equal(t, "abc", m.SearchBuffer())

	updated, _ = m.Update(keyPress("enter"))
	m = updated.(*tui.Model)
	assert.Equal(t, tui.ModeNormal, m.Mode())
}

func TestModel_HelpToggle(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenClusters, Build: version.BuildInfo{Version: "v0.7.3", Commit: "abcdef0"}, Width: 80})

	updated, _ := m.Update(keyPress("?"))
	m = updated.(*tui.Model)
	assert.Equal(t, tui.ModeHelp, m.Mode())
	out := m.Render()
	assert.Contains(t, out, "Help")
	assert.Contains(t, out, "v0.7.3")

	updated, _ = m.Update(keyPress("esc"))
	m = updated.(*tui.Model)
	assert.Equal(t, tui.ModeNormal, m.Mode())
}

func TestModel_AutoRefreshToggle(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics})
	assert.True(t, m.AutoRefresh())

	updated, _ := m.Update(keyPress("ctrl+r"))
	m = updated.(*tui.Model)
	assert.False(t, m.AutoRefresh())

	updated, _ = m.Update(keyPress("ctrl+r"))
	m = updated.(*tui.Model)
	assert.True(t, m.AutoRefresh())
}

func TestModel_QuitFromTopOfStack(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics})

	updated, cmd := m.Update(keyPress("q"))
	m = updated.(*tui.Model)
	assert.True(t, m.Quit())
	require.NotNil(t, cmd)
}

func TestModel_QPopsStack(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenClusters})
	m.Router().Push(tui.ScreenTopics)

	updated, _ := m.Update(keyPress("q"))
	m = updated.(*tui.Model)
	assert.False(t, m.Quit())
	assert.Equal(t, tui.ScreenClusters, m.Router().Active())
}

func TestModel_CtrlCQuits(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics})
	updated, cmd := m.Update(keyPress("ctrl+c"))
	m = updated.(*tui.Model)
	assert.True(t, m.Quit())
	require.NotNil(t, cmd)
}

func TestModel_WindowSizeUpdatesGeometry(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics})
	updated, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 40})
	m = updated.(*tui.Model)

	out := m.Render()
	assert.NotEmpty(t, out)
}

func TestModel_RenderHeaderIncludesClusterAndStatus(t *testing.T) {
	now := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
	m := tui.New(tui.Options{
		Cluster:      "prod-east",
		ClusterColor: theme.ClusterRed,
		ReadOnly:     true,
		FromCLI:      true,
		Initial:      tui.ScreenTopics,
		Width:        100,
		Now:          func() time.Time { return now },
	})
	m.SetStatus(layout.StatusInfo{
		Mode:        layout.RefreshAuto,
		Interval:    5 * time.Second,
		LastRefresh: now.Add(-3 * time.Second),
		Now:         now,
	})

	out := m.Render()
	assert.Contains(t, out, "kafka-tui")
	assert.Contains(t, out, "prod-east")
	assert.Contains(t, out, "[RO]")
	assert.Contains(t, out, "(cli)")
	assert.Contains(t, out, "auto: 5s")
	assert.Contains(t, out, "refreshed 3s ago")
	assert.Contains(t, out, "topics — coming soon")
}

func TestModel_KeyHintsRenderedAtBottom(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics, Width: 80})
	out := m.Render()

	// hints contain the default labels
	for _, label := range []string{"command", "search", "help", "refresh", "back/quit"} {
		assert.Contains(t, out, label)
	}
}

func TestModel_SetKeyHints(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics})
	m.SetKeyHints([]layout.KeyHint{{Key: "n", Label: "new"}})
	out := m.Render()

	assert.Contains(t, out, "new")
	// default hints should be replaced.
	lines := strings.Split(out, "\n")
	assert.NotContains(t, lines[len(lines)-1], "command")
}

func TestModel_StatusForRefreshMode(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics, Width: 80})

	m.SetStatus(layout.StatusInfo{Mode: layout.RefreshManual})
	assert.Contains(t, m.Render(), "manual")

	m.SetStatus(layout.StatusInfo{Mode: layout.RefreshPaused})
	assert.Contains(t, m.Render(), "paused")
}

// keyPress builds a tea.KeyPressMsg matching by String(). For literal `:` we
// also need msg.Text to be set so the command-mode handler appends the rune
// to its buffer.
func keyPress(name string) tea.KeyPressMsg {
	switch name {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "backspace":
		return tea.KeyPressMsg{Code: tea.KeyBackspace}
	case "ctrl+r":
		return tea.KeyPressMsg{Code: 'r', Mod: tea.ModCtrl}
	case "ctrl+c":
		return tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl}
	case ":":
		return tea.KeyPressMsg{Code: ':', Text: ":"}
	case "/":
		return tea.KeyPressMsg{Code: '/', Text: "/"}
	case "?":
		return tea.KeyPressMsg{Code: '?', Text: "?"}
	case "q":
		return tea.KeyPressMsg{Code: 'q', Text: "q"}
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
