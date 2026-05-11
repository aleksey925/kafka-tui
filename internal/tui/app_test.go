package tui_test

import (
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
	// `:logs` is used here because it doesn't require a connected client —
	// `:topics` is blocked from the clusters screen until a cluster is selected.
	m := tui.New(tui.Options{Initial: tui.ScreenClusters, Width: 80, Height: 24})

	// open command bar
	updated, _ := m.Update(keyPress(":"))
	m = updated.(*tui.Model)
	assert.Equal(t, tui.ModeCommand, m.Mode())

	// type "logs"
	for _, ch := range "logs" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}
	assert.Equal(t, "logs", m.CommandBuffer())

	// submit
	updated, _ = m.Update(keyPress("enter"))
	m = updated.(*tui.Model)

	assert.Equal(t, tui.ModeNormal, m.Mode())
	assert.Empty(t, m.CommandBuffer())
	assert.Equal(t, tui.ScreenLogs, m.Router().Active())
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

func TestCompletionSuggestion(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{name: "empty", prefix: "", want: ""},
		{name: "t matches topics", prefix: "t", want: "topics"},
		{name: "to matches topics", prefix: "to", want: "topics"},
		{name: "top matches topics", prefix: "top", want: "topics"},
		{name: "topics exact no suggestion", prefix: "topics", want: ""},
		{name: "g matches groups", prefix: "g", want: "groups"},
		{name: "cl matches clusters", prefix: "cl", want: "clusters"},
		{name: "cluster matches clusters", prefix: "cluster", want: "clusters"},
		{name: "clusters exact no suggestion", prefix: "clusters", want: ""},
		{name: "l matches logs", prefix: "l", want: "logs"},
		{name: "c matches clusters (first alphabetically)", prefix: "c", want: "clusters"},
		{name: "co matches config sources", prefix: "co", want: "config sources"},
		{name: "config matches config sources", prefix: "config", want: "config sources"},
		{name: "config s matches config sources", prefix: "config s", want: "config sources"},
		{name: "unknown prefix", prefix: "z", want: ""},
		{name: "leading colon stripped", prefix: ":to", want: "topics"},
		{name: "case insensitive", prefix: "TO", want: "topics"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tui.CompletionSuggestion(tc.prefix)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestModel_CommandTabCompletion(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenClusters, Width: 80, Height: 24})

	// arrange
	updated, _ := m.Update(keyPress(":"))
	m = updated.(*tui.Model)

	// act — type "to", should suggest "topics"
	for _, ch := range "to" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}

	// assert
	assert.Equal(t, "to", m.CommandBuffer())
	assert.Equal(t, "topics", m.CommandSuggestion())
	assert.Contains(t, m.Render(), "pics")

	// act — press Tab to accept
	updated, _ = m.Update(keyPress("tab"))
	m = updated.(*tui.Model)

	// assert
	assert.Equal(t, "topics", m.CommandBuffer())
	assert.Empty(t, m.CommandSuggestion())
}

func TestModel_CommandTabCompletion__no_suggestion(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenClusters, Width: 80, Height: 24})

	// arrange
	updated, _ := m.Update(keyPress(":"))
	m = updated.(*tui.Model)
	for _, ch := range "zzz" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}

	// act — Tab with no suggestion is a no-op
	updated, _ = m.Update(keyPress("tab"))
	m = updated.(*tui.Model)

	// assert
	assert.Equal(t, "zzz", m.CommandBuffer())
	assert.Empty(t, m.CommandSuggestion())
}

func TestModel_CommandTabCompletion__backspace_updates_suggestion(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenClusters, Width: 80, Height: 24})

	// arrange
	updated, _ := m.Update(keyPress(":"))
	m = updated.(*tui.Model)
	for _, ch := range "topics" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}
	assert.Empty(t, m.CommandSuggestion())

	// act — backspace to "topic"
	updated, _ = m.Update(keyPress("backspace"))
	m = updated.(*tui.Model)

	// assert — suggestion reappears
	assert.Equal(t, "topics", m.CommandSuggestion())
}

func TestModel_CommandTabCompletion__tab_then_enter(t *testing.T) {
	// `:logs` is used here because it doesn't require a connected client —
	// `:groups` and `:topics` are blocked from the clusters screen until a
	// cluster is selected.
	m := tui.New(tui.Options{Initial: tui.ScreenClusters, Width: 80, Height: 24})

	// arrange
	updated, _ := m.Update(keyPress(":"))
	m = updated.(*tui.Model)
	for _, ch := range "lo" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}

	// act — Tab then Enter
	updated, _ = m.Update(keyPress("tab"))
	m = updated.(*tui.Model)
	assert.Equal(t, "logs", m.CommandBuffer())

	updated, _ = m.Update(keyPress("enter"))
	m = updated.(*tui.Model)

	// assert
	assert.Equal(t, tui.ModeNormal, m.Mode())
	assert.Equal(t, tui.ScreenLogs, m.Router().Active())
}

func TestModel_SearchModeOpensPromptAndForwardsToScreen(t *testing.T) {
	// k9s-style filter: `/` opens the host's prompt (same chrome slot
	// as `:`) and pushes each keystroke into the active screen as a
	// live search. esc cancels the filter, enter dismisses the prompt
	// while keeping the filter applied.
	m := tui.New(tui.Options{Initial: tui.ScreenTopics, Width: 100, Height: 30})

	updated, _ := m.Update(keyPress("/"))
	m = updated.(*tui.Model)
	assert.Equal(t, tui.ModeSearch, m.Mode())

	for _, ch := range "abc" {
		updated, _ = m.Update(keyPressRune(ch))
		m = updated.(*tui.Model)
	}

	updated, _ = m.Update(keyPress("enter"))
	m = updated.(*tui.Model)
	assert.Equal(t, tui.ModeNormal, m.Mode())
}

func TestModel_HelpToggle(t *testing.T) {
	// Height needs to fit the full grid (general / navigation / text editing
	// / commands) plus the footer line carrying the build version.
	m := tui.New(tui.Options{Initial: tui.ScreenClusters, Build: version.BuildInfo{Version: "v0.7.3", Commit: "abcdef0"}, Width: 80, Height: 40})

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
	// new k9s-style header surfaces these as labeled rows; literal tags
	// like "[RO]" / "(cli)" no longer appear in the chrome.
	assert.Contains(t, out, "read-only")
	assert.Contains(t, out, "cli")
	assert.Contains(t, out, "auto 5s")
	// the elapsed marker drops " ago" — the "·" separator already conveys
	// "since last refresh" and the shorter form keeps the chrome compact.
	assert.Contains(t, out, "· 3s")
	assert.NotContains(t, out, "3s ago")
	assert.Contains(t, out, "topics — coming soon")
}

func TestModel_KeyHintsRenderedInHeader(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics, Width: 100})
	out := m.Render()

	// hints contain the default labels — now rendered in the middle pane
	// of the header instead of the bottom row.
	for _, label := range []string{"command", "search", "help", "refresh", "back/quit"} {
		assert.Contains(t, out, label)
	}
}

func TestModel_SetKeyHints(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics, Width: 100})
	m.SetKeyHints([]layout.KeyHint{{Key: "n", Label: "new"}})
	out := m.Render()

	assert.Contains(t, out, "new")
}

func TestModel_StatusForRefreshMode(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics, Width: 80})

	m.SetStatus(layout.StatusInfo{Mode: layout.RefreshManual})
	assert.Contains(t, m.Render(), "manual")

	m.SetStatus(layout.StatusInfo{Mode: layout.RefreshPaused})
	assert.Contains(t, m.Render(), "paused")
}

// keyPressTable maps a name to its tea.KeyPressMsg. Literal printable keys
// also carry msg.Text so handlers that append to a buffer (command bar,
// search prompt) treat them as input rather than just a keystroke.
var keyPressTable = map[string]tea.KeyPressMsg{
	"enter":         {Code: tea.KeyEnter},
	"esc":           {Code: tea.KeyEscape},
	"tab":           {Code: tea.KeyTab},
	"backspace":     {Code: tea.KeyBackspace},
	"delete":        {Code: tea.KeyDelete},
	"up":            {Code: tea.KeyUp},
	"down":          {Code: tea.KeyDown},
	"right":         {Code: tea.KeyRight},
	"ctrl+a":        {Code: 'a', Mod: tea.ModCtrl},
	"ctrl+c":        {Code: 'c', Mod: tea.ModCtrl},
	"ctrl+e":        {Code: 'e', Mod: tea.ModCtrl},
	"ctrl+f":        {Code: 'f', Mod: tea.ModCtrl},
	"ctrl+k":        {Code: 'k', Mod: tea.ModCtrl},
	"ctrl+o":        {Code: 'o', Mod: tea.ModCtrl},
	"ctrl+r":        {Code: 'r', Mod: tea.ModCtrl},
	"ctrl+u":        {Code: 'u', Mod: tea.ModCtrl},
	"ctrl+w":        {Code: 'w', Mod: tea.ModCtrl},
	"alt+b":         {Code: 'b', Mod: tea.ModAlt},
	"alt+f":         {Code: 'f', Mod: tea.ModAlt},
	"alt+backspace": {Code: tea.KeyBackspace, Mod: tea.ModAlt},
	":":             {Code: ':', Text: ":"},
	"/":             {Code: '/', Text: "/"},
	"?":             {Code: '?', Text: "?"},
	"q":             {Code: 'q', Text: "q"},
}

// keyPress builds a tea.KeyPressMsg matching by String(). For literal `:` we
// also need msg.Text to be set so the command-mode handler appends the rune
// to its buffer.
func keyPress(name string) tea.KeyPressMsg {
	if msg, ok := keyPressTable[name]; ok {
		return msg
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
