package logs_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/screens/logs"
)

func drive(t *testing.T, m *logs.Model, cmd tea.Cmd) {
	t.Helper()
	// follow ticks would otherwise loop forever; allow a single tick through
	// per call so the appendCmd it triggers can run.
	tickSeen := false
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
		if _, ok := msg.(logs.FollowTickMsg); ok {
			if tickSeen {
				continue
			}
			tickSeen = true
		}
		_, follow := m.Update(msg)
		queue = append(queue, follow)
	}
}

func TestNew_LoadsExistingFileAndRendersLines(t *testing.T) {
	// arrange
	path := writeLog(t, "2026-04-28T10:00:00Z INFO  hello\n2026-04-28T10:00:01Z WARN  oops\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(100, 10)

	// act
	drive(t, m, m.Init())

	// assert
	assert.Equal(t, []string{
		"2026-04-28T10:00:00Z INFO  hello",
		"2026-04-28T10:00:01Z WARN  oops",
	}, m.Lines())
	out := m.View()
	assert.Contains(t, out, "hello")
	assert.Contains(t, out, "oops")
	// line count moved to the frame title.
	assert.Contains(t, m.Title(), "2 lines")
}

func TestNew_MissingFileShowsToast(t *testing.T) {
	// arrange
	m := logs.New(logs.Options{Path: filepath.Join(t.TempDir(), "missing.log")})
	m.SetSize(100, 10)

	// act
	drive(t, m, m.Init())

	// assert
	assert.True(t, m.Missing())
	out := m.View()
	assert.Contains(t, out, "No log file found at")
}

func TestEsc_RaisesBackAction(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO ready\n")
	m := logs.New(logs.Options{Path: path})
	drive(t, m, m.Init())

	// act
	_, _ = m.Update(keyPress("esc"))

	// assert
	assert.True(t, m.ConsumeAction().Back)
}

func TestF_TogglesFollowMode(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO ready\n")
	m := logs.New(logs.Options{Path: path, FollowInterval: time.Millisecond})
	drive(t, m, m.Init())
	require.False(t, m.Following())

	// act
	_, cmd := m.Update(keyPress("f"))

	// assert
	assert.True(t, m.Following())
	require.NotNil(t, cmd)
	// LIVE indicator lives in the frame title now.
	assert.Contains(t, m.Title(), "● LIVE")

	_, _ = m.Update(keyPress("f"))
	assert.False(t, m.Following())
}

func TestFollowTick_AppendsNewLines(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO first\n")
	m := logs.New(logs.Options{Path: path, FollowInterval: time.Millisecond})
	drive(t, m, m.Init())
	_, _ = m.Update(keyPress("f")) // toggle follow

	// act
	appendLog(t, path, "INFO second\n")
	drive(t, m, tickFollow())

	// assert
	assert.Equal(t, []string{
		"INFO first",
		"INFO second",
	}, m.Lines())
}

func TestFollowTick_TruncationTriggersReload(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO 1\nINFO 2\nINFO 3\n")
	m := logs.New(logs.Options{Path: path, FollowInterval: time.Millisecond})
	drive(t, m, m.Init())
	require.Len(t, m.Lines(), 3)
	_, _ = m.Update(keyPress("f"))

	// act
	require.NoError(t, os.WriteFile(path, []byte("INFO fresh\n"), 0o600))
	drive(t, m, tickFollow())

	// assert
	assert.Equal(t, []string{"INFO fresh"}, m.Lines())
}

func TestSearch_FindsMatchingLine(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO alpha\nINFO bravo\nINFO charlie\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())

	// act — open search, type "bra", submit.
	_, _ = m.Update(keyPress("/"))
	_, _ = m.Update(textKey("b"))
	_, _ = m.Update(textKey("r"))
	_, _ = m.Update(textKey("a"))
	_, _ = m.Update(keyPress("enter"))

	// assert
	assert.Equal(t, 1, m.Cursor())
}

func TestSearchN_JumpsToNextMatch(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO alpha\nINFO bravo alpha\nINFO charlie\nINFO alpha tail\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("/"))
	_, _ = m.Update(textKey("a"))
	_, _ = m.Update(textKey("l"))
	_, _ = m.Update(textKey("p"))
	_, _ = m.Update(textKey("h"))
	_, _ = m.Update(textKey("a"))
	_, _ = m.Update(keyPress("enter"))
	require.Equal(t, 0, m.Cursor())

	// act
	_, _ = m.Update(textKey("n"))
	assert.Equal(t, 1, m.Cursor())
	_, _ = m.Update(textKey("n"))
	assert.Equal(t, 3, m.Cursor())
	_, _ = m.Update(textKey("N"))
	assert.Equal(t, 1, m.Cursor())
}

func TestNavigation_GgJumpsToTop(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO 1\nINFO 2\nINFO 3\nINFO 4\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())
	require.Equal(t, 3, m.Cursor())

	// act
	_, _ = m.Update(textKey("g"))
	_, _ = m.Update(textKey("g"))

	// assert
	assert.Equal(t, 0, m.Cursor())
}

func TestNavigation_BigGJumpsToBottom(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO 1\nINFO 2\nINFO 3\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())
	_, _ = m.Update(textKey("g"))
	_, _ = m.Update(textKey("g"))
	require.Equal(t, 0, m.Cursor())

	// act
	_, _ = m.Update(keyPress("G"))

	// assert
	assert.Equal(t, 2, m.Cursor())
}

func TestNavigation_JK(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO 1\nINFO 2\nINFO 3\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())
	_, _ = m.Update(textKey("g"))
	_, _ = m.Update(textKey("g"))
	require.Equal(t, 0, m.Cursor())

	// act / assert
	_, _ = m.Update(textKey("j"))
	assert.Equal(t, 1, m.Cursor())
	_, _ = m.Update(textKey("k"))
	assert.Equal(t, 0, m.Cursor())
}

func TestColorize_LevelsHaveDistinctRendering(t *testing.T) {
	path := writeLog(t, strings.Join([]string{
		"2026-04-28 INFO ready",
		"2026-04-28 WARN backoff",
		"2026-04-28 ERROR boom",
		"2026-04-28 DEBUG inner",
	}, "\n")+"\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(120, 20)
	drive(t, m, m.Init())

	out := m.View()
	for _, tag := range []string{"INFO", "WARN", "ERROR", "DEBUG"} {
		assert.Contains(t, out, tag)
	}
}

func TestMaxLines_TrimsBuffer(t *testing.T) {
	path := writeLog(t, "INFO 1\nINFO 2\nINFO 3\nINFO 4\nINFO 5\n")
	m := logs.New(logs.Options{Path: path, MaxLines: 3})
	m.SetSize(80, 20)
	drive(t, m, m.Init())

	assert.Equal(t, []string{
		"INFO 3",
		"INFO 4",
		"INFO 5",
	}, m.Lines())
}

func TestKeyHints_IncludesExpectedLabels(t *testing.T) {
	m := logs.New(logs.Options{Path: filepath.Join(t.TempDir(), "missing.log")})
	hints := m.KeyHints()
	labels := make([]string, 0, len(hints))
	for _, h := range hints {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	for _, want := range []string{"follow", "search", "next/prev match", "back"} {
		assert.Contains(t, got, want)
	}
}

// ----- helpers -----

func writeLog(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func appendLog(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // test-only path under t.TempDir()
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = f.WriteString(content)
	require.NoError(t, err)
}

func tickFollow() tea.Cmd {
	return func() tea.Msg { return logs.FollowTickMsg{} }
}

func keyPress(name string) tea.KeyPressMsg {
	switch name {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "tab":
		return tea.KeyPressMsg{Code: tea.KeyTab}
	case "backspace":
		return tea.KeyPressMsg{Code: tea.KeyBackspace}
	}
	if len(name) == 1 {
		r := rune(name[0])
		return tea.KeyPressMsg{Code: r, Text: string(r)}
	}
	return tea.KeyPressMsg{}
}

func textKey(text string) tea.KeyPressMsg {
	r := rune(text[0])
	return tea.KeyPressMsg{Code: r, Text: text}
}

func TestNavigation_CtrlDPagesDownAndCtrlUPagesUp(t *testing.T) {
	// arrange
	lines := make([]string, 50)
	for i := range lines {
		lines[i] = fmt.Sprintf("line %d", i)
	}
	path := writeLog(t, strings.Join(lines, "\n"))
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 12) // bodyHeight ≈ 12-2(chrome) -> non-trivial pageStep
	drive(t, m, m.Init())

	// the viewer starts at the tail; jump to the top first so ctrl+d has
	// somewhere to advance into.
	for range 2 {
		m, _ = m.Update(keyPress("g"))
	}
	require.Equal(t, 0, m.Cursor())

	m, _ = m.Update(ctrlKey('d'))
	cursorAfterDown := m.Cursor()
	require.Positive(t, cursorAfterDown, "ctrl+d must move cursor down")

	m, _ = m.Update(ctrlKey('u'))
	assert.Less(t, m.Cursor(), cursorAfterDown, "ctrl+u must move cursor up")
}

func ctrlKey(r rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: r, Mod: tea.ModCtrl}
}
