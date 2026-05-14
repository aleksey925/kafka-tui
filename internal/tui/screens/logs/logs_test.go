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
		follow := m.Update(msg)
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
	_ = m.Update(keyPress("esc"))

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
	cmd := m.Update(keyPress("f"))

	// assert
	assert.True(t, m.Following())
	require.NotNil(t, cmd)
	// LIVE indicator lives in the frame title now.
	assert.Contains(t, m.Title(), "● LIVE")

	_ = m.Update(keyPress("f"))
	assert.False(t, m.Following())
}

func TestFollowTick_AppendsNewLines(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO first\n")
	m := logs.New(logs.Options{Path: path, FollowInterval: time.Millisecond})
	drive(t, m, m.Init())
	_ = m.Update(keyPress("f")) // toggle follow

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
	_ = m.Update(keyPress("f"))

	// act
	require.NoError(t, os.WriteFile(path, []byte("INFO fresh\n"), 0o600))
	drive(t, m, tickFollow())

	// assert
	assert.Equal(t, []string{"INFO fresh"}, m.Lines())
}

// Regression: handleLoaded used to return nil, so the rotation reload
// path (Truncated → loadCmd → LoadedMsg) terminated the follow tick chain
// even though m.follow stayed true. The LIVE indicator was on but no new
// lines arrived until the user toggled follow off and on.
func TestRotation_KeepsFollowTickAlive(t *testing.T) {
	// arrange — start with enough content that a shrink is detectable.
	path := writeLog(t, "INFO 1\nINFO 2\nINFO 3\n")
	m := logs.New(logs.Options{Path: path, FollowInterval: time.Millisecond})
	drive(t, m, m.Init())
	_ = m.Update(keyPress("f"))
	require.NoError(t, os.WriteFile(path, []byte("X\n"), 0o600))

	// act — manually walk the rotation chain so we can inspect the last cmd
	// returned by handleLoaded, which the bug used to make nil.
	tickCmd := m.Update(logs.FollowTickMsg{})
	require.NotNil(t, tickCmd)
	appended, ok := tickCmd().(logs.AppendedMsg)
	require.True(t, ok)
	require.True(t, appended.Truncated, "shrunken file must produce Truncated AppendedMsg")

	reloadCmd := m.Update(appended)
	require.NotNil(t, reloadCmd)
	loaded, ok := reloadCmd().(logs.LoadedMsg)
	require.True(t, ok)

	next := m.Update(loaded)

	// assert
	require.NotNil(t, next,
		"handleLoaded must schedule the next tick while follow is on — otherwise rotation kills the chain")
	_, isTick := next().(logs.FollowTickMsg)
	assert.True(t, isTick, "expected a FollowTickMsg to keep LIVE mode alive after rotation")
}

// Regression: handleLoaded used to unconditionally snap the cursor to the
// bottom of the new content on every reload — so a user reading mid-file
// while not following would be yanked to the end every time a LoadedMsg
// arrived (e.g. the race window where follow is toggled off between
// loadCmd and its message).
func TestHandleLoaded_PreservesCursorWhenNotFollowing(t *testing.T) {
	// arrange
	path := writeLog(t, "1\n2\n3\n4\n5\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())
	require.Equal(t, 4, m.Cursor(), "initial load should snap to bottom")
	// scroll up to a middle row.
	for range 2 {
		_ = m.Update(keyPress("k"))
	}
	require.Equal(t, 2, m.Cursor())

	// act — directly inject a reload with fresh content (simulating the
	// rotation path arriving while follow is off).
	_ = m.Update(logs.LoadedMsg{
		Lines:      []string{"a", "b", "c", "d", "e"},
		NextOffset: 10,
	})

	// assert
	assert.Equal(t, 2, m.Cursor(),
		"cursor index must be preserved across reload when not following")
}

func TestSearch_FindsMatchingLine(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO alpha\nINFO bravo\nINFO charlie\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())

	// act — host owns the prompt; drive the screen via SetSearch.
	m.SetSearch("bra")

	// assert
	assert.Equal(t, 1, m.Cursor())
}

func TestSearchN_JumpsToNextMatch(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO alpha\nINFO bravo alpha\nINFO charlie\nINFO alpha tail\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())

	m.SetSearch("alpha")
	require.Equal(t, 0, m.Cursor())

	// act
	_ = m.Update(textKey("n"))
	assert.Equal(t, 1, m.Cursor())
	_ = m.Update(textKey("n"))
	assert.Equal(t, 3, m.Cursor())
	_ = m.Update(textKey("N"))
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
	_ = m.Update(textKey("g"))
	_ = m.Update(textKey("g"))

	// assert
	assert.Equal(t, 0, m.Cursor())
}

func TestNavigation_BigGJumpsToBottom(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO 1\nINFO 2\nINFO 3\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())
	_ = m.Update(textKey("g"))
	_ = m.Update(textKey("g"))
	require.Equal(t, 0, m.Cursor())

	// act
	_ = m.Update(keyPress("G"))

	// assert
	assert.Equal(t, 2, m.Cursor())
}

func TestNavigation_JK(t *testing.T) {
	// arrange
	path := writeLog(t, "INFO 1\nINFO 2\nINFO 3\n")
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 10)
	drive(t, m, m.Init())
	_ = m.Update(textKey("g"))
	_ = m.Update(textKey("g"))
	require.Equal(t, 0, m.Cursor())

	// act / assert
	_ = m.Update(textKey("j"))
	assert.Equal(t, 1, m.Cursor())
	_ = m.Update(textKey("k"))
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
	for _, want := range []string{"follow", "filter", "next match", "previous match"} {
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

func TestNavigation_CtrlFPagesDownAndCtrlBPagesUp(t *testing.T) {
	// arrange
	lines := make([]string, 50)
	for i := range lines {
		lines[i] = fmt.Sprintf("line %d", i)
	}
	path := writeLog(t, strings.Join(lines, "\n"))
	m := logs.New(logs.Options{Path: path})
	m.SetSize(80, 12) // bodyHeight ≈ 12-2(chrome) -> non-trivial pageStep
	drive(t, m, m.Init())

	// the viewer starts at the tail; jump to the top first so ctrl+f has
	// somewhere to advance into.
	for range 2 {
		_ = m.Update(keyPress("g"))
	}
	require.Equal(t, 0, m.Cursor())

	_ = m.Update(ctrlKey('f'))
	cursorAfterDown := m.Cursor()
	require.Positive(t, cursorAfterDown, "ctrl+f must move cursor down")

	_ = m.Update(ctrlKey('b'))
	assert.Less(t, m.Cursor(), cursorAfterDown, "ctrl+b must move cursor up")
}

func ctrlKey(r rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: r, Mod: tea.ModCtrl}
}
