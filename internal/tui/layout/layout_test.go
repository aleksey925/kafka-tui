package layout_test

import (
	"strings"
	"testing"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestHintsFromBindings_OnlyEmitsHintFlaggedEntries(t *testing.T) {
	// arrange
	bs := []keymap.Binding{
		{Keys: []string{"a"}, Label: "alpha", Hint: true},
		{Keys: []string{"b"}, Label: "bravo"},
		{Keys: []string{"c", "C"}, Label: "charlie", Hint: true},
	}

	// act
	got := layout.HintsFromBindings(bs)

	// assert
	assert.Len(t, got, 2)
	assert.Equal(t, "a", got[0].Key)
	assert.Equal(t, "c / C", got[1].Key)
}

func TestHeader_RendersAllParts(t *testing.T) {
	s := theme.DefaultStyles()

	out := layout.Header(s, layout.HeaderInfo{
		Cluster:      "prod-east",
		ClusterColor: theme.ClusterRed,
		ReadOnly:     true,
		FromCLI:      true,
	}, layout.StatusInfo{Mode: layout.RefreshNotApplicable}, []layout.KeyHint{
		{Key: "n", Label: "new"},
	}, layout.Build{Version: "v1.2.3", Commit: "abc"}, 120)

	assert.Contains(t, out, "kafka-tui")
	assert.Contains(t, out, "prod-east")
	assert.Contains(t, out, "read-only")
	assert.Contains(t, out, "cli")
	assert.Contains(t, out, "v1.2.3")
}

func TestHeader_OnlyTitleWithoutCluster(t *testing.T) {
	s := theme.DefaultStyles()

	out := layout.Header(s, layout.HeaderInfo{},
		layout.StatusInfo{Mode: layout.RefreshNotApplicable}, nil,
		layout.Build{Version: "v1.0.0"}, 120)

	assert.Contains(t, out, "kafka-tui")
	assert.NotContains(t, out, "read-only")
}

func TestCommandLine_RendersBufferAndError(t *testing.T) {
	s := theme.DefaultStyles()

	// cursor at end so the buffer renders as one contiguous run plus the
	// trailing block cursor — easy to grep for in the test.
	out := layout.CommandLine(s, layout.CommandBar{
		Active: true,
		Prefix: ':',
		Buffer: "topics",
		Cursor: 6,
	}, 60)
	assert.Contains(t, out, ":")
	assert.Contains(t, out, "topics")

	withErr := layout.CommandLine(s, layout.CommandBar{
		Active: true,
		Prefix: ':',
		Buffer: "foo",
		Cursor: 3,
		Error:  "unknown",
	}, 60)
	assert.Contains(t, withErr, "unknown")
}

func TestCommandLine_InactiveIsEmpty(t *testing.T) {
	s := theme.DefaultStyles()

	out := layout.CommandLine(s, layout.CommandBar{}, 40)

	// inactive prompt occupies zero rows — the body fills the freed space
	// and only shrinks when the bar opens.
	assert.Empty(t, out)
}

// TestHeader_RefreshLabelDropsAgoSuffix pins the chrome-compaction work:
// the elapsed-since-refresh marker no longer appends " ago" (the "·"
// separator already conveys "since last refresh"). Without this the long
// auto-refresh label would push flush against the menu pane on common
// 100-col terminals.
func TestHeader_RefreshLabelDropsAgoSuffix(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 5, 0, time.UTC)
	last := now.Add(-3 * time.Second)
	out := layout.Header(theme.DefaultStyles(),
		layout.HeaderInfo{Cluster: "alpha"},
		layout.StatusInfo{
			Mode:        layout.RefreshAuto,
			Interval:    30 * time.Second,
			LastRefresh: last,
			Now:         now,
		},
		nil,
		layout.Build{Version: "v0"},
		120,
	)
	assert.Contains(t, out, "auto 30s")
	assert.Contains(t, out, "· 3s")
	assert.NotContains(t, out, "ago")
}

// TestHeader_RefreshElapsedCoarsensAboveOneMinute pins the precision-vs-width
// trade-off: minute+ ranges floor to whole minutes (and hour+ to whole
// hours) so the value stays at 2-3 chars. Without this "1m25s ago" forms
// blew past the left pane.
func TestHeader_RefreshElapsedCoarsensAboveOneMinute(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		elapsed time.Duration
		want    string
	}{
		{45 * time.Second, "· 45s"},
		{90 * time.Second, "· 1m"},
		{75 * time.Minute, "· 1h"},
	}
	for _, tc := range cases {
		out := layout.Header(theme.DefaultStyles(),
			layout.HeaderInfo{Cluster: "alpha"},
			layout.StatusInfo{
				Mode:        layout.RefreshAuto,
				Interval:    30 * time.Second,
				LastRefresh: now.Add(-tc.elapsed),
				Now:         now,
			},
			nil,
			layout.Build{},
			120,
		)
		assert.Contains(t, out, tc.want, "elapsed=%s", tc.elapsed)
	}
}

// TestHeader_LeftPaneReservesGutter pins the visual-separation guarantee:
// even when a row's value butts against the right edge, the left pane
// truncates so the menu pane never sits flush against it on the same row.
func TestHeader_LeftPaneReservesGutter(t *testing.T) {
	// a 60-char terminal puts leftW = max(20, 60/3) = 20. A long cluster
	// name overflows the inner area; truncation must kick in.
	out := layout.Header(theme.DefaultStyles(),
		layout.HeaderInfo{Cluster: "production-east-region-multi-az-cluster-1"},
		layout.StatusInfo{Mode: layout.RefreshNotApplicable},
		nil,
		layout.Build{},
		60,
	)
	for line := range strings.SplitSeq(out, "\n") {
		assert.LessOrEqual(t, lipgloss.Width(line), 60,
			"each rendered line must stay within terminal width: %q", line)
	}
}

func TestHeader_NarrowTerminalUsesCompactFallback(t *testing.T) {
	out := layout.Header(
		theme.DefaultStyles(),
		layout.HeaderInfo{Cluster: "alpha", ReadOnly: true},
		layout.StatusInfo{},
		nil,
		layout.Build{},
		20, // < 40 forces compact path
	)
	assert.Contains(t, out, "kafka-tui")
	assert.Contains(t, out, "alpha")
	assert.Contains(t, out, "[RO]")
	assert.NotContains(t, out, "Cluster", "compact header must not include the multi-row labels")
}

func TestHeader_InsecureTLSRendersWarnInFullAndCompact(t *testing.T) {
	s := theme.DefaultStyles()

	full := layout.Header(s, layout.HeaderInfo{
		Cluster:     "risky",
		InsecureTLS: true,
	}, layout.StatusInfo{Mode: layout.RefreshNotApplicable}, nil,
		layout.Build{Version: "v0"}, 120)
	assert.Contains(t, full, "no-tls-verify", "full header must annotate insecure-tls in Mode row")

	compact := layout.Header(s, layout.HeaderInfo{
		Cluster:     "risky",
		InsecureTLS: true,
	}, layout.StatusInfo{}, nil, layout.Build{}, 20)
	assert.Contains(t, compact, "[NO-TLS-VERIFY]", "compact header must include [NO-TLS-VERIFY] marker")
}

func TestHeader_InsecureTLSAbsent_NoMarker(t *testing.T) {
	// matched-pair negative test so a future regression that always renders
	// the marker (e.g. dropping the `if info.InsecureTLS` guard) is caught.
	s := theme.DefaultStyles()

	full := layout.Header(s, layout.HeaderInfo{Cluster: "safe"},
		layout.StatusInfo{Mode: layout.RefreshNotApplicable}, nil,
		layout.Build{Version: "v0"}, 120)
	assert.NotContains(t, full, "no-tls-verify")

	compact := layout.Header(s, layout.HeaderInfo{Cluster: "safe"},
		layout.StatusInfo{}, nil, layout.Build{}, 20)
	assert.NotContains(t, compact, "[NO-TLS-VERIFY]")
}

// TestHeader_LongHintsDoNotCollide guards the menu pane against a long hint in
// one column overrunning into the next. The produce form has the longest
// labels in the app; with a fixed width/cols split they used to butt up against
// the neighboring key ("previous field<enter>").
func TestHeader_LongHintsDoNotCollide(t *testing.T) {
	s := theme.DefaultStyles()
	hints := []layout.KeyHint{
		{Key: "+ / _", Label: "toggle fullscreen"},
		{Key: "tab / down / j", Label: "next field"},
		{Key: "shift+tab / up / k", Label: "previous field"},
		{Key: "ctrl+u", Label: "clear form"},
		{Key: "s", Label: "send"},
		{Key: "e", Label: "open record in $EDITOR"},
		{Key: "enter", Label: "edit focused field"},
		{Key: "esc", Label: "cancel edit / close form"},
	}
	out := layout.Header(s, layout.HeaderInfo{Cluster: "local"},
		layout.StatusInfo{Mode: layout.RefreshNotApplicable}, hints,
		layout.Build{Version: "v0"}, 160)

	plain := stripANSI(out)
	// a label flush against the next column's "<key>" is the collision signature.
	assert.NotContains(t, plain, "field<")
	// the labels themselves must survive intact at this width (no truncation).
	assert.Contains(t, plain, "previous field")
	assert.Contains(t, plain, "edit focused field")
}

// TestHeader_MenuAlignsLabelsWithinColumn pins the k9s-style intra-column
// alignment: keys in a column vary in width (`<r>` vs `<ctrl+e>`), so each key
// is padded to the column's widest key and every label in that column starts
// at the same x. hints[0] and hints[1] share column 0 (column-major fill,
// 3 hints over 2 columns => 2 rows), so their labels must line up.
func TestHeader_MenuAlignsLabelsWithinColumn(t *testing.T) {
	s := theme.DefaultStyles()
	hints := []layout.KeyHint{
		{Key: "r", Label: "refresh"},
		{Key: "ctrl+e", Label: "edit record"},
		{Key: "q", Label: "quit"},
	}
	out := layout.Header(s, layout.HeaderInfo{Cluster: "local"},
		layout.StatusInfo{Mode: layout.RefreshNotApplicable}, hints,
		layout.Build{Version: "v0"}, 160)

	lines := strings.Split(out, "\n")
	// the left pane has a fixed visible width, so a label's absolute offset in
	// the full plain line differs from its menu-relative offset by the same
	// constant on every row - comparing absolute offsets is enough.
	row0, row1 := stripANSI(lines[0]), stripANSI(lines[1])
	assert.Equal(t,
		strings.Index(row0, "refresh"),
		strings.Index(row1, "edit record"),
		"labels in the same menu column must start at the same offset")
}

// TestHeader_MenuShowsEveryHint guards against the silent-drop regression: a
// fixed 2-column grid caps at 2*HeaderRows hints, but topics/messages list
// views carry more than that. The column count must grow so no hint vanishes.
func TestHeader_MenuShowsEveryHint(t *testing.T) {
	s := theme.DefaultStyles()
	hints := make([]layout.KeyHint, 12)
	for i := range hints {
		hints[i] = layout.KeyHint{Key: "k", Label: "hint-label-" + string(rune('a'+i))}
	}
	// a wide terminal so every label survives without truncation.
	out := layout.Header(s, layout.HeaderInfo{Cluster: "local"},
		layout.StatusInfo{Mode: layout.RefreshNotApplicable}, hints,
		layout.Build{Version: "v0"}, 220)

	plain := stripANSI(out)
	for _, h := range hints {
		assert.Contains(t, plain, h.Label, "hint dropped from the menu bar")
	}
}

// TestHeader_MenuFansColumnsOnModeratePane pins the narrow-pane improvement:
// at a common 100-col terminal the menu pane is far below the old single-column
// threshold, yet the column count must still grow enough to show every hint of
// a typical browse view instead of capping at HeaderRows.
func TestHeader_MenuFansColumnsOnModeratePane(t *testing.T) {
	s := theme.DefaultStyles()
	hints := make([]layout.KeyHint, 8)
	for i := range hints {
		hints[i] = layout.KeyHint{Key: "x", Label: "lbl" + string(rune('a'+i))}
	}
	out := layout.Header(s, layout.HeaderInfo{Cluster: "local"},
		layout.StatusInfo{Mode: layout.RefreshNotApplicable}, hints,
		layout.Build{Version: "v0"}, 100)

	plain := stripANSI(out)
	for _, h := range hints {
		assert.Contains(t, plain, h.Label, "hint dropped on a moderate-width pane")
	}
	for line := range strings.SplitSeq(out, "\n") {
		assert.LessOrEqual(t, lipgloss.Width(line), 100, "line overflowed terminal width: %q", line)
	}
}

func stripANSI(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			j := i + 2
			for j < len(s) && (s[j] < 0x40 || s[j] > 0x7e) {
				j++
			}
			if j < len(s) {
				j++
			}
			i = j - 1
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}
