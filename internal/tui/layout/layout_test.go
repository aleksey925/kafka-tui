package layout_test

import (
	"testing"

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
	}, layout.StatusInfo{Mode: layout.RefreshOff}, []layout.KeyHint{
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
		layout.StatusInfo{Mode: layout.RefreshOff}, nil,
		layout.Build{Version: "v1.0.0"}, 120)

	assert.Contains(t, out, "kafka-tui")
	assert.NotContains(t, out, "read-only")
}

func TestKeyHints_RendersPairs(t *testing.T) {
	s := theme.DefaultStyles()
	out := layout.KeyHints(s, []layout.KeyHint{
		{Key: ":", Label: "command"},
		{Key: "?", Label: "help"},
	})

	assert.Contains(t, out, ":")
	assert.Contains(t, out, "command")
	assert.Contains(t, out, "?")
	assert.Contains(t, out, "help")
}

func TestKeyHints_EmptyList(t *testing.T) {
	s := theme.DefaultStyles()
	assert.Empty(t, layout.KeyHints(s, nil))
}

func TestCommandLine_RendersBufferAndError(t *testing.T) {
	s := theme.DefaultStyles()

	out := layout.CommandLine(s, layout.CommandBar{
		Active: true,
		Prefix: ':',
		Buffer: "topics",
	}, 60)
	assert.Contains(t, out, ":")
	assert.Contains(t, out, "topics")

	withErr := layout.CommandLine(s, layout.CommandBar{
		Active: true,
		Prefix: ':',
		Buffer: "foo",
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
