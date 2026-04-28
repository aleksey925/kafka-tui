package layout_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestHeader_RendersAllParts(t *testing.T) {
	s := theme.DefaultStyles()

	out := layout.Header(s, layout.HeaderInfo{
		Cluster:      "prod-east",
		ClusterColor: theme.ClusterRed,
		ReadOnly:     true,
		FromCLI:      true,
	})

	assert.Contains(t, out, "kafka-tui")
	assert.Contains(t, out, "prod-east")
	assert.Contains(t, out, "(red)")
	assert.Contains(t, out, "[RO]")
	assert.Contains(t, out, "(cli)")
}

func TestHeader_OnlyTitleWithoutCluster(t *testing.T) {
	s := theme.DefaultStyles()

	out := layout.Header(s, layout.HeaderInfo{})

	assert.Contains(t, out, "kafka-tui")
	assert.NotContains(t, out, "[RO]")
	assert.NotContains(t, out, "(cli)")
}

func TestStatus_AutoIncludesIntervalAndAge(t *testing.T) {
	s := theme.DefaultStyles()
	now := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)

	out := layout.Status(s, layout.StatusInfo{
		Mode:        layout.RefreshAuto,
		Interval:    5 * time.Second,
		LastRefresh: now.Add(-3 * time.Second),
		Now:         now,
	})

	assert.Contains(t, out, "auto: 5s")
	assert.Contains(t, out, "refreshed 3s ago")
}

func TestStatus_AutoWithoutLastRefresh(t *testing.T) {
	s := theme.DefaultStyles()
	out := layout.Status(s, layout.StatusInfo{
		Mode:     layout.RefreshAuto,
		Interval: 30 * time.Second,
	})
	assert.Contains(t, out, "auto: 30s")
	assert.NotContains(t, out, "refreshed")
}

func TestStatus_ManualAndPaused(t *testing.T) {
	s := theme.DefaultStyles()

	assert.Contains(t, layout.Status(s, layout.StatusInfo{Mode: layout.RefreshManual}), "manual")
	assert.Contains(t, layout.Status(s, layout.StatusInfo{Mode: layout.RefreshPaused}), "paused")
	assert.Empty(t, layout.Status(s, layout.StatusInfo{Mode: layout.RefreshOff}))
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
	})
	assert.Contains(t, out, ":")
	assert.Contains(t, out, "topics")

	withErr := layout.CommandLine(s, layout.CommandBar{
		Active: true,
		Prefix: ':',
		Buffer: "foo",
		Error:  "unknown",
	})
	assert.Contains(t, withErr, "unknown")
}

func TestCommandLine_InactiveIsEmpty(t *testing.T) {
	s := theme.DefaultStyles()
	assert.Empty(t, layout.CommandLine(s, layout.CommandBar{}))
}

func TestStatus_DurationFormatting(t *testing.T) {
	s := theme.DefaultStyles()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		elapsed time.Duration
		want    string
	}{
		{300 * time.Millisecond, "0s"},
		{45 * time.Second, "45s"},
		{2 * time.Minute, "2m"},
		{2*time.Minute + 15*time.Second, "2m15s"},
		{1 * time.Hour, "1h"},
		{1*time.Hour + 30*time.Minute, "1h30m"},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			out := layout.Status(s, layout.StatusInfo{
				Mode:        layout.RefreshAuto,
				Interval:    5 * time.Second,
				LastRefresh: now.Add(-c.elapsed),
				Now:         now,
			})
			assert.Contains(t, out, "refreshed "+c.want+" ago")
		})
	}
}
