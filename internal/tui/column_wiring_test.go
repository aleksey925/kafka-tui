package tui

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/config"
)

// TestScreenFactory_ForwardsConfiguredColumns guards against the wiring drift
// where a *.columns config field exists but the screen factory never passes it
// to the screen. Each list screen is built through its factory with a
// non-default column selection; the rendered header must reflect it - present
// the kept column, drop the removed one. A factory that ignores the config keeps
// every default column and fails the absent check.
func TestScreenFactory_ForwardsConfiguredColumns(t *testing.T) {
	cases := []struct {
		name    string
		cfg     func(*config.Config)
		build   func(*Model) screenView
		present string
		absent  string
	}{
		{
			name:    "topics",
			cfg:     func(c *config.Config) { c.Topics.Columns = []string{"size", "name"} },
			build:   func(m *Model) screenView { return m.newTopics() },
			present: "Size",
			absent:  "Partitions",
		},
		{
			name:    "messages",
			cfg:     func(c *config.Config) { c.Messages.Columns = []string{"offset", "key"} },
			build:   func(m *Model) screenView { return m.newMessages() },
			present: "Offset",
			absent:  "Timestamp",
		},
		{
			name:    "groups",
			cfg:     func(c *config.Config) { c.Groups.Columns = []string{"total_lag", "name"} },
			build:   func(m *Model) screenView { return m.newGroups() },
			present: "Total Lag",
			absent:  "Coordinator",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// arrange
			cfg := config.Defaults()
			tc.cfg(&cfg)
			m := &Model{boot: &Bootstrap{Loaded: &config.Loaded{Config: cfg}}}

			// act
			screen := tc.build(m)
			screen.SetSize(200, 40)
			view := screen.View()

			// assert
			assert.Contains(t, view, tc.present)
			assert.NotContains(t, view, tc.absent)
		})
	}
}

type screenView interface {
	SetSize(width, height int)
	View() string
}
