package configsrc_test

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/configsrc"
)

func TestNew_RendersBothTables(t *testing.T) {
	// arrange
	src := config.Sources{
		Config: map[string]config.Source{
			"logging.level": {Path: "/p/.kafka-tui/config.yaml", Layer: config.LayerProject},
			"refresh.topics_list": {
				Path:  "/h/.kafka-tui/config.yaml",
				Layer: config.LayerGlobal,
			},
		},
		Clusters: map[string]map[string]config.Source{
			"prod": {
				"brokers": {Path: "/h/.kafka-tui/clusters.yaml", Layer: config.LayerGlobal},
				"sasl.password": {
					Path:  "/p/.kafka-tui/clusters.yaml",
					Layer: config.LayerProject,
				},
			},
		},
	}

	// act
	m := configsrc.New(configsrc.Options{Sources: src})

	// assert
	out := m.View()
	assert.Contains(t, out, "logging.level")
	assert.Contains(t, out, "refresh.topics_list")
	assert.Contains(t, out, string(config.LayerProject))
	assert.Contains(t, out, string(config.LayerGlobal))
	assert.Contains(t, out, "prod")
	assert.Contains(t, out, "brokers")
	assert.Contains(t, out, "sasl.password")
}

func TestEsc_RaisesBackAction(t *testing.T) {
	// arrange
	m := configsrc.New(configsrc.Options{})

	// act
	_, _ = m.Update(keyPress("esc"))

	// assert
	assert.True(t, m.ConsumeAction().Back)
}

func TestTab_TogglesFocus(t *testing.T) {
	// arrange
	m := configsrc.New(configsrc.Options{})
	require.False(t, m.FocusClusters())

	// act / assert
	_, _ = m.Update(keyPress("tab"))
	assert.True(t, m.FocusClusters())
	_, _ = m.Update(keyPress("tab"))
	assert.False(t, m.FocusClusters())
}

func TestEmpty_ConfigSourcesShowsNoRows(t *testing.T) {
	// arrange
	m := configsrc.New(configsrc.Options{})

	// act
	out := m.View()

	// assert
	assert.Contains(t, out, "Config fields")
	assert.Contains(t, out, "Cluster fields")
	assert.Contains(t, out, "(no rows)")
}

func TestKeyHints_IncludesExpectedLabels(t *testing.T) {
	m := configsrc.New(configsrc.Options{})
	hints := m.KeyHints()
	labels := make([]string, 0, len(hints))
	for _, h := range hints {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	for _, want := range []string{"switch table", "search", "sort", "back"} {
		assert.Contains(t, got, want)
	}
}

// TestSetSearch_AppliesToBothTables pins the screen-level filter
// contract: the host-driven `/` prompt narrows both sub-tables (config
// fields and per-cluster fields), so an esc-cascade can clear the
// filter regardless of which sub-table currently has focus.
func TestSetSearch_AppliesToBothTables(t *testing.T) {
	src := config.Sources{
		Config: map[string]config.Source{
			"logging.level":       {Path: "/g/c.yaml", Layer: config.LayerGlobal},
			"refresh.topics_list": {Path: "/g/c.yaml", Layer: config.LayerGlobal},
			"produce.history":     {Path: "/g/c.yaml", Layer: config.LayerGlobal},
		},
		Clusters: map[string]map[string]config.Source{
			"alpha": {"brokers": {Path: "/g/clusters.yaml", Layer: config.LayerGlobal}},
			"beta":  {"brokers": {Path: "/g/clusters.yaml", Layer: config.LayerGlobal}},
		},
	}
	m := configsrc.New(configsrc.Options{Sources: src})

	m.SetSearch("logging")
	out := m.View()
	assert.Contains(t, out, "logging.level")
	assert.NotContains(t, out, "refresh.topics_list")
	assert.NotContains(t, out, "produce.history")
	// per-cluster table holds rows keyed by cluster name; "logging" is
	// not a substring of any of them, so the second table must collapse
	// too — proving the filter applies to both.
	assert.NotContains(t, out, "alpha")
	assert.NotContains(t, out, "beta")
	assert.Equal(t, "logging", m.ActiveFilter())

	// switching focus to the cluster table must keep the same filter
	// reported by ActiveFilter so the host's esc-cascade can clear it.
	_, _ = m.Update(keyPress("tab"))
	require.True(t, m.FocusClusters())
	assert.Equal(t, "logging", m.ActiveFilter())

	m.SetSearch("")
	assert.Empty(t, m.ActiveFilter())
}

func keyPress(name string) tea.KeyPressMsg {
	switch name {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "tab":
		return tea.KeyPressMsg{Code: tea.KeyTab}
	}
	if len(name) == 1 {
		r := rune(name[0])
		return tea.KeyPressMsg{Code: r, Text: string(r)}
	}
	return tea.KeyPressMsg{}
}
