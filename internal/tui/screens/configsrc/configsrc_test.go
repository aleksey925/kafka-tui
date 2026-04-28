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

func TestSearch_FiltersConfigTable(t *testing.T) {
	src := config.Sources{
		Config: map[string]config.Source{
			"logging.level":       {Path: "/g/c.yaml", Layer: config.LayerGlobal},
			"refresh.topics_list": {Path: "/g/c.yaml", Layer: config.LayerGlobal},
			"produce.history":     {Path: "/g/c.yaml", Layer: config.LayerGlobal},
		},
	}
	m := configsrc.New(configsrc.Options{Sources: src})

	_, _ = m.Update(keyPress("/"))
	for _, r := range "logging" {
		_, _ = m.Update(textKey(string(r)))
	}
	_, _ = m.Update(keyPress("enter"))

	out := m.View()
	assert.Contains(t, out, "logging.level")
	assert.NotContains(t, out, "refresh.topics_list")
	assert.NotContains(t, out, "produce.history")
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

func textKey(text string) tea.KeyPressMsg {
	r := rune(text[0])
	return tea.KeyPressMsg{Code: r, Text: text}
}
