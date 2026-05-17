package topics_test

import (
	"context"
	"errors"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
)

func driveConfigs(t *testing.T, m *topics.ConfigsModel, cmd tea.Cmd) {
	t.Helper()
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
		follow := m.Update(msg)
		queue = append(queue, follow)
	}
}

func TestConfigsScreen_GroupsRowsByCategory(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: "min.compaction.lag.ms", Value: "0", Source: "DEFAULT_CONFIG"},
		{Key: "compression.type", Value: "producer", Source: "DEFAULT_CONFIG"},
		{Key: kafka.ConfigRetentionMs, Value: "60000", Source: "STATIC_BROKER_CONFIG"},
	}

	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())

	// configs are loaded
	assert.Len(t, m.Configs(), 3)

	out := m.View()
	assert.Contains(t, out, "Topic configs · alpha")
	// every category that has at least one row appears as a header
	assert.Contains(t, out, "Compaction")
	assert.Contains(t, out, "Compression")
	assert.Contains(t, out, "Retention")
	// row body shows key + value (source moved to the help popup)
	assert.Contains(t, out, "compression.type")
	assert.Contains(t, out, "producer")
}

func TestConfigsScreen_UnknownKeyFallsBackToOther(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: "vendor.custom.feature", Value: "on", Source: "DYNAMIC_TOPIC_CONFIG"},
	}
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())

	out := m.View()
	assert.Contains(t, out, "Other")
	assert.Contains(t, out, "vendor.custom.feature")
}

func TestConfigsScreen_LoadErrorRaisesToast(t *testing.T) {
	svc := newConfigsFake()
	svc.cfgErr = errors.New("metadata timeout")
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())
	out := m.View()
	assert.Contains(t, out, "metadata timeout")
}

func TestConfigsScreen_EscRaisesBack(t *testing.T) {
	svc := newConfigsFake()
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())
	_ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Back)
}

func TestConfigsScreen_EnterRaisesEdit(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: kafka.ConfigCleanupPolicy, Value: "delete", Source: "DEFAULT_CONFIG"},
	}
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())

	_ = m.Update(keyPress("enter"))
	assert.Equal(t, kafka.ConfigCleanupPolicy, m.ConsumeAction().Edit)
}

func TestConfigsScreen_EditBlockedInReadOnly(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: kafka.ConfigCleanupPolicy, Value: "delete", Source: "DEFAULT_CONFIG"},
	}
	m := topics.NewConfigsModel(topics.ConfigsOptions{
		Service:  svc,
		Topic:    "alpha",
		ReadOnly: true,
	})
	driveConfigs(t, m, m.Init())

	_ = m.Update(keyPressRune('e'))
	assert.Empty(t, m.ConsumeAction().Edit, "edit must not propagate in read-only")

	flash, ok := m.LatestFlash()
	require.True(t, ok)
	assert.Contains(t, flash.Message, "read-only")
}

func TestConfigsScreen_HelpToggleShowsDocumentation(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: "compression.type", Value: "producer", Source: "DEFAULT_CONFIG"},
	}
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	m.SetSize(120, 30)
	driveConfigs(t, m, m.Init())

	require.False(t, m.HelpOpen())
	_ = m.Update(keyPressRune('i'))
	require.True(t, m.HelpOpen())

	out := m.View()
	// the bundled doc for compression.type mentions the gzip codec.
	assert.Contains(t, out, "gzip")
	assert.Contains(t, out, "type: select")

	// pressing esc closes the overlay before yielding to the host.
	_ = m.Update(keyPress("esc"))
	assert.False(t, m.HelpOpen())
	assert.False(t, m.ConsumeAction().Back, "esc must close the overlay first")
}

func TestConfigsScreen_SetSearchFiltersRows(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: kafka.ConfigRetentionMs, Value: "60000", Source: "DEFAULT_CONFIG"},
		{Key: "compression.type", Value: "producer", Source: "DEFAULT_CONFIG"},
	}
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())

	// the filter matches key, value, source, and category — "compression"
	// hits exactly one row by both key and category.
	m.SetSearch("compression")
	out := m.View()
	assert.Contains(t, out, "compression.type")
	assert.NotContains(t, out, kafka.ConfigRetentionMs)
	assert.Equal(t, "compression", m.ActiveFilter())

	m.SetSearch("")
	assert.Empty(t, m.ActiveFilter())
}

func TestConfigsScreen_ViewportScrollsWithCursor(t *testing.T) {
	svc := newConfigsFake()
	cfgs := make([]kafka.TopicConfig, 0, 30)
	for _, k := range []string{
		"min.compaction.lag.ms", "max.compaction.lag.ms", "min.cleanable.dirty.ratio",
		"compression.type", "compression.gzip.level", "compression.lz4.level", "compression.zstd.level",
		"retention.ms", "retention.bytes", "delete.retention.ms",
	} {
		cfgs = append(cfgs, kafka.TopicConfig{Key: k, Value: "v"})
	}
	svc.configs["alpha"] = cfgs
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	// only 6 list lines fit (height 8, minus 2 chrome rows).
	m.SetSize(120, 8)
	driveConfigs(t, m, m.Init())

	// cursor at top: first key visible, last key not.
	out := m.View()
	require.Contains(t, out, "min.cleanable.dirty.ratio") // first row in Compaction
	assert.NotContains(t, out, "delete.retention.ms")     // last row, off-screen

	// page down repeatedly — viewport must follow until the bottom row
	// is reachable.
	for range 4 {
		_ = m.Update(keyPress("ctrl+f"))
	}
	out = m.View()
	assert.Contains(t, out, "delete.retention.ms")
	assert.NotContains(t, out, "min.cleanable.dirty.ratio")
}

func TestConfigsScreen_HelpRendersAsCenteredPopup(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: "compression.type", Value: "producer", Source: "DEFAULT_CONFIG"},
	}
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	m.SetSize(120, 30)
	driveConfigs(t, m, m.Init())

	_ = m.Update(keyPressRune('i'))
	require.True(t, m.HelpOpen())

	out := m.View()
	// popup overlays the list area so the row body must NOT remain
	// visible behind it.
	assert.NotContains(t, out, "▸ ")
	// popup contents include the documentation and source.
	assert.Contains(t, out, "DEFAULT_CONFIG")
	assert.Contains(t, out, "type: select")
}

func TestConfigsScreen_FocusKeyPositionsCursorAfterLoad(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: "compression.type", Value: "producer", Source: "DEFAULT_CONFIG"},
		{Key: kafka.ConfigCleanupPolicy, Value: "compact", Source: "DEFAULT_CONFIG"},
		{Key: kafka.ConfigRetentionMs, Value: "60000", Source: "DEFAULT_CONFIG"},
	}
	m := topics.NewConfigsModel(topics.ConfigsOptions{
		Service:  svc,
		Topic:    "alpha",
		FocusKey: kafka.ConfigRetentionMs,
	})
	driveConfigs(t, m, m.Init())

	// without FocusKey the cursor would land on compression.type
	// (sorted first by category). FocusKey overrides that for one load.
	assert.Equal(t, kafka.ConfigRetentionMs, m.SelectedKey())
}

func TestConfigsScreen_DownArrowMovesCursor(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: "compression.type", Value: "producer", Source: "DEFAULT_CONFIG"},
		{Key: kafka.ConfigCleanupPolicy, Value: "compact", Source: "DEFAULT_CONFIG"},
	}
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())

	// rows are sorted by category, then key — Compression < Retention.
	require.Equal(t, "compression.type", m.SelectedKey())
	_ = m.Update(keyPress("down"))
	assert.Equal(t, kafka.ConfigCleanupPolicy, m.SelectedKey())
}

// ----- helpers -----

type configsFakeService struct {
	*fakeService
	cfgErr error
}

func newConfigsFake() *configsFakeService {
	return &configsFakeService{fakeService: newFakeService(nil, nil)}
}

func (f *configsFakeService) DescribeAllTopicConfigs(ctx context.Context, topic string) ([]kafka.TopicConfig, error) {
	if f.cfgErr != nil {
		return nil, f.cfgErr
	}
	return f.fakeService.DescribeAllTopicConfigs(ctx, topic)
}
