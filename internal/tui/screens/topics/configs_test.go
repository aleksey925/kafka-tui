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
		_, follow := m.Update(msg)
		queue = append(queue, follow)
	}
}

func TestConfigsScreen_LoadsAndRendersConfigsAndPartitions(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{
		{Key: kafka.ConfigCleanupPolicy, Value: "compact", Source: "DYNAMIC_TOPIC_CONFIG"},
		{Key: kafka.ConfigRetentionMs, Value: "60000", Source: "STATIC_BROKER_CONFIG"},
	}
	svc.parts["alpha"] = []kafka.PartitionDetail{
		{Partition: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}},
		{Partition: 1, Leader: 2, Replicas: []int32{2, 3}, ISR: []int32{2}},
	}

	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())

	assert.Len(t, m.Configs(), 2)
	assert.Len(t, m.Partitions(), 2)
	out := m.View()
	assert.Contains(t, out, "Topic configs · alpha")
	assert.Contains(t, out, "Partitions · alpha")
	assert.Contains(t, out, kafka.ConfigCleanupPolicy)
	assert.Contains(t, out, "compact")
	assert.Contains(t, out, "DYNAMIC_TOPIC_CONFIG")
	// partition table shows leader and replicas
	assert.Contains(t, out, "1,2")
}

func TestConfigsScreen_LoadErrorRaisesToast(t *testing.T) {
	svc := newConfigsFake()
	svc.cfgErr = errors.New("metadata timeout")
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())
	out := m.View()
	assert.Contains(t, out, "metadata timeout")
}

func TestConfigsScreen_TabSwitchesFocus(t *testing.T) {
	svc := newConfigsFake()
	svc.configs["alpha"] = []kafka.TopicConfig{{Key: "k", Value: "v", Source: "s"}}
	svc.parts["alpha"] = []kafka.PartitionDetail{{Partition: 0, Leader: 1}}

	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())

	require.False(t, m.FocusPartitions())
	_, _ = m.Update(keyPress("tab"))
	assert.True(t, m.FocusPartitions())
	_, _ = m.Update(keyPress("tab"))
	assert.False(t, m.FocusPartitions())
}

func TestConfigsScreen_EscRaisesBack(t *testing.T) {
	svc := newConfigsFake()
	m := topics.NewConfigsModel(topics.ConfigsOptions{Service: svc, Topic: "alpha"})
	driveConfigs(t, m, m.Init())
	_, _ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Back)
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
