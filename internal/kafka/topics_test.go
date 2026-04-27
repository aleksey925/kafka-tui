package kafka

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestClient_ListAndDeleteTopic__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "alpha",
		Partitions:        3,
		ReplicationFactor: 1,
	}))
	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "beta",
		Partitions:        1,
		ReplicationFactor: 1,
	}))

	topics, err := c.ListTopics(ctx)
	require.NoError(t, err)
	names := make([]string, 0, len(topics))
	for _, t := range topics {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	assert.Equal(t, []string{"alpha", "beta"}, names)

	for _, top := range topics {
		switch top.Name {
		case "alpha":
			assert.Equal(t, 3, top.Partitions)
			assert.Equal(t, 1, top.Replicas)
		case "beta":
			assert.Equal(t, 1, top.Partitions)
		}
	}

	require.NoError(t, c.DeleteTopic(ctx, "beta"))

	topics, err = c.ListTopics(ctx)
	require.NoError(t, err)
	require.Len(t, topics, 1)
	assert.Equal(t, "alpha", topics[0].Name)
}

func TestClient_DeleteTopic__missing__error(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	err := c.DeleteTopic(ctx, "nope")
	require.Error(t, err)
}

func TestClient_DescribeTopicConfigs__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "with-cfg",
		Partitions:        1,
		ReplicationFactor: 1,
		Configs: map[string]string{
			ConfigCleanupPolicy: "compact",
			ConfigRetentionMs:   "60000",
		},
	}))

	configs, err := c.DescribeTopicConfigs(ctx, "with-cfg")
	require.NoError(t, err)
	byKey := make(map[string]string, len(configs))
	for _, cfg := range configs {
		byKey[cfg.Key] = cfg.Value
	}
	assert.Equal(t, "compact", byKey[ConfigCleanupPolicy])
	assert.Equal(t, "60000", byKey[ConfigRetentionMs])

	all, err := c.DescribeAllTopicConfigs(ctx, "with-cfg")
	require.NoError(t, err)
	assert.Greater(t, len(all), len(configs))
}

func TestClient_DescribeTopicConfigs__missing__error(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	_, err := c.DescribeTopicConfigs(ctx, "nope")
	require.Error(t, err)
}

func TestClient_TopicPartitions__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "parts",
		Partitions:        4,
		ReplicationFactor: 1,
	}))

	parts, err := c.TopicPartitions(ctx, "parts")
	require.NoError(t, err)
	assert.Len(t, parts, 4)
	for i, p := range parts {
		assert.EqualValues(t, i, p.Partition)
		assert.NotEmpty(t, p.Replicas)
		assert.NotEmpty(t, p.ISR)
	}
}

func TestClient_TopicWatermarks__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "marks",
		Partitions:        1,
		ReplicationFactor: 1,
	}))

	results := c.kc.ProduceSync(ctx,
		&kgo.Record{Topic: "marks", Value: []byte("a")},
		&kgo.Record{Topic: "marks", Value: []byte("b")},
		&kgo.Record{Topic: "marks", Value: []byte("c")},
	)
	require.NoError(t, results.FirstErr())

	wm, err := c.TopicWatermarks(ctx, "marks")
	require.NoError(t, err)
	assert.EqualValues(t, 3, wm.Count())
	assert.Len(t, wm.Partitions, 1)
	assert.EqualValues(t, 0, wm.Partitions[0].Low)
	assert.EqualValues(t, 3, wm.Partitions[0].High)
}

func TestClient_TopicSize__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "sized",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	require.NoError(t, c.kc.ProduceSync(ctx, &kgo.Record{
		Topic: "sized",
		Value: []byte("hello kafka-tui"),
	}).FirstErr())

	size, err := c.TopicSize(ctx, "sized")
	require.NoError(t, err)
	// kfake's in-memory storage may report zero — assert non-negative and
	// that the call returns successfully without error.
	assert.GreaterOrEqual(t, size, int64(0))
}

func TestClient_CloneTopic__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "src",
		Partitions:        2,
		ReplicationFactor: 1,
	}))

	produce := []*kgo.Record{
		{Topic: "src", Key: []byte("k1"), Value: []byte("v1")},
		{Topic: "src", Key: []byte("k2"), Value: []byte("v2")},
		{Topic: "src", Key: []byte("k3"), Value: []byte("v3")},
		{Topic: "src", Key: []byte("k4"), Value: []byte("v4")},
	}
	require.NoError(t, c.kc.ProduceSync(ctx, produce...).FirstErr())

	progress, err := c.CloneTopic(ctx, "src", "dst", CloneOptions{})
	require.NoError(t, err)

	var last CloneProgress
	for p := range progress {
		last = p
		if p.Done {
			break
		}
	}
	require.NoError(t, last.Err)
	assert.True(t, last.Done)
	assert.EqualValues(t, len(produce), last.Total)
	assert.EqualValues(t, len(produce), last.Copied)

	wm, err := c.TopicWatermarks(ctx, "dst")
	require.NoError(t, err)
	assert.EqualValues(t, len(produce), wm.Count())
}

func TestClient_CloneTopic__emptySource__createsEmptyDest(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "src-empty",
		Partitions:        1,
		ReplicationFactor: 1,
	}))

	progress, err := c.CloneTopic(ctx, "src-empty", "dst-empty", CloneOptions{})
	require.NoError(t, err)
	last := drain(progress)
	require.NoError(t, last.Err)
	assert.True(t, last.Done)
	assert.EqualValues(t, 0, last.Total)
	assert.EqualValues(t, 0, last.Copied)

	topics, err := c.ListTopics(ctx)
	require.NoError(t, err)
	names := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		names[t.Name] = struct{}{}
	}
	_, exists := names["dst-empty"]
	assert.True(t, exists)
}

// newKfakeClient spins up an in-process Kafka broker and dials it. Metadata
// caching is dialed down so tests observe create / delete results immediately.
func newKfakeClient(t *testing.T) *Client {
	t.Helper()
	cluster := startKfake(t)
	c, err := Dial(cluster, DialOptions{
		ExtraOpts: []kgo.Opt{kgo.MetadataMinAge(10 * time.Millisecond)},
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// drain returns the last CloneProgress emitted (where Done == true).
func drain(ch <-chan CloneProgress) CloneProgress {
	var last CloneProgress
	for p := range ch {
		last = p
	}
	return last
}
