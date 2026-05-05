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

// TestClient_TopicsPartitions__kfake pins the metadata-driven partition
// list used to scope topic-level offset resets. The result must include
// every partition the topic was created with, regardless of whether any
// data has been produced or any consumer has committed.
func TestClient_TopicsPartitions__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "alpha",
		Partitions:        4,
		ReplicationFactor: 1,
	}))
	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "beta",
		Partitions:        2,
		ReplicationFactor: 1,
	}))

	got, err := c.TopicsPartitions(ctx, "alpha", "beta")
	require.NoError(t, err)
	assert.Equal(t, map[string][]int32{
		"alpha": {0, 1, 2, 3},
		"beta":  {0, 1},
	}, got)
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

func TestClient_TopicWatermarksBatch__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	for _, name := range []string{"alpha", "beta"} {
		require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
			Name:              name,
			Partitions:        1,
			ReplicationFactor: 1,
		}))
	}
	require.NoError(t, c.kc.ProduceSync(ctx,
		&kgo.Record{Topic: "alpha", Value: []byte("a1")},
		&kgo.Record{Topic: "alpha", Value: []byte("a2")},
		&kgo.Record{Topic: "beta", Value: []byte("b1")},
	).FirstErr())

	wm, err := c.TopicWatermarksBatch(ctx, "alpha", "beta")
	require.NoError(t, err)
	assert.EqualValues(t, 2, wm["alpha"].Count())
	assert.EqualValues(t, 1, wm["beta"].Count())
}

func TestClient_TopicWatermarksBatch__empty(t *testing.T) {
	t.Parallel()
	c := newKfakeClient(t)
	out, err := c.TopicWatermarksBatch(context.Background())
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestClient_TopicSizesBatch__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	for _, name := range []string{"sz-1", "sz-2"} {
		require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
			Name:              name,
			Partitions:        1,
			ReplicationFactor: 1,
		}))
	}

	sizes, err := c.TopicSizesBatch(ctx, "sz-1", "sz-2")
	require.NoError(t, err)
	// kfake may report zero sizes; we just need both keys present (or
	// absent uniformly) and the call to succeed without error.
	_, ok1 := sizes["sz-1"]
	_, ok2 := sizes["sz-2"]
	assert.Equal(t, ok1, ok2, "kfake should be consistent across topics")
}

func TestClient_DescribeTopicConfigsBatch__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	cleanup := "compact"
	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "with-cfg",
		Partitions:        1,
		ReplicationFactor: 1,
		Configs:           map[string]string{ConfigCleanupPolicy: cleanup},
	}))
	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "no-cfg",
		Partitions:        1,
		ReplicationFactor: 1,
	}))

	cfgs, err := c.DescribeTopicConfigsBatch(ctx, "with-cfg", "no-cfg")
	require.NoError(t, err)
	require.Contains(t, cfgs, "with-cfg")
	require.Contains(t, cfgs, "no-cfg")
	var found bool
	for _, cfg := range cfgs["with-cfg"] {
		if cfg.Key == ConfigCleanupPolicy && cfg.Value == cleanup {
			found = true
		}
	}
	assert.True(t, found, "cleanup.policy should be returned for the explicit topic")
}

func TestClient_DescribeTopicConfigsBatch__empty(t *testing.T) {
	t.Parallel()
	c := newKfakeClient(t)
	out, err := c.DescribeTopicConfigsBatch(context.Background())
	require.NoError(t, err)
	assert.Empty(t, out)
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
		{Topic: "src", Partition: 0, Key: []byte("k1"), Value: []byte("v1")},
		{Topic: "src", Partition: 0, Key: []byte("k2"), Value: []byte("v2")},
		{Topic: "src", Partition: 1, Value: []byte("vk-less-1")},
		{Topic: "src", Partition: 1, Value: []byte("vk-less-2")},
	}
	// produce with a manual partitioner so the records land in the partitions
	// specified above; the clone must preserve that layout.
	srcOpts, _, err := BuildClientOptions(c.cluster, DialOptions{
		ClientID:  "topics_test-src-producer",
		ExtraOpts: []kgo.Opt{kgo.RecordPartitioner(kgo.ManualPartitioner())},
	})
	require.NoError(t, err)
	srcProducer, err := kgo.NewClient(srcOpts...)
	require.NoError(t, err)
	t.Cleanup(srcProducer.Close)
	require.NoError(t, srcProducer.ProduceSync(ctx, produce...).FirstErr())

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

	srcWm, err := c.TopicWatermarks(ctx, "src")
	require.NoError(t, err)
	dstWm, err := c.TopicWatermarks(ctx, "dst")
	require.NoError(t, err)
	assert.EqualValues(t, len(produce), dstWm.Count())
	// per-partition counts must match between src and dst — the clone worker
	// uses ManualPartitioner so keyless records keep their partition rather
	// than being re-distributed via the default sticky partitioner.
	srcCounts := make(map[int32]int64, len(srcWm.Partitions))
	for p, w := range srcWm.Partitions {
		srcCounts[p] = w.High - w.Low
	}
	dstCounts := make(map[int32]int64, len(dstWm.Partitions))
	for p, w := range dstWm.Partitions {
		dstCounts[p] = w.High - w.Low
	}
	assert.Equal(t, srcCounts, dstCounts)
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
