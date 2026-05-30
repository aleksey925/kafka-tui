package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestReadOnly_BlocksMutations(t *testing.T) {
	t.Parallel()

	c := newKfakeReadOnlyClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	cases := []struct {
		name string
		run  func() error
	}{
		{"Produce", func() error {
			_, err := c.Produce(ctx, ProduceSpec{Topic: "t", Value: []byte("v")})
			return err
		}},
		{"CreateTopic", func() error {
			return c.CreateTopic(ctx, CreateTopicSpec{Name: "t", Partitions: 1, ReplicationFactor: 1})
		}},
		{"DeleteTopic", func() error {
			return c.DeleteTopic(ctx, "t")
		}},
		{"AlterTopicConfig", func() error {
			return c.AlterTopicConfig(ctx, "t", "retention.ms", "1000")
		}},
		{"CloneTopic", func() error {
			_, err := c.CloneTopic(ctx, "src", "dst", CloneOptions{})
			return err
		}},
		{"ResetOffsets", func() error {
			_, err := c.ResetOffsets(ctx, "g", ResetSpec{
				Strategy: ResetEarliest,
				Targets:  []TopicPartition{{Topic: "t", Partition: 0}},
			})
			return err
		}},
		{"DeleteConsumerGroup", func() error {
			return c.DeleteConsumerGroup(ctx, "g")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.ErrorIs(t, tc.run(), ErrReadOnly)
		})
	}
}

func TestReadOnly_ReadsStillWork(t *testing.T) {
	t.Parallel()

	c := newKfakeReadOnlyClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	_, err := c.ListTopics(ctx)
	assert.NoError(t, err)
}

func newKfakeReadOnlyClient(t *testing.T) *Client {
	t.Helper()
	cluster := startKfake(t)
	cluster.ReadOnly = true
	c, err := Dial(cluster, DialOptions{
		ExtraOpts: []kgo.Opt{kgo.MetadataMinAge(10 * time.Millisecond)},
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}
