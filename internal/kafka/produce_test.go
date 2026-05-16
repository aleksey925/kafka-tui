package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCompression(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in      string
		want    Compression
		wantErr bool
	}{
		{"", CompressionNone, false},
		{"none", CompressionNone, false},
		{"NONE", CompressionNone, false},
		{" gzip ", CompressionGzip, false},
		{"snappy", CompressionSnappy, false},
		{"lz4", CompressionLZ4, false},
		{"zstd", CompressionZstd, false},
		{"bz2", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := ParseCompression(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCompressionCodec_unknown__error(t *testing.T) {
	t.Parallel()
	_, err := compressionCodec(Compression("xyz"))
	require.Error(t, err)
}

func TestProduce__emptyTopic__error(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	_, err := c.Produce(ctx, ProduceSpec{})
	require.Error(t, err)
}

func TestProduce__autoPartition__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "produce-auto",
		Partitions:        2,
		ReplicationFactor: 1,
	}))

	res, err := c.Produce(ctx, ProduceSpec{
		Topic:       "produce-auto",
		Partition:   PartitionAuto,
		Key:         []byte("k"),
		Value:       []byte(`{"hello":"world"}`),
		Headers:     []Header{{Key: "x-source", Value: []byte("test")}},
		Compression: CompressionGzip,
	})
	require.NoError(t, err)
	assert.Equal(t, "produce-auto", res.Topic)
	assert.GreaterOrEqual(t, res.Partition, int32(0))
	assert.EqualValues(t, 0, res.Offset)
	assert.Greater(t, res.Duration, time.Duration(0))

	msgs, err := c.FetchLastN(ctx, "produce-auto", 1, nil)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.JSONEq(t, `{"hello":"world"}`, string(msgs[0].Value))
	assert.Equal(t, []byte("k"), msgs[0].Key)
	require.Len(t, msgs[0].Headers, 1)
	assert.Equal(t, "x-source", msgs[0].Headers[0].Key)
	assert.Equal(t, []byte("test"), msgs[0].Headers[0].Value)
}

func TestProduce__manualPartition__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "produce-manual",
		Partitions:        4,
		ReplicationFactor: 1,
	}))

	for _, p := range []int32{0, 2, 3} {
		_, err := c.Produce(ctx, ProduceSpec{
			Topic:       "produce-manual",
			Partition:   p,
			Value:       []byte("on-" + string('0'+p)),
			Compression: CompressionNone,
		})
		require.NoError(t, err)
	}

	all, err := c.FetchLastN(ctx, "produce-manual", 10, nil)
	require.NoError(t, err)
	bypart := map[int32][]byte{}
	for _, m := range all {
		bypart[m.Partition] = m.Value
	}
	assert.Equal(t, []byte("on-0"), bypart[0])
	assert.Equal(t, []byte("on-2"), bypart[2])
	assert.Equal(t, []byte("on-3"), bypart[3])
	_, present := bypart[1]
	assert.False(t, present, "partition 1 should be empty")
}

func TestProduce__compressionCodecs__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "produce-codecs",
		Partitions:        1,
		ReplicationFactor: 1,
	}))

	for _, codec := range []Compression{CompressionNone, CompressionGzip, CompressionSnappy, CompressionLZ4, CompressionZstd} {
		_, err := c.Produce(ctx, ProduceSpec{
			Topic:       "produce-codecs",
			Partition:   PartitionAuto,
			Value:       []byte(string(codec) + "-payload"),
			Compression: codec,
		})
		require.NoError(t, err, "codec %s", codec)
	}
}
