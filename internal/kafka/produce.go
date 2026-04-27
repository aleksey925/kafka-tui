package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Compression names exposed in the produce form, matching the spec wording.
type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionGzip   Compression = "gzip"
	CompressionSnappy Compression = "snappy"
	CompressionLZ4    Compression = "lz4"
	CompressionZstd   Compression = "zstd"
)

// AllCompressions lists the compression options in the order shown in the
// produce form dropdown.
var AllCompressions = []Compression{
	CompressionNone,
	CompressionGzip,
	CompressionSnappy,
	CompressionLZ4,
	CompressionZstd,
}

// PartitionAuto requests round-robin / sticky partitioner selection. Any
// non-negative value picks an explicit partition.
const PartitionAuto = int32(-1)

// ProduceSpec is the input collected by the produce form §7.5.
type ProduceSpec struct {
	Topic       string
	Partition   int32 // PartitionAuto for round-robin
	Key         []byte
	Value       []byte
	Headers     []Header
	Compression Compression
}

// ProduceResult mirrors the §7.5 toast text "Sent to <topic> P<n>:<offset>
// (<ms>ms)".
type ProduceResult struct {
	Topic     string
	Partition int32
	Offset    int64
	Duration  time.Duration
}

// Produce sends a single record using a transient producer client whose
// compression / partitioner options match the spec.
//
// We open a fresh kgo.Client each call rather than reusing the long-lived one
// for two reasons: (a) compression preference is a client-level option, not
// per-record; (b) the partitioner choice (manual vs. sticky) depends on
// whether the user picked auto or manual partition.
func (c *Client) Produce(ctx context.Context, spec ProduceSpec) (ProduceResult, error) {
	if spec.Topic == "" {
		return ProduceResult{}, errors.New("kafka: produce: topic is empty")
	}
	codec, err := compressionCodec(spec.Compression)
	if err != nil {
		return ProduceResult{}, err
	}
	extra := []kgo.Opt{kgo.ProducerBatchCompression(codec)}
	if spec.Partition >= 0 {
		extra = append(extra, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}
	opts, _, err := BuildClientOptions(c.cluster, DialOptions{
		ClientID:  DefaultClientID + "-produce",
		ExtraOpts: extra,
	})
	if err != nil {
		return ProduceResult{}, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return ProduceResult{}, fmt.Errorf("kafka: produce: %w", err)
	}
	defer cl.Close()

	headers := make([]kgo.RecordHeader, 0, len(spec.Headers))
	for _, h := range spec.Headers {
		headers = append(headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	rec := &kgo.Record{
		Topic:   spec.Topic,
		Key:     spec.Key,
		Value:   spec.Value,
		Headers: headers,
	}
	if spec.Partition >= 0 {
		rec.Partition = spec.Partition
	}

	start := time.Now()
	results := cl.ProduceSync(ctx, rec)
	elapsed := time.Since(start)
	if err := results.FirstErr(); err != nil {
		return ProduceResult{}, fmt.Errorf("kafka: produce: %w", err)
	}
	return ProduceResult{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Duration:  elapsed,
	}, nil
}

// ParseCompression validates and normalizes a user-provided compression name.
// An empty string maps to CompressionNone.
func ParseCompression(s string) (Compression, error) {
	norm := Compression(strings.ToLower(strings.TrimSpace(s)))
	if norm == "" {
		return CompressionNone, nil
	}
	if slices.Contains(AllCompressions, norm) {
		return norm, nil
	}
	return "", fmt.Errorf("kafka: unknown compression %q", s)
}

func compressionCodec(c Compression) (kgo.CompressionCodec, error) {
	switch c {
	case "", CompressionNone:
		return kgo.NoCompression(), nil
	case CompressionGzip:
		return kgo.GzipCompression(), nil
	case CompressionSnappy:
		return kgo.SnappyCompression(), nil
	case CompressionLZ4:
		return kgo.Lz4Compression(), nil
	case CompressionZstd:
		return kgo.ZstdCompression(), nil
	default:
		return kgo.CompressionCodec{}, fmt.Errorf("kafka: unknown compression %q", c)
	}
}
