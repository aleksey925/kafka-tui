package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksey925/kafka-tui/internal/config"
)

// Message is the UI-facing representation of a Kafka record. Headers are
// flattened so callers don't have to drag in kgo types.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Header is a single record header.
type Header struct {
	Key   string
	Value []byte
}

// ValueFormat is the auto-detected display format for a record value.
type ValueFormat int

const (
	ValueFormatBinary ValueFormat = iota
	ValueFormatUTF8
	ValueFormatJSON
)

// String returns the lowercase name of the format ("json", "utf8", "binary").
func (f ValueFormat) String() string {
	switch f {
	case ValueFormatJSON:
		return "json"
	case ValueFormatUTF8:
		return "utf8"
	default:
		return "binary"
	}
}

// DetectValueFormat reports the inferred display format for a record value
// in the order JSON → UTF-8 → binary used by §7.4 of the spec.
func DetectValueFormat(v []byte) ValueFormat {
	if len(v) == 0 {
		return ValueFormatUTF8
	}
	if json.Valid(v) {
		return ValueFormatJSON
	}
	if utf8.Valid(v) && !hasControlBytes(v) {
		return ValueFormatUTF8
	}
	return ValueFormatBinary
}

// hasControlBytes reports whether b contains a byte we would not want to
// render verbatim in a UTF-8 view (anything below 0x20 except whitespace).
func hasControlBytes(b []byte) bool {
	for _, c := range b {
		if c < 0x20 && c != '\t' && c != '\n' && c != '\r' {
			return true
		}
	}
	return false
}

// ParsePartitionFilter parses a list/range expression like "0-4,7,10-12" into
// a sorted, deduplicated slice of int32 partition ids. An empty input returns
// (nil, nil) which the caller should interpret as "all partitions".
func ParsePartitionFilter(s string) ([]int32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	seen := map[int32]struct{}{}
	for raw := range strings.SplitSeq(s, ",") {
		token := strings.TrimSpace(raw)
		if token == "" {
			continue
		}
		if strings.Contains(token, "-") {
			ab := strings.SplitN(token, "-", 2)
			lo, errLo := strconv.Atoi(strings.TrimSpace(ab[0]))
			hi, errHi := strconv.Atoi(strings.TrimSpace(ab[1]))
			if errLo != nil || errHi != nil || lo < 0 || hi < 0 || lo > hi || hi > maxPartition {
				return nil, fmt.Errorf("kafka: partition filter: invalid range %q", token)
			}
			for i := lo; i <= hi; i++ {
				seen[int32(i)] = struct{}{} //nolint:gosec // bounds checked above
			}
		} else {
			n, err := strconv.Atoi(token)
			if err != nil || n < 0 || n > maxPartition {
				return nil, fmt.Errorf("kafka: partition filter: invalid value %q", token)
			}
			seen[int32(n)] = struct{}{} //nolint:gosec // bounds checked above
		}
	}
	out := make([]int32, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	slices.Sort(out)
	return out, nil
}

// maxPartition is the upper bound on Kafka partition ids (the wire protocol
// uses int32). Used by ParsePartitionFilter to keep the int→int32 narrowing
// safe.
const maxPartition = (1 << 31) - 1

var relativeTimeRe = regexp.MustCompile(`^(\d+)\s*([smhd])\s+ago$`)

// ParseTimestamp accepts the formats supported by the messages-screen jump
// command:
//   - RFC 3339 / ISO 8601 ("2026-04-27T10:00:00Z", "2026-04-27 10:00:00")
//   - Relative ("1h ago", "30m ago", "2d ago", "45s ago")
//   - Date keywords ("yesterday", "today")
//
// `now` is injected so tests are deterministic.
func ParseTimestamp(s string, now time.Time) (time.Time, error) {
	original := strings.TrimSpace(s)
	lower := strings.ToLower(original)
	switch lower {
	case "":
		return time.Time{}, errors.New("empty timestamp")
	case "today":
		y, m, d := now.Date()
		return time.Date(y, m, d, 0, 0, 0, 0, now.Location()), nil
	case "yesterday":
		y, m, d := now.AddDate(0, 0, -1).Date()
		return time.Date(y, m, d, 0, 0, 0, 0, now.Location()), nil
	}
	if match := relativeTimeRe.FindStringSubmatch(lower); match != nil {
		n, _ := strconv.Atoi(match[1])
		var d time.Duration
		switch match[2] {
		case "s":
			d = time.Duration(n) * time.Second
		case "m":
			d = time.Duration(n) * time.Minute
		case "h":
			d = time.Duration(n) * time.Hour
		case "d":
			d = time.Duration(n) * 24 * time.Hour
		}
		return now.Add(-d), nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.ParseInLocation(layout, original, now.Location()); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("kafka: cannot parse timestamp %q", s)
}

// FetchLastN reads up to n most-recent messages across all partitions of a
// topic (or the subset given in `partitions`). The returned slice is sorted
// newest-first by timestamp; the count is approximate when partitions hold
// fewer messages than the requested per-partition share.
func (c *Client) FetchLastN(ctx context.Context, topic string, n int, partitions []int32) ([]Message, error) {
	if n <= 0 {
		return nil, nil
	}
	wm, err := c.TopicWatermarks(ctx, topic)
	if err != nil {
		return nil, err
	}
	parts := selectPartitions(wm, partitions)
	if len(parts) == 0 {
		return nil, nil
	}
	per := perPartitionShare(n, len(parts))
	starts, ends := map[int32]kgo.Offset{}, map[int32]int64{}
	for p, w := range parts {
		if w.High <= w.Low {
			continue
		}
		from := max(w.High-int64(per), w.Low)
		starts[p] = kgo.NewOffset().At(from)
		ends[p] = w.High
	}
	if len(starts) == 0 {
		return nil, nil
	}
	msgs, err := c.fetchUntilOffsets(ctx, topic, starts, ends)
	if err != nil {
		return nil, err
	}
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].Timestamp.After(msgs[j].Timestamp) })
	if len(msgs) > n {
		msgs = msgs[:n]
	}
	return msgs, nil
}

// FetchAtOffset reads up to count messages starting at `offset` on the given
// partition. The returned slice is in ascending offset order. Offsets outside
// the partition's [low, high) window are clamped silently.
func (c *Client) FetchAtOffset(ctx context.Context, topic string, partition int32, offset int64, count int) ([]Message, error) {
	if count <= 0 {
		return nil, nil
	}
	wm, err := c.TopicWatermarks(ctx, topic)
	if err != nil {
		return nil, err
	}
	pw, ok := wm.Partitions[partition]
	if !ok {
		return nil, fmt.Errorf("kafka: partition %d not found in %q", partition, topic)
	}
	offset = max(offset, pw.Low)
	end := min(offset+int64(count), pw.High)
	if offset >= end {
		return nil, nil
	}
	return c.fetchUntilOffsets(ctx, topic,
		map[int32]kgo.Offset{partition: kgo.NewOffset().At(offset)},
		map[int32]int64{partition: end},
	)
}

// FetchAtTimestamp reads up to `count` messages from each filtered partition
// starting at the first offset with timestamp >= ts. Partitions without any
// record at or after ts are skipped.
func (c *Client) FetchAtTimestamp(ctx context.Context, topic string, ts time.Time, partitions []int32, count int) ([]Message, error) {
	if count <= 0 {
		return nil, nil
	}
	listed, err := c.adm.ListOffsetsAfterMilli(ctx, ts.UnixMilli(), topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: list offsets after milli: %w", err)
	}
	wm, err := c.TopicWatermarks(ctx, topic)
	if err != nil {
		return nil, err
	}
	parts := selectPartitions(wm, partitions)
	starts, ends := map[int32]kgo.Offset{}, map[int32]int64{}
	for p, w := range parts {
		off := listed[topic][p]
		if off.Err != nil || off.Offset < 0 {
			continue
		}
		from := off.Offset
		end := min(from+int64(count), w.High)
		if from >= end {
			continue
		}
		starts[p] = kgo.NewOffset().At(from)
		ends[p] = end
	}
	if len(starts) == 0 {
		return nil, nil
	}
	return c.fetchUntilOffsets(ctx, topic, starts, ends)
}

// FetchEarlier loads up to `count` messages older than `baseline` across
// partitions. `baseline[p]` is the lowest offset already shown for partition p
// (exclusive upper bound for this fetch). Returned messages are in
// (partition, offset) ascending order.
func (c *Client) FetchEarlier(ctx context.Context, topic string, baseline map[int32]int64, count int, partitions []int32) ([]Message, error) {
	return c.fetchWindow(ctx, topic, baseline, count, partitions, fetchDirectionEarlier)
}

// FetchLater loads up to `count` messages newer than `baseline`. `baseline[p]`
// is the highest offset already shown for partition p (exclusive lower bound).
func (c *Client) FetchLater(ctx context.Context, topic string, baseline map[int32]int64, count int, partitions []int32) ([]Message, error) {
	return c.fetchWindow(ctx, topic, baseline, count, partitions, fetchDirectionLater)
}

type fetchDirection int

const (
	fetchDirectionEarlier fetchDirection = iota
	fetchDirectionLater
)

func (c *Client) fetchWindow(
	ctx context.Context,
	topic string,
	baseline map[int32]int64,
	count int,
	partitions []int32,
	dir fetchDirection,
) ([]Message, error) {
	if count <= 0 {
		return nil, nil
	}
	wm, err := c.TopicWatermarks(ctx, topic)
	if err != nil {
		return nil, err
	}
	parts := selectPartitions(wm, partitions)
	if len(parts) == 0 {
		return nil, nil
	}
	per := perPartitionShare(count, len(parts))
	starts, ends := map[int32]kgo.Offset{}, map[int32]int64{}
	for p, w := range parts {
		switch dir {
		case fetchDirectionEarlier:
			upper, ok := baseline[p]
			if !ok || upper > w.High {
				upper = w.High
			}
			from := max(upper-int64(per), w.Low)
			if from >= upper {
				continue
			}
			starts[p] = kgo.NewOffset().At(from)
			ends[p] = upper
		case fetchDirectionLater:
			lower, ok := baseline[p]
			if !ok || lower < w.Low-1 {
				lower = w.Low - 1
			}
			from := max(lower+1, w.Low)
			end := min(from+int64(per), w.High)
			if from >= end {
				continue
			}
			starts[p] = kgo.NewOffset().At(from)
			ends[p] = end
		}
	}
	if len(starts) == 0 {
		return nil, nil
	}
	msgs, err := c.fetchUntilOffsets(ctx, topic, starts, ends)
	if err != nil {
		return nil, err
	}
	sort.Slice(msgs, func(i, j int) bool {
		if msgs[i].Partition != msgs[j].Partition {
			return msgs[i].Partition < msgs[j].Partition
		}
		return msgs[i].Offset < msgs[j].Offset
	})
	return msgs, nil
}

// FollowSession streams new records from the end of a topic. The caller must
// invoke Close (or cancel the parent context) to release the underlying
// franz-go consumer.
type FollowSession struct {
	Messages <-chan Message
	Errors   <-chan error

	cancel context.CancelFunc
	cl     *kgo.Client
}

// Close terminates the follow session. Safe to call multiple times.
func (s *FollowSession) Close() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if s.cl != nil {
		s.cl.Close()
		s.cl = nil
	}
}

// Follow opens a consumer positioned at the end of every (filtered) partition
// of `topic` and streams subsequent records. The returned channels are closed
// when the session ends.
func (c *Client) Follow(ctx context.Context, topic string, partitions []int32) (*FollowSession, error) {
	wm, err := c.TopicWatermarks(ctx, topic)
	if err != nil {
		return nil, err
	}
	parts := selectPartitions(wm, partitions)
	if len(parts) == 0 {
		return nil, fmt.Errorf("kafka: follow: no partitions for %q", topic)
	}
	consume := map[int32]kgo.Offset{}
	for p, w := range parts {
		consume[p] = kgo.NewOffset().At(w.High)
	}
	cl, err := newConsumerClient(c.cluster, "follow", topic, consume)
	if err != nil {
		return nil, err
	}

	sCtx, cancel := context.WithCancel(ctx)
	msgCh := make(chan Message, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(msgCh)
		defer close(errCh)
		for {
			fetches := cl.PollFetches(sCtx)
			if sCtx.Err() != nil {
				return
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				if !errors.Is(errs[0].Err, context.Canceled) {
					errCh <- errs[0].Err
				}
				return
			}
			fetches.EachRecord(func(r *kgo.Record) {
				select {
				case msgCh <- recordToMessage(r):
				case <-sCtx.Done():
				}
			})
		}
	}()

	return &FollowSession{Messages: msgCh, Errors: errCh, cancel: cancel, cl: cl}, nil
}

// fetchUntilOffsets opens a transient consumer client, reads each partition
// from its starting offset until the corresponding `ends` value is reached,
// and returns the collected messages in fetch order.
func (c *Client) fetchUntilOffsets(
	ctx context.Context,
	topic string,
	starts map[int32]kgo.Offset,
	ends map[int32]int64,
) ([]Message, error) {
	cl, err := newConsumerClient(c.cluster, "fetch", topic, starts)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	progress := map[int32]int64{}
	var out []Message
	for !allReached(progress, ends) {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("kafka: fetch: %w", err)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("kafka: fetch: %w", errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			end, ok := ends[r.Partition]
			if !ok || r.Offset >= end {
				return
			}
			out = append(out, recordToMessage(r))
			progress[r.Partition] = r.Offset + 1
		})
	}
	return out, nil
}

// newConsumerClient builds a fresh kgo.Client that will only consume the
// partitions in `consume` (mapping partition → starting offset). Used both for
// bounded fetches and follow-mode subscriptions.
func newConsumerClient(cluster config.Cluster, label, topic string, consume map[int32]kgo.Offset) (*kgo.Client, error) {
	opts, _, err := BuildClientOptions(cluster, DialOptions{
		ClientID: DefaultClientID + "-" + label,
		ExtraOpts: []kgo.Opt{
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: consume}),
		},
	})
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: %s consumer: %w", label, err)
	}
	return cl, nil
}

func allReached(progress, ends map[int32]int64) bool {
	if len(ends) == 0 {
		return true
	}
	for p, end := range ends {
		if progress[p] < end {
			return false
		}
	}
	return true
}

func selectPartitions(wm TopicWatermarks, filter []int32) map[int32]PartitionWatermarks {
	if len(filter) == 0 {
		out := make(map[int32]PartitionWatermarks, len(wm.Partitions))
		maps.Copy(out, wm.Partitions)
		return out
	}
	out := map[int32]PartitionWatermarks{}
	for _, p := range filter {
		if w, ok := wm.Partitions[p]; ok {
			out[p] = w
		}
	}
	return out
}

func perPartitionShare(total, parts int) int {
	if parts <= 0 {
		return 0
	}
	return (total + parts - 1) / parts
}

func recordToMessage(r *kgo.Record) Message {
	headers := make([]Header, 0, len(r.Headers))
	for _, h := range r.Headers {
		headers = append(headers, Header{Key: h.Key, Value: append([]byte(nil), h.Value...)})
	}
	return Message{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
		Key:       append([]byte(nil), r.Key...),
		Value:     append([]byte(nil), r.Value...),
		Headers:   headers,
	}
}
