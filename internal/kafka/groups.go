package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// GroupListInfo is the lightweight per-group snapshot used by the groups list
// screen — the data ListGroups returns without an extra DescribeGroups round
// trip.
type GroupListInfo struct {
	Group        string
	State        string
	ProtocolType string
	Coordinator  int32
}

// GroupMember is a single consumer in a described group.
type GroupMember struct {
	MemberID    string
	InstanceID  string
	ClientID    string
	ClientHost  string
	Topics      []string
	Assignments []MemberAssignment
}

// MemberAssignment is the (topic, partitions) pair owned by a single member.
type MemberAssignment struct {
	Topic      string
	Partitions []int32
}

// GroupDescription is the detailed view §7.7 of the spec consumes — members,
// coordinator, protocol, and state.
type GroupDescription struct {
	Group           string
	State           string
	ProtocolType    string
	Protocol        string
	CoordinatorID   int32
	CoordinatorHost string
	CoordinatorPort int32
	Members         []GroupMember
}

// PartitionLag mirrors the columns the group-detail screen renders: per
// partition committed/end/lag with the owning member's id.
type PartitionLag struct {
	Topic     string
	Partition int32
	Committed int64 // -1 when no commit has been recorded yet
	End       int64
	Lag       int64 // -1 when commit/end could not be loaded
	MemberID  string
	Err       error
}

// ResetStrategy is one of the offset-reset strategies the §7.8 dialog exposes.
type ResetStrategy int

const (
	// ResetEarliest moves every targeted partition to its log-start offset.
	ResetEarliest ResetStrategy = iota
	// ResetLatest moves every targeted partition to its log-end offset.
	ResetLatest
	// ResetShift adds Shift to the current commit (negative shifts move
	// backwards). Out-of-range results are clamped to [low, high].
	ResetShift
	// ResetTimestamp seeks the first record with timestamp >= Timestamp.
	// Partitions with no record at/after the timestamp fall back to the
	// log-end offset (with a "→ high" note in the preview).
	ResetTimestamp
	// ResetSpecific sets every targeted partition to Offset, clamped to
	// [low, high].
	ResetSpecific
)

// String returns the strategy name used in toast messages and the spec.
func (s ResetStrategy) String() string {
	switch s {
	case ResetEarliest:
		return "earliest"
	case ResetLatest:
		return "latest"
	case ResetShift:
		return "shift"
	case ResetTimestamp:
		return "timestamp"
	case ResetSpecific:
		return "specific"
	default:
		return "unknown"
	}
}

// TopicPartition is a (topic, partition) pair used for reset scopes.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// ResetSpec describes a single reset operation.
type ResetSpec struct {
	// Strategy selects the algorithm used to compute the new offset.
	Strategy ResetStrategy
	// Shift is the delta applied to the current commit when Strategy is
	// ResetShift. Negative values move backwards.
	Shift int64
	// Timestamp is the time used by ResetTimestamp.
	Timestamp time.Time
	// Offset is the absolute offset used by ResetSpecific.
	Offset int64
	// Targets restricts the reset to these (topic, partition) pairs. When
	// empty the reset applies to every partition the group has committed
	// offsets for.
	Targets []TopicPartition
}

// PartitionResetPreview is the per-partition row of the §7.8 preview table.
type PartitionResetPreview struct {
	Topic     string
	Partition int32
	Committed int64 // current committed offset, -1 if none
	Low       int64
	High      int64
	Target    int64  // post-clamp target offset
	Diff      int64  // Target - Committed (or Target when no commit)
	Note      string // see resetNote* constants
}

const (
	// ResetNoteClampedLow indicates the requested offset (shift/specific)
	// was below the partition's low watermark and was clamped up.
	ResetNoteClampedLow = "clamped to low"
	// ResetNoteClampedHigh indicates the requested offset was above the
	// partition's high watermark and was clamped down.
	ResetNoteClampedHigh = "clamped to high"
	// ResetNoteTimestampNoBefore indicates no record predates the requested
	// timestamp; the partition was sent to its low watermark.
	ResetNoteTimestampNoBefore = "→ low (no msgs before)"
	// ResetNoteTimestampNoAfter indicates no record matches/follows the
	// requested timestamp; the partition was sent to its high watermark.
	ResetNoteTimestampNoAfter = "→ high (no msgs after)"
)

// ResetSummary aggregates re-consume / skip estimates for the preview footer.
type ResetSummary struct {
	// Reconsume is the number of records the group will re-process (sum of
	// negative diffs, magnitude). Populated for earliest/shift/timestamp.
	Reconsume int64
	// Skipped is the number of records the group will skip past (sum of
	// positive diffs). Populated for latest/shift.
	Skipped int64
}

// ResetPreview is the §7.8 preview returned by PreviewReset. The same shape
// is returned by ResetOffsets so the UI can render the post-commit table.
type ResetPreview struct {
	Group      string
	Strategy   ResetStrategy
	Partitions []PartitionResetPreview
	Summary    ResetSummary
}

// ListConsumerGroups returns a cheap snapshot of all consumer-protocol groups
// in the cluster. The list view uses this; lag is loaded lazily on focus.
func (c *Client) ListConsumerGroups(ctx context.Context) ([]GroupListInfo, error) {
	listed, err := c.adm.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: list groups: %w", err)
	}
	out := make([]GroupListInfo, 0, len(listed))
	for _, g := range listed {
		if g.ProtocolType != "" && g.ProtocolType != "consumer" {
			continue
		}
		out = append(out, GroupListInfo{
			Group:        g.Group,
			State:        g.State,
			ProtocolType: g.ProtocolType,
			Coordinator:  g.Coordinator,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out, nil
}

// DescribeConsumerGroup returns members, coordinator, and protocol info for a
// single group.
func (c *Client) DescribeConsumerGroup(ctx context.Context, group string) (GroupDescription, error) {
	described, err := c.adm.DescribeGroups(ctx, group)
	if err != nil {
		return GroupDescription{}, fmt.Errorf("kafka: describe group %q: %w", group, err)
	}
	d, ok := described[group]
	if !ok {
		return GroupDescription{}, fmt.Errorf("kafka: group %q not found", group)
	}
	if d.Err != nil {
		return GroupDescription{}, fmt.Errorf("kafka: describe group %q: %w", group, d.Err)
	}
	return groupDescriptionFromKadm(d), nil
}

func groupDescriptionFromKadm(d kadm.DescribedGroup) GroupDescription {
	out := GroupDescription{
		Group:           d.Group,
		State:           d.State,
		ProtocolType:    d.ProtocolType,
		Protocol:        d.Protocol,
		CoordinatorID:   d.Coordinator.NodeID,
		CoordinatorHost: d.Coordinator.Host,
		CoordinatorPort: d.Coordinator.Port,
		Members:         make([]GroupMember, 0, len(d.Members)),
	}
	for _, m := range d.Members {
		mem := GroupMember{
			MemberID:   m.MemberID,
			ClientID:   m.ClientID,
			ClientHost: m.ClientHost,
		}
		if m.InstanceID != nil {
			mem.InstanceID = *m.InstanceID
		}
		if join, ok := m.Join.AsConsumer(); ok {
			mem.Topics = append(mem.Topics, join.Topics...)
		}
		if assigned, ok := m.Assigned.AsConsumer(); ok {
			for _, t := range assigned.Topics {
				parts := append([]int32(nil), t.Partitions...)
				slices.Sort(parts)
				mem.Assignments = append(mem.Assignments, MemberAssignment{
					Topic:      t.Topic,
					Partitions: parts,
				})
			}
			sort.Slice(mem.Assignments, func(i, j int) bool {
				return mem.Assignments[i].Topic < mem.Assignments[j].Topic
			})
		}
		sort.Strings(mem.Topics)
		out.Members = append(out.Members, mem)
	}
	sort.Slice(out.Members, func(i, j int) bool {
		a, b := out.Members[i], out.Members[j]
		if a.InstanceID != b.InstanceID {
			return a.InstanceID < b.InstanceID
		}
		return a.MemberID < b.MemberID
	})
	return out
}

// GroupOffsets returns the per-partition committed/end/lag snapshot for a
// group. Lag of -1 indicates the partition was assigned but its end offset
// could not be loaded.
func (c *Client) GroupOffsets(ctx context.Context, group string) ([]PartitionLag, error) {
	lags, err := c.adm.Lag(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("kafka: lag for %q: %w", group, err)
	}
	gl, ok := lags[group]
	if !ok {
		return nil, fmt.Errorf("kafka: group %q not found", group)
	}
	if err := gl.Error(); err != nil {
		return nil, fmt.Errorf("kafka: lag for %q: %w", group, err)
	}

	memberByPartition := map[TopicPartition]string{}
	for i := range gl.Members {
		m := &gl.Members[i]
		if assigned, ok := m.Assigned.AsConsumer(); ok {
			for _, t := range assigned.Topics {
				for _, p := range t.Partitions {
					memberByPartition[TopicPartition{Topic: t.Topic, Partition: p}] = m.MemberID
				}
			}
		}
	}

	sorted := gl.Lag.Sorted()
	out := make([]PartitionLag, 0, len(sorted))
	for _, ml := range sorted {
		row := PartitionLag{
			Topic:     ml.Topic,
			Partition: ml.Partition,
			Committed: ml.Commit.At,
			End:       ml.End.Offset,
			Lag:       ml.Lag,
			MemberID:  memberByPartition[TopicPartition{Topic: ml.Topic, Partition: ml.Partition}],
			Err:       ml.Err,
		}
		if ml.End.Err != nil {
			row.End = -1
		}
		out = append(out, row)
	}
	return out, nil
}

// FilterGroupsByTopic returns the groups that have committed offsets for the
// given topic OR have members subscribed to it. Used by `g` on the topics
// screen to scope the groups list to a topic.
func (c *Client) FilterGroupsByTopic(ctx context.Context, topic string) ([]GroupListInfo, error) {
	groups, err := c.ListConsumerGroups(ctx)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, nil
	}
	names := make([]string, len(groups))
	for i, g := range groups {
		names[i] = g.Group
	}

	described, err := c.adm.DescribeGroups(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe groups: %w", err)
	}
	commits := c.adm.FetchManyOffsets(ctx, names...)

	keep := make([]GroupListInfo, 0, len(groups))
	for _, g := range groups {
		if matchesTopic(g.Group, topic, described, commits) {
			keep = append(keep, g)
		}
	}
	return keep, nil
}

func matchesTopic(group, topic string, described kadm.DescribedGroups, commits kadm.FetchOffsetsResponses) bool {
	if d, ok := described[group]; ok {
		if slices.Contains(d.JoinTopics(), topic) {
			return true
		}
		if assigned, ok := d.AssignedPartitions()[topic]; ok && len(assigned) > 0 {
			return true
		}
	}
	if r, ok := commits[group]; ok && r.Err == nil {
		if _, hasTopic := r.Fetched[topic]; hasTopic {
			return true
		}
	}
	return false
}

// PreviewReset computes the per-partition diff implied by spec without
// committing anything. Use this to drive the §7.8 preview table.
func (c *Client) PreviewReset(ctx context.Context, group string, spec ResetSpec) (ResetPreview, error) {
	return c.computeReset(ctx, group, spec)
}

// ResetOffsets commits the offsets implied by spec and returns the same
// preview shape so the UI can render the post-commit confirmation.
func (c *Client) ResetOffsets(ctx context.Context, group string, spec ResetSpec) (ResetPreview, error) {
	preview, err := c.computeReset(ctx, group, spec)
	if err != nil {
		return ResetPreview{}, err
	}
	if len(preview.Partitions) == 0 {
		return preview, nil
	}
	commits := make(kadm.Offsets)
	for _, p := range preview.Partitions {
		commits.Add(kadm.Offset{
			Topic:       p.Topic,
			Partition:   p.Partition,
			At:          p.Target,
			LeaderEpoch: -1,
		})
	}
	if err := c.adm.CommitAllOffsets(ctx, group, commits); err != nil {
		return ResetPreview{}, fmt.Errorf("kafka: commit offsets for %q: %w", group, err)
	}
	return preview, nil
}

// DeleteConsumerGroup deletes a group, but only when it is in the Empty state
// (KIP-229 requires no active members). Returns a structured error otherwise.
func (c *Client) DeleteConsumerGroup(ctx context.Context, group string) error {
	described, err := c.adm.DescribeGroups(ctx, group)
	if err != nil {
		return fmt.Errorf("kafka: describe group %q: %w", group, err)
	}
	d, ok := described[group]
	if !ok {
		return fmt.Errorf("kafka: group %q not found", group)
	}
	if d.Err != nil {
		return fmt.Errorf("kafka: describe group %q: %w", group, d.Err)
	}
	if d.State != "" && d.State != "Empty" && d.State != "Dead" {
		return fmt.Errorf("kafka: refusing to delete group %q in state %q: %w", group, d.State, ErrNonEmptyGroup)
	}
	resp, err := c.adm.DeleteGroup(ctx, group)
	if err != nil {
		return fmt.Errorf("kafka: delete group %q: %w", group, err)
	}
	if resp.Err != nil {
		return fmt.Errorf("kafka: delete group %q: %w", group, resp.Err)
	}
	return nil
}

// computeReset is the shared body of PreviewReset and ResetOffsets — it
// resolves the targets, loads watermarks/commits, and applies the strategy
// with clamping.
func (c *Client) computeReset(ctx context.Context, group string, spec ResetSpec) (ResetPreview, error) {
	if spec.Strategy < ResetEarliest || spec.Strategy > ResetSpecific {
		return ResetPreview{}, fmt.Errorf("kafka: unknown reset strategy %d", spec.Strategy)
	}

	described, err := c.adm.DescribeGroups(ctx, group)
	if err != nil {
		return ResetPreview{}, fmt.Errorf("kafka: describe group %q: %w", group, err)
	}
	d, ok := described[group]
	if !ok {
		return ResetPreview{}, fmt.Errorf("kafka: group %q not found", group)
	}
	if d.Err != nil {
		return ResetPreview{}, fmt.Errorf("kafka: describe group %q: %w", group, d.Err)
	}
	if d.State != "" && d.State != "Empty" && d.State != "Dead" {
		return ResetPreview{}, fmt.Errorf("kafka: refusing to reset offsets for %q in state %q: %w", group, d.State, ErrNonEmptyGroup)
	}

	commits, err := c.adm.FetchOffsets(ctx, group)
	if err != nil {
		return ResetPreview{}, fmt.Errorf("kafka: fetch offsets for %q: %w", group, err)
	}

	targets := resolveResetTargets(spec.Targets, commits)
	if len(targets) == 0 {
		return ResetPreview{Group: group, Strategy: spec.Strategy}, nil
	}

	topics := uniqueTopicsFromTargets(targets)
	startOffsets, err := c.adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return ResetPreview{}, fmt.Errorf("kafka: list start offsets: %w", err)
	}
	endOffsets, err := c.adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return ResetPreview{}, fmt.Errorf("kafka: list end offsets: %w", err)
	}

	var atTimestamp kadm.ListedOffsets
	if spec.Strategy == ResetTimestamp {
		atTimestamp, err = c.adm.ListOffsetsAfterMilli(ctx, spec.Timestamp.UnixMilli(), topics...)
		if err != nil {
			return ResetPreview{}, fmt.Errorf("kafka: list offsets at timestamp: %w", err)
		}
	}

	preview := ResetPreview{Group: group, Strategy: spec.Strategy, Partitions: make([]PartitionResetPreview, 0, len(targets))}
	for _, tp := range targets {
		row, err := buildResetRow(tp, spec, commits, startOffsets, endOffsets, atTimestamp)
		if err != nil {
			return ResetPreview{}, err
		}
		preview.Partitions = append(preview.Partitions, row)
		switch {
		case row.Diff > 0:
			preview.Summary.Skipped += row.Diff
		case row.Diff < 0:
			preview.Summary.Reconsume += -row.Diff
		}
	}
	sort.Slice(preview.Partitions, func(i, j int) bool {
		a, b := preview.Partitions[i], preview.Partitions[j]
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.Partition < b.Partition
	})
	return preview, nil
}

// resolveResetTargets returns either the explicit targets, deduplicated, or —
// if targets is empty — every (topic, partition) the group has a commit for.
func resolveResetTargets(targets []TopicPartition, commits kadm.OffsetResponses) []TopicPartition {
	if len(targets) > 0 {
		seen := make(map[TopicPartition]struct{}, len(targets))
		out := make([]TopicPartition, 0, len(targets))
		for _, tp := range targets {
			if _, dup := seen[tp]; dup {
				continue
			}
			seen[tp] = struct{}{}
			out = append(out, tp)
		}
		return out
	}
	var out []TopicPartition
	for t, ps := range commits {
		for p := range ps {
			out = append(out, TopicPartition{Topic: t, Partition: p})
		}
	}
	return out
}

func uniqueTopicsFromTargets(targets []TopicPartition) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, tp := range targets {
		if _, ok := seen[tp.Topic]; ok {
			continue
		}
		seen[tp.Topic] = struct{}{}
		out = append(out, tp.Topic)
	}
	sort.Strings(out)
	return out
}

func buildResetRow(
	tp TopicPartition,
	spec ResetSpec,
	commits kadm.OffsetResponses,
	startOffsets, endOffsets, atTimestamp kadm.ListedOffsets,
) (PartitionResetPreview, error) {
	low, high, err := watermarksFor(tp, startOffsets, endOffsets)
	if err != nil {
		return PartitionResetPreview{}, err
	}
	committed := int64(-1)
	if c, ok := commits.Lookup(tp.Topic, tp.Partition); ok && c.Err == nil {
		committed = c.At
	}

	target, note, err := computeResetTarget(tp, spec, committed, low, high, atTimestamp)
	if err != nil {
		return PartitionResetPreview{}, err
	}

	row := PartitionResetPreview{
		Topic:     tp.Topic,
		Partition: tp.Partition,
		Committed: committed,
		Low:       low,
		High:      high,
		Target:    target,
		Note:      note,
	}
	if committed >= 0 {
		row.Diff = target - committed
	} else {
		row.Diff = target
	}
	return row, nil
}

func watermarksFor(tp TopicPartition, startOffsets, endOffsets kadm.ListedOffsets) (int64, int64, error) {
	low, lowOK := lookupListedOffset(startOffsets, tp)
	high, highOK := lookupListedOffset(endOffsets, tp)
	if !lowOK || !highOK {
		return 0, 0, fmt.Errorf("kafka: missing watermarks for %s/%d", tp.Topic, tp.Partition)
	}
	return low, high, nil
}

func lookupListedOffset(offsets kadm.ListedOffsets, tp TopicPartition) (int64, bool) {
	ps, ok := offsets[tp.Topic]
	if !ok {
		return 0, false
	}
	o, ok := ps[tp.Partition]
	if !ok {
		return 0, false
	}
	if o.Err != nil {
		return 0, false
	}
	return o.Offset, true
}

func computeResetTarget(
	tp TopicPartition,
	spec ResetSpec,
	committed, low, high int64,
	atTimestamp kadm.ListedOffsets,
) (int64, string, error) {
	switch spec.Strategy {
	case ResetEarliest:
		return low, "", nil
	case ResetLatest:
		return high, "", nil
	case ResetShift:
		base := committed
		if base < 0 {
			base = low
		}
		target, note := clampOffset(base+spec.Shift, low, high)
		return target, note, nil
	case ResetSpecific:
		target, note := clampOffset(spec.Offset, low, high)
		return target, note, nil
	case ResetTimestamp:
		if offset, ok := lookupListedOffsetAt(atTimestamp, tp); ok {
			return offset, "", nil
		}
		if high == low {
			return low, ResetNoteTimestampNoBefore, nil
		}
		return high, ResetNoteTimestampNoAfter, nil
	default:
		return 0, "", fmt.Errorf("kafka: unsupported strategy %v", spec.Strategy)
	}
}

// lookupListedOffsetAt returns the offset for tp in offsets, but only when the
// broker reported a real (non-negative) offset for it. A -1 offset means
// "no record at-or-after the requested timestamp" and is treated as a miss so
// the caller can apply the empty-partition fallback.
func lookupListedOffsetAt(offsets kadm.ListedOffsets, tp TopicPartition) (int64, bool) {
	ps, ok := offsets[tp.Topic]
	if !ok {
		return 0, false
	}
	o, ok := ps[tp.Partition]
	if !ok || o.Err != nil || o.Offset < 0 {
		return 0, false
	}
	return o.Offset, true
}

func clampOffset(want, low, high int64) (int64, string) {
	switch {
	case want < low:
		return low, ResetNoteClampedLow
	case want > high:
		return high, ResetNoteClampedHigh
	default:
		return want, ""
	}
}

// ErrNonEmptyGroup is returned when DeleteConsumerGroup or ResetOffsets is
// called on a group that still has active members. The TUI maps this to a
// dedicated toast.
var ErrNonEmptyGroup = errors.New("group is not empty")

// IsNonEmptyGroup reports whether err signals the "group must be Empty"
// precondition surfaced by ResetOffsets and DeleteConsumerGroup. Either our
// own preflight check or the broker's NON_EMPTY_GROUP error qualifies.
func IsNonEmptyGroup(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNonEmptyGroup) {
		return true
	}
	return errors.Is(err, kerr.NonEmptyGroup)
}
