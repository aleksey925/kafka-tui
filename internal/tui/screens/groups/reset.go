package groups

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ResetScope is the user's choice for which (topic, partition) pairs the
// reset will touch. Implementations are immutable values that the reset model
// inspects when building the [kafka.ResetSpec].
type ResetScope interface {
	// Targets returns the explicit (topic, partition) restriction. Returning
	// nil/empty defers to the kafka.Client's "every partition the group has
	// commits for" fallback.
	Targets() []kafka.TopicPartition
	// HeaderLabel returns the §7.8 adaptive header text describing the scope.
	HeaderLabel(partitionCount, topicCount int) string
}

// ScopeWholeGroup means "every partition the group has committed offsets
// for". When Topic is non-empty, the scope is restricted to that topic
// (the filtered-list entry point).
type ScopeWholeGroup struct {
	Group string
	Topic string // empty = unrestricted
}

// Targets restricts the operation to the specific topic when one is set;
// otherwise it returns nil so the kafka.Client fallback applies.
func (s ScopeWholeGroup) Targets() []kafka.TopicPartition { return nil }

// HeaderLabel renders the §7.8 adaptive header for a whole-group scope.
func (s ScopeWholeGroup) HeaderLabel(partitionCount, topicCount int) string {
	if s.Topic != "" {
		return fmt.Sprintf("Resetting %d partitions in %s", partitionCount, s.Topic)
	}
	if topicCount == 1 {
		return fmt.Sprintf("Resetting %d partitions across 1 topic", partitionCount)
	}
	if topicCount > 1 {
		return fmt.Sprintf("Resetting %d partitions across %d topics", partitionCount, topicCount)
	}
	return fmt.Sprintf("Resetting all partitions (%d total)", partitionCount)
}

// ScopeDetail names the same default as ScopeWholeGroup for the "R from
// detail view" path; it's a distinct type so callers can distinguish flow
// origins later if needed.
type ScopeDetail struct{ Group string }

// Targets returns nil so the kafka.Client falls back to "all committed
// partitions" — the detail view always operates on the whole group.
func (s ScopeDetail) Targets() []kafka.TopicPartition { return nil }

// HeaderLabel renders the §7.8 adaptive header for a detail-view scope.
func (s ScopeDetail) HeaderLabel(partitionCount, topicCount int) string {
	if topicCount == 1 {
		return fmt.Sprintf("Resetting %d partitions across 1 topic", partitionCount)
	}
	if topicCount > 1 {
		return fmt.Sprintf("Resetting %d partitions across %d topics", partitionCount, topicCount)
	}
	return fmt.Sprintf("Resetting all partitions (%d total)", partitionCount)
}

// ResetStep is the current step of the §7.8 4-step flow.
type ResetStep int

const (
	// StepStrategy lets the user pick earliest/latest/shift/timestamp/specific.
	StepStrategy ResetStep = iota
	// StepParams collects shift / timestamp / specific input (skipped for
	// earliest and latest).
	StepParams
	// StepPreview shows the diff table and waits for y to commit. Skipped in
	// express mode.
	StepPreview
	// StepDone marks the flow as complete (committed or canceled).
	StepDone
)

// ResetAction is the host-facing intent of the reset model.
type ResetAction struct {
	// Cancel is set when the user pressed esc before committing.
	Cancel bool
	// Done is set after a successful commit (Result holds the post-commit
	// preview) OR after a cancel (Result is nil).
	Done   bool
	Result *kafka.ResetPreview
}

// ResetOptions configure a [ResetModel].
type ResetOptions struct {
	Service Service
	Group   string
	Scope   ResetScope
	// Express skips the preview step — params → commit.
	Express bool
	Now     func() time.Time
	Styles  theme.Styles
}

// ResetModel hosts the 4-step reset flow. The model is *not* read-only-aware
// itself: callers gate the flow behind the ReadOnly check.
type ResetModel struct {
	svc     Service
	group   string
	scope   ResetScope
	express bool

	step     ResetStep
	strategy kafka.ResetStrategy
	form     *components.Form
	preview  kafka.ResetPreview
	result   *kafka.ResetPreview

	committing bool
	previewing bool
	err        string

	width, height int
	action        ResetAction
	now           func() time.Time
	styles        theme.Styles
}

// Reset-form field keys.
const (
	resetFieldShift     = "shift"
	resetFieldTimestamp = "timestamp"
	resetFieldOffset    = "offset"
)

// NewResetModel constructs a fresh reset model.
func NewResetModel(opts ResetOptions) *ResetModel {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	return &ResetModel{
		svc:      opts.Service,
		group:    opts.Group,
		scope:    opts.Scope,
		express:  opts.Express,
		step:     StepStrategy,
		strategy: kafka.ResetEarliest,
		now:      now,
		styles:   styles,
	}
}

// Init returns no command — the reset model is purely interactive until the
// user picks a strategy.
func (r *ResetModel) Init() tea.Cmd { return nil }

// Group returns the group name this reset is bound to.
func (r *ResetModel) Group() string { return r.group }

// Scope returns the active scope (for tests).
func (r *ResetModel) Scope() ResetScope { return r.scope }

// Step returns the current flow step (for tests).
func (r *ResetModel) Step() ResetStep { return r.step }

// Strategy returns the currently selected strategy (for tests).
func (r *ResetModel) Strategy() kafka.ResetStrategy { return r.strategy }

// Express reports whether this is an express-mode flow.
func (r *ResetModel) Express() bool { return r.express }

// Preview returns the current preview snapshot (for tests).
func (r *ResetModel) Preview() kafka.ResetPreview { return r.preview }

// Action returns the current pending action.
func (r *ResetModel) Action() ResetAction { return r.action }

// ConsumeAction returns the pending action and clears it.
func (r *ResetModel) ConsumeAction() ResetAction {
	a := r.action
	r.action = ResetAction{}
	return a
}

// Err returns the latest validation/commit error (or empty).
func (r *ResetModel) Err() string { return r.err }

// SetSize updates width/height.
func (r *ResetModel) SetSize(w, h int) { r.width, r.height = w, h }

// KeyHints returns the screen-specific hints.
func (r *ResetModel) KeyHints() []layout.KeyHint {
	switch r.step {
	case StepStrategy:
		return []layout.KeyHint{
			{Key: "↑/↓", Label: "select"},
			{Key: "enter", Label: "next"},
			{Key: "esc", Label: "cancel"},
		}
	case StepParams:
		return []layout.KeyHint{
			{Key: "tab", Label: "next field"},
			{Key: "enter", Label: "next"},
			{Key: "esc", Label: "cancel"},
		}
	case StepPreview:
		return []layout.KeyHint{
			{Key: "y", Label: "commit"},
			{Key: "n/esc", Label: "cancel"},
		}
	case StepDone:
		return nil
	}
	return nil
}

// Update routes a message into the reset model.
func (r *ResetModel) Update(msg tea.Msg) (*ResetModel, tea.Cmd) {
	switch msg := msg.(type) {
	case ResetPreviewMsg:
		r.handlePreview(msg)
		return r, nil
	case ResetCommittedMsg:
		r.handleCommitted(msg)
		return r, nil
	case tea.KeyPressMsg:
		return r.handleKey(msg)
	}
	return r, nil
}

func (r *ResetModel) handleKey(key tea.KeyPressMsg) (*ResetModel, tea.Cmd) {
	if key.String() == "esc" {
		r.action.Cancel = true
		return r, nil
	}
	switch r.step {
	case StepStrategy:
		return r.handleStrategyKey(key)
	case StepParams:
		return r.handleParamsKey(key)
	case StepPreview:
		return r.handlePreviewKey(key)
	case StepDone:
		return r, nil
	}
	return r, nil
}

func (r *ResetModel) handleStrategyKey(key tea.KeyPressMsg) (*ResetModel, tea.Cmd) {
	strategies := []kafka.ResetStrategy{
		kafka.ResetEarliest,
		kafka.ResetLatest,
		kafka.ResetShift,
		kafka.ResetTimestamp,
		kafka.ResetSpecific,
	}
	idx := 0
	for i, s := range strategies {
		if s == r.strategy {
			idx = i
			break
		}
	}
	switch key.String() {
	case "j", "down":
		idx = (idx + 1) % len(strategies)
		r.strategy = strategies[idx]
	case "k", "up":
		idx = (idx - 1 + len(strategies)) % len(strategies)
		r.strategy = strategies[idx]
	case "enter":
		return r.advanceFromStrategy()
	}
	return r, nil
}

// advanceFromStrategy moves out of the strategy step. Earliest/Latest skip the
// params step; everything else opens the params form.
func (r *ResetModel) advanceFromStrategy() (*ResetModel, tea.Cmd) {
	switch r.strategy {
	case kafka.ResetEarliest, kafka.ResetLatest:
		return r.dispatchAfterParams()
	case kafka.ResetShift, kafka.ResetTimestamp, kafka.ResetSpecific:
		// fall through to the params form below.
	}
	r.form = r.buildParamsForm()
	r.step = StepParams
	return r, nil
}

func (r *ResetModel) buildParamsForm() *components.Form {
	switch r.strategy {
	case kafka.ResetShift:
		return components.NewForm([]components.Field{
			{Key: resetFieldShift, Label: "Shift (positive forward / negative back)", Kind: components.FieldText, Value: "0"},
		}, components.WithFormStyles(r.styles))
	case kafka.ResetTimestamp:
		return components.NewForm([]components.Field{
			{Key: resetFieldTimestamp, Label: "Timestamp (RFC3339, e.g. 2026-04-28T10:00:00Z)", Kind: components.FieldText, Value: r.now().UTC().Format(time.RFC3339)},
		}, components.WithFormStyles(r.styles))
	case kafka.ResetEarliest, kafka.ResetLatest:
		return nil
	case kafka.ResetSpecific:
		return components.NewForm([]components.Field{
			{Key: resetFieldOffset, Label: "Offset", Kind: components.FieldText, Value: "0"},
		}, components.WithFormStyles(r.styles))
	}
	return nil
}

func (r *ResetModel) handleParamsKey(key tea.KeyPressMsg) (*ResetModel, tea.Cmd) {
	if key.String() == "enter" {
		return r.dispatchAfterParams()
	}
	if r.form != nil {
		f, _ := r.form.Update(key)
		r.form = f
	}
	return r, nil
}

// dispatchAfterParams validates the params and either dispatches the preview
// (normal flow) or directly commits (express flow).
func (r *ResetModel) dispatchAfterParams() (*ResetModel, tea.Cmd) {
	spec, err := r.spec()
	if err != nil {
		r.err = err.Error()
		return r, nil
	}
	r.err = ""
	if r.express {
		r.committing = true
		r.step = StepPreview // preview rendered post-commit
		return r, commitCmd(r.svc, r.group, spec)
	}
	r.previewing = true
	r.step = StepPreview
	return r, previewCmd(r.svc, r.group, spec)
}

func (r *ResetModel) handlePreviewKey(key tea.KeyPressMsg) (*ResetModel, tea.Cmd) {
	switch strings.ToLower(key.String()) {
	case "y":
		spec, err := r.spec()
		if err != nil {
			r.err = err.Error()
			return r, nil
		}
		r.committing = true
		return r, commitCmd(r.svc, r.group, spec)
	case "n":
		r.action.Cancel = true
		return r, nil
	}
	return r, nil
}

func (r *ResetModel) handlePreview(msg ResetPreviewMsg) {
	r.previewing = false
	if msg.Err != nil {
		if kafka.IsNonEmptyGroup(msg.Err) {
			r.err = "group is not empty — stop active consumers first"
			return
		}
		r.err = msg.Err.Error()
		return
	}
	r.err = ""
	r.preview = msg.Preview
}

func (r *ResetModel) handleCommitted(msg ResetCommittedMsg) {
	r.committing = false
	if msg.Err != nil {
		if kafka.IsNonEmptyGroup(msg.Err) {
			r.err = "group is not empty — stop active consumers first"
			return
		}
		r.err = msg.Err.Error()
		return
	}
	res := msg.Result
	r.result = &res
	r.preview = res
	r.action.Done = true
	r.action.Result = &res
	r.step = StepDone
}

// spec returns the [kafka.ResetSpec] implied by the current strategy / form.
func (r *ResetModel) spec() (kafka.ResetSpec, error) {
	spec := kafka.ResetSpec{
		Strategy: r.strategy,
		Targets:  r.scope.Targets(),
	}
	switch r.strategy {
	case kafka.ResetShift:
		raw := strings.TrimSpace(r.fieldValue(resetFieldShift))
		if raw == "" {
			return kafka.ResetSpec{}, errors.New("shift is required")
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return kafka.ResetSpec{}, fmt.Errorf("shift must be an integer (got %q)", raw)
		}
		spec.Shift = n
	case kafka.ResetTimestamp:
		raw := strings.TrimSpace(r.fieldValue(resetFieldTimestamp))
		if raw == "" {
			return kafka.ResetSpec{}, errors.New("timestamp is required")
		}
		ts, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return kafka.ResetSpec{}, fmt.Errorf("timestamp must be RFC3339 (got %q)", raw)
		}
		spec.Timestamp = ts
	case kafka.ResetSpecific:
		raw := strings.TrimSpace(r.fieldValue(resetFieldOffset))
		if raw == "" {
			return kafka.ResetSpec{}, errors.New("offset is required")
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return kafka.ResetSpec{}, fmt.Errorf("offset must be an integer (got %q)", raw)
		}
		if n < 0 {
			return kafka.ResetSpec{}, fmt.Errorf("offset must be non-negative (got %d)", n)
		}
		spec.Offset = n
	case kafka.ResetEarliest, kafka.ResetLatest:
		// no extra parameters required.
	}
	return spec, nil
}

func (r *ResetModel) fieldValue(key string) string {
	if r.form == nil {
		return ""
	}
	f, _ := r.form.Field(key)
	return f.Value
}

// View renders the reset flow, dispatching to the per-step renderer.
func (r *ResetModel) View() string {
	body := []string{r.headerBlock()}
	if r.err != "" {
		body = append(body, r.styles.StatusErr.Render("error: "+r.err))
	}
	switch r.step {
	case StepStrategy:
		body = append(body, r.renderStrategyStep())
	case StepParams:
		body = append(body, r.renderParamsStep())
	case StepPreview:
		body = append(body, r.renderPreviewStep())
	case StepDone:
		body = append(body, r.renderPreviewStep())
	}
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(strings.Join(body, "\n"))
	if r.width <= 0 {
		return box
	}
	return lipgloss.PlaceHorizontal(r.width, lipgloss.Center, box)
}

func (r *ResetModel) headerBlock() string {
	count, topicCount := r.previewCounts()
	header := r.scope.HeaderLabel(count, topicCount)
	expressTag := ""
	if r.express {
		expressTag = "  " + r.styles.HintKey.Render("[express]")
	}
	stepTag := r.styles.HintLabel.Render(stepLabel(r.step, r.express))
	return r.styles.HelpTitle.Render("Reset offsets · "+r.group) + "\n" +
		r.styles.StatusInfo.Render(header) + expressTag + "  " + stepTag
}

func stepLabel(step ResetStep, express bool) string {
	if express {
		switch step {
		case StepStrategy:
			return "step 1/2 strategy"
		case StepParams:
			return "step 2/2 params"
		case StepPreview, StepDone:
			return "committing"
		}
		return ""
	}
	switch step {
	case StepStrategy:
		return "step 1/3 strategy"
	case StepParams:
		return "step 2/3 params"
	case StepPreview:
		return "step 3/3 preview"
	case StepDone:
		return "done"
	}
	return ""
}

func (r *ResetModel) previewCounts() (int, int) {
	parts := len(r.preview.Partitions)
	topics := map[string]struct{}{}
	for _, p := range r.preview.Partitions {
		topics[p.Topic] = struct{}{}
	}
	return parts, len(topics)
}

func (r *ResetModel) renderStrategyStep() string {
	options := []struct {
		s     kafka.ResetStrategy
		label string
		hint  string
	}{
		{kafka.ResetEarliest, "earliest", "seek to log-start (re-consume everything)"},
		{kafka.ResetLatest, "latest", "seek to log-end (skip to current)"},
		{kafka.ResetShift, "shift", "add a delta to the current commit"},
		{kafka.ResetTimestamp, "timestamp", "seek to the first record at-or-after a time"},
		{kafka.ResetSpecific, "specific", "seek to a specific offset"},
	}
	lines := []string{r.styles.HelpTitle.Render("Choose strategy")}
	for _, opt := range options {
		marker := "( ) "
		style := r.styles.Command
		if opt.s == r.strategy {
			marker = "(•) "
			style = r.styles.CommandHL
		}
		lines = append(lines, "  "+style.Render(marker+opt.label)+"  "+r.styles.HintLabel.Render(opt.hint))
	}
	return strings.Join(lines, "\n")
}

func (r *ResetModel) renderParamsStep() string {
	if r.form == nil {
		return r.styles.StatusInfo.Render("(no params)")
	}
	return r.styles.HelpTitle.Render("Parameters") + "\n" + r.form.View()
}

func (r *ResetModel) renderPreviewStep() string {
	parts := []string{r.styles.HelpTitle.Render("Preview")}
	if r.previewing {
		parts = append(parts, r.styles.StatusInfo.Render("computing preview…"))
		return strings.Join(parts, "\n")
	}
	if r.committing {
		parts = append(parts, r.styles.StatusInfo.Render("committing…"))
		return strings.Join(parts, "\n")
	}
	if len(r.preview.Partitions) == 0 {
		parts = append(parts, r.styles.StatusInfo.Render("(no partitions)"))
	} else {
		parts = append(parts, r.renderPreviewTable())
	}
	parts = append(parts, "", r.renderSummary())
	if r.step == StepDone {
		parts = append(parts, "", r.styles.StatusInfo.Render("commit applied"))
	} else {
		parts = append(parts, "", r.styles.HintLabel.Render("y commit  n/esc cancel"))
	}
	return strings.Join(parts, "\n")
}

func (r *ResetModel) renderPreviewTable() string {
	header := []string{
		padRight("Topic", 24),
		padRight("P", 4),
		padRight("Committed", 14),
		padRight("Target", 14),
		padRight("Diff", 14),
		padRight("Note", 24),
	}
	lines := []string{r.styles.HelpTitle.Render(strings.Join(header, "  "))}
	for _, p := range r.preview.Partitions {
		row := []string{
			padRight(p.Topic, 24),
			padRight(strconv.FormatInt(int64(p.Partition), 10), 4),
			padRight(offsetCell(p.Committed), 14),
			padRight(formatThousands(p.Target), 14),
			padRight(formatDiff(p.Diff), 14),
			padRight(p.Note, 24),
		}
		lines = append(lines, "  "+strings.Join(row, "  "))
	}
	return strings.Join(lines, "\n")
}

func (r *ResetModel) renderSummary() string {
	switch r.preview.Strategy {
	case kafka.ResetEarliest, kafka.ResetShift, kafka.ResetTimestamp:
		body := fmt.Sprintf(
			"summary: ~%s records will be re-consumed",
			formatThousands(r.preview.Summary.Reconsume),
		)
		if r.preview.Strategy == kafka.ResetShift {
			body += fmt.Sprintf(", %s skipped", formatThousands(r.preview.Summary.Skipped))
		}
		return r.styles.StatusInfo.Render(body)
	case kafka.ResetLatest:
		return r.styles.StatusInfo.Render(fmt.Sprintf(
			"summary: ~%s records will be skipped",
			formatThousands(r.preview.Summary.Skipped),
		))
	case kafka.ResetSpecific:
		return r.styles.StatusInfo.Render(fmt.Sprintf(
			"summary: re-consume %s, skip %s",
			formatThousands(r.preview.Summary.Reconsume),
			formatThousands(r.preview.Summary.Skipped),
		))
	}
	return ""
}

func formatDiff(d int64) string {
	if d > 0 {
		return "+" + formatThousands(d)
	}
	return formatThousands(d)
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}

// ----- Messages -----

// ResetPreviewMsg surfaces the preview snapshot from PreviewReset.
type ResetPreviewMsg struct {
	Preview kafka.ResetPreview
	Err     error
}

// ResetCommittedMsg surfaces the post-commit snapshot from ResetOffsets.
type ResetCommittedMsg struct {
	Result kafka.ResetPreview
	Err    error
}

func previewCmd(svc Service, group string, spec kafka.ResetSpec) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pv, err := svc.PreviewReset(ctx, group, spec)
		return ResetPreviewMsg{Preview: pv, Err: err}
	}
}

func commitCmd(svc Service, group string, spec kafka.ResetSpec) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		res, err := svc.ResetOffsets(ctx, group, spec)
		return ResetCommittedMsg{Result: res, Err: err}
	}
}
