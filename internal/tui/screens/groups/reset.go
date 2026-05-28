package groups

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ResetScope is the user's choice for which (topic, partition) pairs the
// reset will touch.
type ResetScope interface {
	// Targets returns the explicit restriction; nil/empty defers to the
	// kafka.Client's "every partition with commits" fallback.
	Targets() []kafka.TopicPartition
	HeaderLabel(partitionCount, topicCount int) string
}

// ScopeWholeGroup targets every partition the group has committed offsets
// for — used when reset is invoked from the groups list.
type ScopeWholeGroup struct{ Group string }

func (s ScopeWholeGroup) Targets() []kafka.TopicPartition { return nil }

func (s ScopeWholeGroup) HeaderLabel(partitionCount, topicCount int) string {
	// pre-preview the scope size is unknown — make the intent explicit so
	// users don't read "Resetting all partitions" as something narrower.
	if partitionCount == 0 {
		return "Resetting every partition of every topic in this group"
	}
	if topicCount == 1 {
		return fmt.Sprintf("Resetting %d partitions across 1 topic in this group", partitionCount)
	}
	return fmt.Sprintf("Resetting %d partitions across %d topics in this group", partitionCount, topicCount)
}

// ScopeTopic restricts the reset to the partitions of one topic. The
// concrete partition list is captured at scope-construction time so the
// kafka client gets exact targets instead of falling back to "all
// partitions in the group".
type ScopeTopic struct {
	Group   string
	Topic   string
	Members []kafka.TopicPartition
}

func (s ScopeTopic) Targets() []kafka.TopicPartition {
	out := make([]kafka.TopicPartition, len(s.Members))
	copy(out, s.Members)
	return out
}

func (s ScopeTopic) HeaderLabel(partitionCount, _ int) string {
	return fmt.Sprintf("Resetting %d partitions in %s", partitionCount, s.Topic)
}

// ScopePartition narrows the reset to a single (topic, partition) pair —
// the most precise scope, useful when the user has drilled into a
// partition row in the detail view.
type ScopePartition struct {
	Group     string
	Topic     string
	Partition int32
}

func (s ScopePartition) Targets() []kafka.TopicPartition {
	return []kafka.TopicPartition{{Topic: s.Topic, Partition: s.Partition}}
}

func (s ScopePartition) HeaderLabel(_, _ int) string {
	return fmt.Sprintf("Resetting %s partition %d", s.Topic, s.Partition)
}

// ResetStep is the current step of the 4-step flow.
type ResetStep int

const (
	StepStrategy ResetStep = iota
	StepParams
	StepPreview
	StepDone
)

// ResetAction is the host-facing intent of the reset model.
type ResetAction struct {
	Cancel bool
	Done   bool
	Result *kafka.ResetPreview
}

type ResetOptions struct {
	Service Service
	Group   string
	Scope   ResetScope
	Now     func() time.Time
	Styles  theme.Styles
}

// ResetModel hosts the 3-step reset flow (strategy → params → preview).
// Callers gate the flow behind their own ReadOnly check.
type ResetModel struct {
	svc   Service
	group string
	scope ResetScope

	step         ResetStep
	strategy     kafka.ResetStrategy
	form         *components.Form
	preview      kafka.ResetPreview
	previewTable *components.Table
	result       *kafka.ResetPreview

	committing bool
	previewing bool
	err        string

	width, height int
	action        ResetAction
	now           func() time.Time
	styles        theme.Styles
}

const (
	resetFieldShift     = "shift"
	resetFieldTimestamp = "timestamp"
	resetFieldOffset    = "offset"
)

func NewResetModel(opts ResetOptions) *ResetModel {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	previewCols := []components.Column{
		{Title: "Topic", Flex: true, MinWidth: 16, Sortable: true},
		{Title: "Partition", Width: 9, Sortable: true},
		{Title: "Committed", Width: 14, Sortable: true},
		{Title: "Target", Width: 14, Sortable: true},
		{Title: "Diff", Width: 14, Sortable: true},
		{Title: "Note", Width: 24, Sortable: false},
	}
	return &ResetModel{
		svc:          opts.Service,
		group:        opts.Group,
		scope:        opts.Scope,
		step:         StepStrategy,
		strategy:     kafka.ResetEarliest,
		previewTable: components.NewTable(previewCols, components.WithStyles(styles)),
		now:          now,
		styles:       styles,
	}
}

func (r *ResetModel) Init() tea.Cmd { return nil }

func (r *ResetModel) Group() string { return r.group }

func (r *ResetModel) Scope() ResetScope { return r.scope }

func (r *ResetModel) Step() ResetStep { return r.step }

func (r *ResetModel) Strategy() kafka.ResetStrategy { return r.strategy }

func (r *ResetModel) Preview() kafka.ResetPreview { return r.preview }

func (r *ResetModel) Action() ResetAction { return r.action }

func (r *ResetModel) ConsumeAction() ResetAction {
	a := r.action
	r.action = ResetAction{}
	return a
}

func (r *ResetModel) Err() string { return r.err }

// SetSize hands the available body area to the preview table. Reset is
// rendered flush with the screen frame (no inner box), so the chrome
// budget is just the static rows around the table.
func (r *ResetModel) SetSize(w, h int) {
	r.width, r.height = w, h
	if w > 0 {
		r.previewTable.SetTotalWidth(w)
	}
	if h > 0 {
		tblH := maxInt(3, h-resetChromeRows)
		r.previewTable.SetHeight(tblH)
	}
}

// resetChromeRows is the layout overhead around the preview table at
// StepPreview: 1 scope/step header + 1 "Preview" title + 1 blank + 1
// summary + 1 blank + 1 hint = 6. The table's own column header sits
// inside the value passed to table.SetHeight and isn't counted here.
const resetChromeRows = 6

func (r *ResetModel) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(r.bindings())
}

func (r *ResetModel) bindings() []keymap.Binding {
	switch r.step {
	case StepStrategy:
		return []keymap.Binding{
			{Keys: []string{"j", "down"}, Label: "next strategy", Category: "Reset", Handler: r.actStrategyMove(+1)},
			{Keys: []string{"k", "up"}, Label: "previous strategy", Category: "Reset", Handler: r.actStrategyMove(-1)},
			{Keys: []string{"enter"}, Label: "next step", Category: "Reset", Hint: true, Handler: r.actAdvanceFromStrategy},
			{Keys: []string{"esc"}, Label: "cancel reset", Category: "Reset", Hint: true, Handler: r.actCancel},
		}
	case StepParams:
		// the params form always has exactly one field (shift / timestamp /
		// offset depending on strategy), so tab / shift+tab / arrows have
		// nothing to navigate. We deliberately omit those bindings — Form
		// still consumes the keys internally as no-ops, so nothing surprises
		// the user, but help doesn't lie about "next form field" either.
		return []keymap.Binding{
			{Keys: []string{"enter"}, Label: "next step", Category: "Reset", Hint: true, Handler: r.actAdvanceFromParams},
			{Keys: []string{"esc"}, Label: "cancel reset", Category: "Reset", Hint: true, Handler: r.actCancel},
		}
	case StepPreview:
		return []keymap.Binding{
			{Keys: []string{"y", "Y"}, Label: "commit reset", Category: "Reset", Hint: true, Handler: r.actCommit},
			{Keys: []string{"n", "N"}, Label: "cancel reset", Category: "Reset", Hint: true, Handler: r.actCancel},
			{Keys: []string{"esc"}, Label: "cancel reset", Category: "Reset", Handler: r.actCancel},
		}
	case StepDone:
		return nil
	}
	return nil
}

func (r *ResetModel) actCancel() tea.Cmd {
	r.action.Cancel = true
	return nil
}

func (r *ResetModel) actStrategyMove(delta int) func() tea.Cmd {
	return func() tea.Cmd {
		strategies := resetStrategies()
		idx := 0
		for i, s := range strategies {
			if s == r.strategy {
				idx = i
				break
			}
		}
		idx = (idx + delta + len(strategies)) % len(strategies)
		r.strategy = strategies[idx]
		return nil
	}
}

func (r *ResetModel) actAdvanceFromStrategy() tea.Cmd {
	_, cmd := r.advanceFromStrategy()
	return cmd
}

func (r *ResetModel) actAdvanceFromParams() tea.Cmd {
	_, cmd := r.dispatchAfterParams()
	return cmd
}

func (r *ResetModel) actCommit() tea.Cmd {
	spec, err := r.spec()
	if err != nil {
		r.err = err.Error()
		return nil
	}
	r.committing = true
	return commitCmd(r.svc, r.group, spec)
}

func resetStrategies() []kafka.ResetStrategy {
	return []kafka.ResetStrategy{
		kafka.ResetEarliest,
		kafka.ResetLatest,
		kafka.ResetShift,
		kafka.ResetTimestamp,
		kafka.ResetSpecific,
	}
}

func (r *ResetModel) Update(msg tea.Msg) (*ResetModel, tea.Cmd) {
	switch msg := msg.(type) {
	case ResetPreviewMsg:
		r.handlePreview(msg)
		return r, nil
	case ResetCommittedMsg:
		r.handleCommitted(msg)
		return r, nil
	case tea.PasteMsg:
		// forward straight to the params form; non-text steps drop it.
		if r.step == StepParams && r.form != nil {
			f, _ := r.form.Update(msg)
			r.form = f
		}
		return r, nil
	case tea.KeyPressMsg:
		return r.handleKey(msg)
	}
	return r, nil
}

func (r *ResetModel) handleKey(key tea.KeyPressMsg) (*ResetModel, tea.Cmd) {
	if cmd, ok := keymap.Dispatch(r.bindings(), key); ok {
		return r, cmd
	}
	// params step forwards unmatched keys to the form for text input.
	if r.step == StepParams && r.form != nil {
		f, _ := r.form.Update(key)
		r.form = f
		return r, nil
	}
	// preview step forwards unmatched keys (j/k/g/G/page-up/down) to the
	// scrollable table — without this, large previews silently truncate.
	if r.step == StepPreview && !r.previewing && !r.committing {
		tbl, _ := r.previewTable.Update(key)
		r.previewTable = tbl
	}
	return r, nil
}

// advanceFromStrategy: Earliest/Latest skip the params step.
func (r *ResetModel) advanceFromStrategy() (*ResetModel, tea.Cmd) {
	switch r.strategy {
	case kafka.ResetEarliest, kafka.ResetLatest:
		return r.dispatchAfterParams()
	case kafka.ResetShift, kafka.ResetTimestamp, kafka.ResetSpecific:
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

func (r *ResetModel) dispatchAfterParams() (*ResetModel, tea.Cmd) {
	spec, err := r.spec()
	if err != nil {
		r.err = err.Error()
		return r, nil
	}
	r.err = ""
	r.previewing = true
	r.step = StepPreview
	return r, previewCmd(r.svc, r.group, spec)
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
	r.previewTable.SetRows(r.buildPreviewRows())
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
	r.previewTable.SetRows(r.buildPreviewRows())
	r.action.Done = true
	r.action.Result = &res
	r.step = StepDone
}

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

// View renders the flow flush with the screen frame (no inner box). The
// host's frame already carries the "Reset offsets · <group>" title, so a
// nested rounded border would just duplicate the chrome and clip when
// the preview list grows tall — see produce.View for the pattern.
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
	case StepPreview, StepDone:
		body = append(body, r.renderPreviewStep())
	}
	return strings.Join(body, "\n")
}

func (r *ResetModel) headerBlock() string {
	count, topicCount := r.previewCounts()
	header := r.scope.HeaderLabel(count, topicCount)
	stepTag := r.styles.HintLabel.Render(stepLabel(r.step))
	return r.styles.StatusInfo.Render(header) + "  " + stepTag
}

func stepLabel(step ResetStep) string {
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

// previewCounts reports (partitionCount, topicCount). It prefers the
// computed preview because that's the authoritative post-clamp set, but
// falls back to the scope's own Targets() so the header at step 1
// already shows "Resetting N partitions" instead of "Resetting 0
// partitions" before the preview RPC has run.
func (r *ResetModel) previewCounts() (int, int) {
	if len(r.preview.Partitions) == 0 {
		if targets := r.scope.Targets(); len(targets) > 0 {
			topics := map[string]struct{}{}
			for _, t := range targets {
				topics[t.Topic] = struct{}{}
			}
			return len(targets), len(topics)
		}
	}
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
		parts = append(parts, r.previewTable.View())
	}
	parts = append(parts, "", r.renderSummary())
	if r.step == StepDone {
		parts = append(parts, "", r.styles.StatusInfo.Render("commit applied"))
	} else {
		parts = append(parts, "", components.HintLine(r.styles,
			components.Hint{Key: "y", Label: "commit"},
			components.Hint{Key: "n/esc", Label: "cancel"},
			components.Hint{Key: "j/k", Label: "scroll"},
		))
	}
	return strings.Join(parts, "\n")
}

func (r *ResetModel) buildPreviewRows() []components.Row {
	rows := make([]components.Row, 0, len(r.preview.Partitions))
	for _, p := range r.preview.Partitions {
		rows = append(rows, components.Row{
			ID: fmt.Sprintf("%s/%d", p.Topic, p.Partition),
			Values: []string{
				p.Topic,
				strconv.FormatInt(int64(p.Partition), 10),
				offsetCell(p.Committed),
				formatThousands(p.Target),
				formatDiff(p.Diff),
				p.Note,
			},
		})
	}
	return rows
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

// ----- Messages -----

type ResetPreviewMsg struct {
	Preview kafka.ResetPreview
	Err     error
}

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
