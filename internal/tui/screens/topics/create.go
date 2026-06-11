package topics

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// FormMode mirrors the produce form's NORMAL/INSERT split.
type FormMode int

const (
	FormNormal FormMode = iota
	FormInsert
)

const keyEsc = "esc"

type CreateForm struct {
	form   *components.Form
	err    string
	styles theme.Styles
	mode   FormMode
}

func NewCreateForm(styles theme.Styles) *CreateForm {
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	cf := &CreateForm{
		form:   components.NewForm(createFormFields(), components.WithFormStyles(styles)),
		styles: styles,
	}
	cf.form.SetEditing(false)
	return cf
}

func createFormFields() []components.Field {
	return []components.Field{
		{Key: "name", Label: "Name", Kind: components.FieldText},
		{Key: "partitions", Label: "Partitions", Kind: components.FieldText, Value: "1"},
		{Key: "replication_factor", Label: "Replication factor", Kind: components.FieldText, Value: "1"},
		{Key: "cleanup_policy", Label: "cleanup.policy", Kind: components.FieldSegmented, Options: []string{"delete", "compact"}, Value: "delete"},
		{Key: "retention_ms", Label: "retention.ms", Kind: components.FieldText, Value: ""},
		{Key: "min_insync_replicas", Label: "min.insync.replicas", Kind: components.FieldText, Value: ""},
	}
}

func (c *CreateForm) Form() *components.Form { return c.form }

func (c *CreateForm) Mode() FormMode { return c.mode }

func (c *CreateForm) clear() *components.Form {
	c.form.Reset()
	applyMode(c.form, c.mode)
	return c.form
}

func (c *CreateForm) Update(msg tea.Msg) (*CreateForm, tea.Cmd) {
	c.err = ""
	if paste, ok := msg.(tea.PasteMsg); ok {
		c.form, c.mode = applyPasteToForm(c.form, c.mode, paste)
		return c, nil
	}
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return c, nil
	}
	c.form, c.mode = updateFormModal(c.form, c.mode, key, c.clear)
	return c, nil
}

func (c *CreateForm) SetError(msg string) { c.err = msg }

func (c *CreateForm) Err() string { return c.err }

// Spec validates the form contents and converts them to a CreateTopicSpec.
func (c *CreateForm) Spec() (kafka.CreateTopicSpec, error) {
	get := func(key string) string {
		fld, _ := c.form.Field(key)
		return strings.TrimSpace(fld.Value)
	}
	name := get("name")
	if name == "" {
		return kafka.CreateTopicSpec{}, errors.New("name is required")
	}
	partsRaw := get("partitions")
	parts, err := parsePositiveInt32(partsRaw, "partitions")
	if err != nil {
		return kafka.CreateTopicSpec{}, err
	}
	rfRaw := get("replication_factor")
	rf, err := parsePositiveInt16(rfRaw, "replication_factor")
	if err != nil {
		return kafka.CreateTopicSpec{}, err
	}

	configs := map[string]string{}
	if v := get("cleanup_policy"); v != "" {
		configs[kafka.ConfigCleanupPolicy] = v
	}
	if v := get("retention_ms"); v != "" {
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return kafka.CreateTopicSpec{}, errors.New("retention_ms must be an integer")
		}
		configs[kafka.ConfigRetentionMs] = v
	}
	if v := get("min_insync_replicas"); v != "" {
		if _, err := strconv.Atoi(v); err != nil {
			return kafka.CreateTopicSpec{}, errors.New("min_insync_replicas must be an integer")
		}
		configs[kafka.ConfigMinInSyncReplica] = v
	}
	return kafka.CreateTopicSpec{
		Name:              name,
		Partitions:        parts,
		ReplicationFactor: rf,
		Configs:           configs,
	}, nil
}

// View renders the form flush with the screen frame — no nested rounded
// border. The host already paints the outer frame and a "Create topic"
// title, so the inner box would just be a duplicate that clips when the
// form is taller than the viewport. Matches the produce-screen pattern.
func (c *CreateForm) View(_ int) string {
	header := c.styles.HelpTitle.Render("New topic")
	hint := components.HintLine(c.styles, formHints(c.mode, "create")...)
	parts := []string{header}
	if c.err != "" {
		parts = append(parts, c.styles.StatusErr.Render(c.err))
	}
	parts = append(parts, c.form.View(), "", hint)
	return strings.Join(parts, "\n")
}

func formHints(mode FormMode, verb string) []components.Hint {
	if mode == FormInsert {
		return []components.Hint{
			{Label: "type to edit"},
			{Key: "tab/enter", Label: "commit & next"},
			{Key: "shift+tab", Label: "back"},
			{Key: "esc", Label: "to NORMAL"},
		}
	}
	return []components.Hint{
		{Key: "tab/↓", Label: "next"},
		{Key: "shift+tab/↑", Label: "prev"},
		{Key: "enter", Label: "edit"},
		{Key: "s", Label: verb},
		{Key: "esc", Label: "cancel"},
	}
}

// updateFormModal is the shared NORMAL/INSERT state machine for the
// create and clone forms; editing flag and focused-suffix are kept in
// sync as a side effect.
//
// clearFn, when non-nil, is invoked on ctrl+u in NORMAL — the form-level
// "reset to defaults" action. It returns the rebuilt form so the caller can
// adopt the new pointer. In INSERT ctrl+u stays a field-level kill (handled
// inside the form via lineedit), so no collision.
func updateFormModal(form *components.Form, mode FormMode, key tea.KeyPressMsg, clearFn func() *components.Form) (*components.Form, FormMode) {
	if mode == FormInsert {
		switch key.String() {
		case keyEsc:
			applyMode(form, FormNormal)
			return form, FormNormal
		case "tab", "enter":
			form.FocusNext()
			applyMode(form, FormNormal)
			return form, FormNormal
		case "shift+tab":
			form.FocusPrev()
			applyMode(form, FormNormal)
			return form, FormNormal
		}
		f, _ := form.Update(key)
		return f, mode
	}
	// segmented popup is modal — nav keys belong to it; otherwise
	// FocusNext/Prev would close it silently.
	if form.PopupActive() {
		switch key.String() {
		case "enter", "up", "down", "left", "right", "j", "k", "h", "l", "tab", "shift+tab", keyEsc:
			f, _ := form.Update(key)
			return f, mode
		}
	}
	switch key.String() {
	case "tab", "down", "j":
		form.FocusNext()
		return form, mode
	case "shift+tab", "up", "k":
		form.FocusPrev()
		return form, mode
	case "ctrl+u":
		// popup is a modal sub-state — defer the form-level clear until it
		// closes, so the user picking an option doesn't accidentally wipe
		// every field they already filled in.
		if form.PopupActive() {
			return form, mode
		}
		if clearFn != nil {
			form = clearFn()
		}
		return form, mode
	case "enter":
		// pickers handle enter natively (cycle / open popup); other
		// fields enter INSERT.
		if isPicker(form.FocusedField().Kind) {
			f, _ := form.Update(key)
			return f, mode
		}
		applyMode(form, FormInsert)
		return form, FormInsert
	}
	// picker left/right cycling stays interactive in NORMAL.
	if isPicker(form.FocusedField().Kind) {
		f, _ := form.Update(key)
		return f, mode
	}
	return form, mode
}

// applyPasteToForm routes a paste event into the focused text-like field
// without changing mode. Paste is a discrete action on the focused field, not
// a keystroke stream, so it never crosses NORMAL into INSERT (see § Paste in
// CLAUDE.md). Non-text fields silently drop the paste — its content has no
// meaning for an option picker.
func applyPasteToForm(form *components.Form, mode FormMode, paste tea.PasteMsg) (*components.Form, FormMode) {
	kind := form.FocusedField().Kind
	if kind != components.FieldText && kind != components.FieldTextarea && kind != components.FieldList {
		return form, mode
	}
	f, _ := form.Update(paste)
	return f, mode
}

func applyMode(form *components.Form, mode FormMode) {
	form.SetEditing(mode == FormInsert)
	if mode == FormInsert {
		form.SetFocusedSuffix("[EDIT]")
	} else {
		form.SetFocusedSuffix("")
	}
}

func isPicker(k components.FieldKind) bool {
	return k == components.FieldDropdown || k == components.FieldSegmented
}

type CloneForm struct {
	source string
	form   *components.Form
	err    string
	styles theme.Styles
	mode   FormMode
}

func NewCloneForm(source string, styles theme.Styles) *CloneForm {
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	cf := &CloneForm{
		source: source,
		form:   components.NewForm(cloneFormFields(source), components.WithFormStyles(styles)),
		styles: styles,
	}
	cf.form.SetEditing(false)
	return cf
}

func cloneFormFields(source string) []components.Field {
	return []components.Field{
		{Key: "destination", Label: "Destination", Kind: components.FieldText, Value: source + "-clone"},
		{Key: "replication_factor", Label: "Replication factor (0=source)", Kind: components.FieldText, Value: "0"},
		{Key: "copy_configs", Label: "Copy configs", Kind: components.FieldSegmented, Options: []string{"yes", "no"}, Value: "yes"},
	}
}

func (c *CloneForm) Mode() FormMode { return c.mode }

func (c *CloneForm) Source() string { return c.source }

func (c *CloneForm) Form() *components.Form { return c.form }

func (c *CloneForm) clear() *components.Form {
	c.form.Reset()
	applyMode(c.form, c.mode)
	return c.form
}

func (c *CloneForm) Update(msg tea.Msg) (*CloneForm, tea.Cmd) {
	c.err = ""
	if paste, ok := msg.(tea.PasteMsg); ok {
		c.form, c.mode = applyPasteToForm(c.form, c.mode, paste)
		return c, nil
	}
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return c, nil
	}
	c.form, c.mode = updateFormModal(c.form, c.mode, key, c.clear)
	return c, nil
}

func (c *CloneForm) SetError(msg string) { c.err = msg }

func (c *CloneForm) Err() string { return c.err }

func (c *CloneForm) Submit() (src, dst string, err error) {
	get := func(key string) string {
		fld, _ := c.form.Field(key)
		return strings.TrimSpace(fld.Value)
	}
	dst = get("destination")
	if dst == "" {
		return "", "", errors.New("destination is required")
	}
	if dst == c.source {
		return "", "", errors.New("destination must differ from source")
	}
	rf := get("replication_factor")
	if rf != "" {
		if _, err := strconv.Atoi(rf); err != nil {
			return "", "", errors.New("replication_factor must be an integer")
		}
	}
	return c.source, dst, nil
}

func (c *CloneForm) Options() kafka.CloneOptions {
	get := func(key string) string {
		fld, _ := c.form.Field(key)
		return strings.TrimSpace(fld.Value)
	}
	rf, _ := strconv.Atoi(get("replication_factor"))
	opts := kafka.CloneOptions{}
	if rf > 0 && rf <= 1<<15-1 {
		opts.ReplicationFactor = int16(rf) //nolint:gosec // bounded above
	}
	if strings.EqualFold(get("copy_configs"), "yes") {
		opts.CopyConfigs = true
	}
	return opts
}

func (c *CloneForm) View(_ int) string {
	header := c.styles.HelpTitle.Render("Clone topic: " + c.source)
	hint := components.HintLine(c.styles, formHints(c.mode, "clone")...)
	parts := []string{header}
	if c.err != "" {
		parts = append(parts, c.styles.StatusErr.Render(c.err))
	}
	parts = append(parts, c.form.View(), "", hint)
	return strings.Join(parts, "\n")
}

func parsePositiveInt32(raw, label string) (int32, error) {
	if raw == "" {
		return 0, fmt.Errorf("%s is required", label)
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", label)
	}
	if n <= 0 || n > (1<<31-1) {
		return 0, fmt.Errorf("%s must be a positive int32", label)
	}
	return int32(n), nil //nolint:gosec // bounded above
}

func parsePositiveInt16(raw, label string) (int16, error) {
	if raw == "" {
		return 0, fmt.Errorf("%s is required", label)
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", label)
	}
	if n <= 0 || n > (1<<15-1) {
		return 0, fmt.Errorf("%s must be a positive int16", label)
	}
	return int16(n), nil //nolint:gosec // bounded above
}
