package topics

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// FormMode mirrors the produce form's NORMAL/INSERT split. NORMAL is for
// navigation (tab / arrows), INSERT is for typing into the focused field.
type FormMode int

const (
	// FormNormal: navigation only — typing into a field is ignored.
	FormNormal FormMode = iota
	// FormInsert: typing inserts into the focused field.
	FormInsert
)

const keyEsc = "esc"

// CreateForm wraps the form component used for creating a new topic.
type CreateForm struct {
	form   *components.Form
	err    string
	styles theme.Styles
	mode   FormMode
}

// NewCreateForm constructs a fresh create form with default values.
func NewCreateForm(styles theme.Styles) *CreateForm {
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	fields := []components.Field{
		{Key: "name", Label: "Name", Kind: components.FieldText},
		{Key: "partitions", Label: "Partitions", Kind: components.FieldText, Value: "1"},
		{Key: "replication_factor", Label: "Replication factor", Kind: components.FieldText, Value: "1"},
		{Key: "cleanup_policy", Label: "cleanup.policy", Kind: components.FieldSegmented, Options: []string{"delete", "compact"}, Value: "delete"},
		{Key: "retention_ms", Label: "retention.ms", Kind: components.FieldText, Value: ""},
		{Key: "min_insync_replicas", Label: "min.insync.replicas", Kind: components.FieldText, Value: ""},
	}
	cf := &CreateForm{
		form:   components.NewForm(fields, components.WithFormStyles(styles)),
		styles: styles,
	}
	cf.form.SetEditing(false)
	return cf
}

// Form exposes the underlying form component (for tests).
func (c *CreateForm) Form() *components.Form { return c.form }

// Mode returns the current edit mode (NORMAL / INSERT).
func (c *CreateForm) Mode() FormMode { return c.mode }

// Update routes a key message through the NORMAL/INSERT state machine,
// mirroring the produce form so the editing UX is consistent.
func (c *CreateForm) Update(msg tea.Msg) (*CreateForm, tea.Cmd) {
	c.err = ""
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return c, nil
	}
	c.form, c.mode = updateFormModal(c.form, c.mode, key)
	return c, nil
}

// SetError surfaces an inline error message above the form.
func (c *CreateForm) SetError(msg string) { c.err = msg }

// Err returns the latest validation error (or empty string).
func (c *CreateForm) Err() string { return c.err }

// Spec validates the form contents and converts them to a CreateTopicSpec.
// On validation error it returns a non-nil error that the caller can display.
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

// View renders the create form. width=0 falls back to natural width.
func (c *CreateForm) View(width int) string {
	header := c.styles.HelpTitle.Render("New topic")
	hint := c.styles.HintLabel.Render(formHintLine(c.mode, "create"))
	parts := []string{header}
	if c.err != "" {
		parts = append(parts, c.styles.StatusErr.Render(c.err))
	}
	parts = append(parts, c.form.View(), "", hint)
	body := strings.Join(parts, "\n")
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(body)
	if width <= 0 {
		return box
	}
	return lipgloss.PlaceHorizontal(width, lipgloss.Center, box)
}

// formHintLine returns the bottom hint string shown below the form, varying
// by mode so the user always sees what the next keystroke will do. `verb`
// is the submit verb ("create" / "clone").
func formHintLine(mode FormMode, verb string) string {
	if mode == FormInsert {
		return "type to edit  tab/enter commit & next  shift+tab back  esc to NORMAL"
	}
	return "tab/↓ next  shift+tab/↑ prev  enter edit  ctrl+s " + verb + "  esc cancel"
}

// updateFormModal is the shared NORMAL/INSERT state machine for the create
// and clone forms. Returns the (possibly replaced) form and the resulting
// mode; the form's editing flag and focused-suffix tag are kept in sync as
// a side effect so callers don't need to mirror the bookkeeping.
func updateFormModal(form *components.Form, mode FormMode, key tea.KeyPressMsg) (*components.Form, FormMode) {
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
	// when a segmented field's popup is open in NORMAL, every navigation
	// keystroke (enter / arrows / hjkl / tab / esc) belongs to the popup —
	// the popup is modal until the user explicitly picks a value or
	// dismisses it. Leaking tab/up to FocusNext/Prev would close it
	// silently and look like the keys went somewhere unexpected.
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
	case "enter":
		// dropdown / segmented handle enter natively (cycle / open popup);
		// other field kinds enter INSERT so the next keystroke writes into
		// the field.
		if isPicker(form.FocusedField().Kind) {
			f, _ := form.Update(key)
			return f, mode
		}
		applyMode(form, FormInsert)
		return form, FormInsert
	}
	// dropdown / segmented left/right cycling stays interactive in NORMAL.
	if isPicker(form.FocusedField().Kind) {
		f, _ := form.Update(key)
		return f, mode
	}
	return form, mode
}

// applyMode keeps the form's editing flag and the focused-suffix tag in
// sync with the modal mode tracked by the wrapper.
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

// CloneForm collects the destination name and replication factor for a clone.
type CloneForm struct {
	source string
	form   *components.Form
	err    string
	styles theme.Styles
	mode   FormMode
}

// NewCloneForm constructs a clone form prefilled with `source-clone` as dst.
func NewCloneForm(source string, styles theme.Styles) *CloneForm {
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	fields := []components.Field{
		{Key: "destination", Label: "Destination", Kind: components.FieldText, Value: source + "-clone"},
		{Key: "replication_factor", Label: "Replication factor (0=source)", Kind: components.FieldText, Value: "0"},
		{Key: "copy_configs", Label: "Copy configs", Kind: components.FieldSegmented, Options: []string{"yes", "no"}, Value: "yes"},
	}
	cf := &CloneForm{
		source: source,
		form:   components.NewForm(fields, components.WithFormStyles(styles)),
		styles: styles,
	}
	cf.form.SetEditing(false)
	return cf
}

// Mode returns the current edit mode (NORMAL / INSERT).
func (c *CloneForm) Mode() FormMode { return c.mode }

// Source returns the source topic name.
func (c *CloneForm) Source() string { return c.source }

// Form exposes the underlying form component (for tests).
func (c *CloneForm) Form() *components.Form { return c.form }

// Update routes a key message through the NORMAL/INSERT state machine.
func (c *CloneForm) Update(msg tea.Msg) (*CloneForm, tea.Cmd) {
	c.err = ""
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return c, nil
	}
	c.form, c.mode = updateFormModal(c.form, c.mode, key)
	return c, nil
}

// SetError surfaces an inline error.
func (c *CloneForm) SetError(msg string) { c.err = msg }

// Err returns the current validation error.
func (c *CloneForm) Err() string { return c.err }

// Submit validates the form and returns (source, destination) plus the
// clone options. Returns an error when the dst name is empty or duplicates
// the source.
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

// Options returns the kafka.CloneOptions implied by the current form values.
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

// View renders the clone form.
func (c *CloneForm) View(width int) string {
	header := c.styles.HelpTitle.Render("Clone topic: " + c.source)
	hint := c.styles.HintLabel.Render(formHintLine(c.mode, "clone"))
	parts := []string{header}
	if c.err != "" {
		parts = append(parts, c.styles.StatusErr.Render(c.err))
	}
	parts = append(parts, c.form.View(), "", hint)
	body := strings.Join(parts, "\n")
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(body)
	if width <= 0 {
		return box
	}
	return lipgloss.PlaceHorizontal(width, lipgloss.Center, box)
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
