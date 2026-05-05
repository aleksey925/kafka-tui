package topics

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka/configcatalog"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

const editFieldKey = "value"

// ConfigEditAction is the host-facing intent of the config-edit screen.
type ConfigEditAction struct {
	Back  bool
	Saved bool
	// Key is set together with Saved so the host can refresh the
	// underlying configs view and toast the right name.
	Key string
}

// ConfigEditOptions configures a [ConfigEditModel].
type ConfigEditOptions struct {
	Service      Service
	Topic        string
	Key          string
	CurrentValue string
	Now          func() time.Time
	Styles       theme.Styles
}

// ConfigEditModel is the topic-level config edit form.
type ConfigEditModel struct {
	svc      Service
	topic    string
	key      string
	entry    configcatalog.Entry
	knownDoc bool

	form *components.Form
	mode FormMode

	saving bool
	err    string

	toasts        *components.Toasts
	width, height int

	action ConfigEditAction
	now    func() time.Time
	styles theme.Styles
}

func NewConfigEditModel(opts ConfigEditOptions) *ConfigEditModel {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	entry, ok := configcatalog.Lookup(opts.Key)
	field := buildEditField(entry, ok, opts.CurrentValue)
	return &ConfigEditModel{
		svc:      opts.Service,
		topic:    opts.Topic,
		key:      opts.Key,
		entry:    entry,
		knownDoc: ok,
		form:     components.NewForm([]components.Field{field}, components.WithFormStyles(styles)),
		toasts:   components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:      now,
		styles:   styles,
	}
}

func buildEditField(entry configcatalog.Entry, known bool, current string) components.Field {
	field := components.Field{
		Key:   editFieldKey,
		Label: "value",
		Kind:  components.FieldText,
		Value: current,
	}
	if !known {
		return field
	}
	switch entry.Type {
	case configcatalog.TypeBoolean:
		field.Kind = components.FieldSegmented
		field.Options = []string{"true", "false"}
		if field.Value != "true" && field.Value != "false" {
			field.Value = "false"
		}
	case configcatalog.TypeSelect:
		field.Kind = components.FieldSegmented
		field.Options = append([]string(nil), entry.EnumValues...)
		if !slices.Contains(field.Options, field.Value) && len(field.Options) > 0 {
			field.Value = field.Options[0]
		}
	case configcatalog.TypeString,
		configcatalog.TypeInteger,
		configcatalog.TypeByteSize,
		configcatalog.TypeDuration,
		configcatalog.TypeRatio:
		// FieldText is the default — nothing more to configure.
	}
	return field
}

func (m *ConfigEditModel) Init() tea.Cmd { return nil }

func (m *ConfigEditModel) Topic() string { return m.topic }

func (m *ConfigEditModel) Key() string { return m.key }

func (m *ConfigEditModel) Action() ConfigEditAction { return m.action }

func (m *ConfigEditModel) ConsumeAction() ConfigEditAction {
	a := m.action
	m.action = ConfigEditAction{}
	return a
}

func (m *ConfigEditModel) Toasts() *components.Toasts { return m.toasts }

func (m *ConfigEditModel) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

func (m *ConfigEditModel) Title() string {
	return "Edit · " + m.topic + " · " + m.key
}

func (m *ConfigEditModel) Breadcrumb() string { return "edit" }

func (m *ConfigEditModel) Mode() FormMode { return m.mode }

func (m *ConfigEditModel) Form() *components.Form { return m.form }

func (m *ConfigEditModel) Saving() bool { return m.saving }

// HasOverlay also covers the saving state so the host's q/esc fallback
// doesn't pop the screen out from under an in-flight AlterTopicConfig RPC.
func (m *ConfigEditModel) HasOverlay() bool {
	return m.mode == FormInsert || m.form.PopupActive() || m.saving
}

// WantsRawInput is true while the user is typing into a text field or a
// segmented popup is open, so global shortcuts like `?` / `:` don't steal
// keystrokes intended for the value.
func (m *ConfigEditModel) WantsRawInput() bool {
	return m.mode == FormInsert || m.form.PopupActive()
}

func (m *ConfigEditModel) SetSize(w, h int) {
	m.width, m.height = w, h
}

func (m *ConfigEditModel) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.bindings())
}

func (m *ConfigEditModel) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.bindings())
}

func (m *ConfigEditModel) bindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"ctrl+s"}, Label: "save", Category: "Edit", Hint: true, Handler: m.actSave},
		{Keys: []string{"esc"}, Label: "cancel / leave INSERT / close popup", Category: "Edit", Hint: true, HandlerMsg: m.actEsc},
	}
}

func (m *ConfigEditModel) actSave() tea.Cmd {
	if m.saving {
		return nil
	}
	value, err := m.validatedValue()
	if err != nil {
		m.err = err.Error()
		return nil
	}
	m.err = ""
	m.saving = true
	return alterConfigCmd(m.svc, m.topic, m.key, value)
}

func (m *ConfigEditModel) actEsc(key tea.KeyPressMsg) tea.Cmd {
	if m.mode == FormInsert || m.form.PopupActive() {
		m.form, m.mode = updateFormModal(m.form, m.mode, key)
		return nil
	}
	if m.saving {
		// the AlterTopicConfig RPC is in flight; popping now would
		// stage a change the user thinks they canceled. Surface a
		// hint and stay on the form until the response arrives.
		m.toasts.Push(components.ToastWarning, "save in progress — wait for the result")
		return nil
	}
	m.action.Back = true
	return nil
}

func (m *ConfigEditModel) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case ConfigAlteredMsg:
		m.handleAltered(msg)
		return nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *ConfigEditModel) handleKey(key tea.KeyPressMsg) tea.Cmd {
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	if cmd, ok := keymap.Dispatch(m.bindings(), key); ok {
		return cmd
	}
	if m.saving {
		// the value is captured at actSave time and the RPC is in
		// flight; further mutations would be silently dropped, so
		// freeze the form until the result lands.
		return nil
	}
	m.form, m.mode = updateFormModal(m.form, m.mode, key)
	return nil
}

func (m *ConfigEditModel) handleAltered(msg ConfigAlteredMsg) {
	m.saving = false
	if msg.Err != nil {
		m.err = msg.Err.Error()
		m.toasts.Push(components.ToastError, "save failed: "+msg.Err.Error())
		return
	}
	m.toasts.Push(components.ToastSuccess, fmt.Sprintf("saved %s = %s", msg.Key, msg.Value))
	m.action.Saved = true
	m.action.Key = msg.Key
	m.action.Back = true
}

// validatedValue returns the value to send to the broker, applying
// type-specific validation (e.g. integer / ratio bounds). Empty input is
// allowed for free-form string keys — some Kafka configs (throttled
// replicas list, etc.) clear when set to an empty string. Numeric and
// enum types always require a value.
func (m *ConfigEditModel) validatedValue() (string, error) {
	fld, _ := m.form.Field(editFieldKey)
	v := strings.TrimSpace(fld.Value)
	if !m.knownDoc {
		return v, nil
	}
	switch m.entry.Type {
	case configcatalog.TypeInteger, configcatalog.TypeByteSize, configcatalog.TypeDuration:
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return "", fmt.Errorf("must be an integer: %q", v)
		}
	case configcatalog.TypeRatio:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return "", fmt.Errorf("must be a ratio: %q", v)
		}
		if f < 0 || f > 1 {
			return "", errors.New("ratio must be between 0 and 1")
		}
	case configcatalog.TypeBoolean:
		if v != "true" && v != "false" {
			return "", errors.New("must be true or false")
		}
	case configcatalog.TypeSelect:
		if !slices.Contains(m.entry.EnumValues, v) {
			return "", fmt.Errorf("must be one of: %s", strings.Join(m.entry.EnumValues, ", "))
		}
	case configcatalog.TypeString:
		// free-form strings — including the empty case — pass straight
		// through so the broker decides what's accepted.
	}
	return v, nil
}

func (m *ConfigEditModel) View() string {
	header := m.styles.HelpTitle.Render(m.Title())
	parts := []string{header}

	if m.knownDoc {
		meta := "type: " + m.entry.Type.String() + " · category: " + m.entry.Category
		parts = append(parts, m.styles.HintLabel.Render(meta))
		if len(m.entry.EnumValues) > 0 {
			parts = append(parts, m.styles.HintLabel.Render("values: "+strings.Join(m.entry.EnumValues, ", ")))
		}
	}

	if m.err != "" {
		parts = append(parts, m.styles.StatusErr.Render(m.err))
	}

	parts = append(parts, "", m.form.View())

	if m.knownDoc && m.entry.Doc != "" {
		parts = append(parts, "", wrap(m.entry.Doc, m.docWidth()))
	}

	parts = append(parts, "", m.styles.HintLabel.Render(m.hintLine()))

	if m.saving {
		parts = append(parts, m.styles.StatusInfo.Render("(saving…)"))
	}
	return strings.Join(parts, "\n")
}

func (m *ConfigEditModel) docWidth() int {
	return max(min(m.width-4, 100), 40)
}

// hintLine renders the bottom keybind hint. The edit form has a single
// field, so the create/clone hint (which advertises tab/shift+tab/↑/↓
// for inter-field navigation) would be misleading — tab/shift+tab are
// no-ops here. We pick keys based on the field kind instead.
func (m *ConfigEditModel) hintLine() string {
	if m.mode == FormInsert {
		return "type to edit  enter/esc — confirm  ctrl+s — save"
	}
	if isPicker(m.form.FocusedField().Kind) {
		return "←/→ — change  enter — pick from list  ctrl+s — save  esc — cancel"
	}
	return "enter — edit  ctrl+s — save  esc — cancel"
}

// ConfigAlteredMsg reports the result of an AlterTopicConfig RPC.
type ConfigAlteredMsg struct {
	Key   string
	Value string
	Err   error
}

func alterConfigCmd(svc Service, topic, key, value string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := svc.AlterTopicConfig(ctx, topic, key, value)
		return ConfigAlteredMsg{Key: key, Value: value, Err: err}
	}
}
