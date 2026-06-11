package topics_test

import (
	"errors"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
)

func driveEdit(t *testing.T, m *topics.ConfigEditModel, cmd tea.Cmd) {
	t.Helper()
	queue := []tea.Cmd{cmd}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		if next == nil {
			continue
		}
		msg := next()
		if msg == nil {
			continue
		}
		if batch, ok := msg.(tea.BatchMsg); ok {
			queue = append(queue, batch...)
			continue
		}
		queue = append(queue, m.Update(msg))
	}
}

func TestConfigEdit_SelectFieldForEnumKey(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "compression.type",
		CurrentValue: "producer",
	})

	field := m.Form().FocusedField()
	assert.Equal(t, components.FieldSegmented, field.Kind)
	assert.Contains(t, field.Options, "gzip")
	assert.Equal(t, "producer", field.Value)
}

func TestConfigEdit_BooleanFieldForBooleanKey(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "preallocate",
		CurrentValue: "false",
	})

	field := m.Form().FocusedField()
	assert.Equal(t, components.FieldSegmented, field.Kind)
	assert.Equal(t, []string{"true", "false"}, field.Options)
}

func TestConfigEdit_TextFieldForUnknownKey(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "vendor.unknown",
		CurrentValue: "x",
	})

	field := m.Form().FocusedField()
	assert.Equal(t, components.FieldText, field.Kind)
}

// saveAndConfirm presses `s` to open the save confirm and answers `y`.
// Returns the Cmd from the confirm answer (the AlterTopicConfig dispatch
// on success, nil if the modal didn't open). Use [confirmYes] when the
// confirm is already open.
func saveAndConfirm(m *topics.ConfigEditModel) tea.Cmd {
	_ = m.Update(keyPress("s"))
	if !m.ConfirmOpen() {
		return nil
	}
	return m.Update(keyPress("y"))
}

func TestConfigEdit_SaveCallsServiceAndYieldsBack(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "retention.ms",
		CurrentValue: "60000",
	})

	// retention.ms is integer — overwrite via INSERT mode.
	_ = m.Update(keyPress("enter"))
	require.Equal(t, topics.FormInsert, m.Mode())
	for _, r := range "12000" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("esc"))
	require.Equal(t, topics.FormNormal, m.Mode())

	driveEdit(t, m, saveAndConfirm(m))

	a := m.ConsumeAction()
	assert.True(t, a.Saved)
	assert.True(t, a.Back)
	assert.Equal(t, "retention.ms", a.Key)

	calls := svc.Altered()
	require.Len(t, calls, 1)
	assert.Equal(t, "alpha", calls[0].topic)
	assert.Equal(t, "retention.ms", calls[0].key)
	// the form is initialized with "60000" and we appended "12000".
	assert.Equal(t, "6000012000", calls[0].value)
}

func TestConfigEdit_IntegerValidation(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "retention.ms",
		CurrentValue: "abc",
	})

	_ = m.Update(keyPress("s"))

	assert.False(t, m.ConfirmOpen(), "validation error must abort before opening the modal")
	assert.Empty(t, svc.Altered(), "broker call must be skipped on validation failure")
	assert.False(t, m.ConsumeAction().Saved)
	assert.Contains(t, m.View(), "must be an integer")
}

func TestConfigEdit_SelectValidation(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "compression.type",
		CurrentValue: "lz4",
	})

	driveEdit(t, m, saveAndConfirm(m))
	assert.True(t, m.ConsumeAction().Saved)

	calls := svc.Altered()
	require.Len(t, calls, 1)
	assert.Equal(t, "lz4", calls[0].value)
}

func TestConfigEdit_BrokerErrorSurfaced(t *testing.T) {
	svc := newFakeService(nil, nil)
	svc.alterErr = errors.New("policy violation")

	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "compression.type",
		CurrentValue: "lz4",
	})

	driveEdit(t, m, saveAndConfirm(m))

	a := m.ConsumeAction()
	assert.False(t, a.Saved)
	assert.False(t, a.Back, "stay on the form so the user can retry")

	flash, ok := m.LatestFlash()
	require.True(t, ok)
	assert.Contains(t, flash.Message, "policy violation")
}

func TestConfigEdit_FormFrozenWhileSaving(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "compression.type",
		CurrentValue: "lz4",
	})

	// segmented field — left/right would normally cycle; pressing them
	// while the RPC is in flight must be no-ops.
	_ = saveAndConfirm(m)
	require.True(t, m.Saving())
	before := m.Form().FocusedField().Value

	_ = m.Update(keyPress("right"))
	_ = m.Update(keyPressRune('h'))
	assert.Equal(t, before, m.Form().FocusedField().Value)
}

func TestConfigEdit_EscBlockedWhileSaving(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "compression.type",
		CurrentValue: "lz4",
	})

	// kick off a save but don't run the resulting cmd — the screen is
	// now in `saving=true` with the broker call hypothetically pending.
	cmd := saveAndConfirm(m)
	require.NotNil(t, cmd, "save must enqueue an RPC")
	require.True(t, m.Saving())

	// esc must NOT request a pop while saving — otherwise the user
	// thinks they canceled but the broker may still apply the change.
	_ = m.Update(keyPress("esc"))
	assert.False(t, m.ConsumeAction().Back, "esc must be suppressed while saving")
	assert.True(t, m.HasOverlay(), "host must treat saving as an overlay so its esc fallback can't pop the screen")

	flash, ok := m.LatestFlash()
	require.True(t, ok)
	assert.Contains(t, flash.Message, "save in progress")
}

func TestConfigEdit_EscWithoutOverlayRequestsBack(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "compression.type",
		CurrentValue: "producer",
	})

	_ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Back)
}

func TestConfigEdit_EmptyValueAcceptedForFreeFormString(t *testing.T) {
	svc := newFakeService(nil, nil)
	// throttled-replicas configs are LIST-as-STRING and the broker
	// clears them when set to "" — the form must let that through.
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "leader.replication.throttled.replicas",
		CurrentValue: "",
	})

	driveEdit(t, m, saveAndConfirm(m))

	calls := svc.Altered()
	require.Len(t, calls, 1)
	assert.Empty(t, calls[0].value)
}

func TestConfigEdit_IntegerRejectsEmpty(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "retention.ms",
		CurrentValue: "",
	})

	_ = m.Update(keyPress("s"))
	assert.False(t, m.ConfirmOpen(), "validation error must abort before opening the modal")
	assert.Empty(t, svc.Altered(), "numeric configs must still reject empty input")
	assert.Contains(t, m.View(), "must be an integer")
}

func TestConfigEdit_PasteInNormalLandsValueAndStaysNormal(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "retention.ms",
		CurrentValue: "60000",
	})
	require.Equal(t, topics.FormNormal, m.Mode())

	_ = m.Update(tea.PasteMsg{Content: "123"})

	got := m.Form().FocusedField()
	assert.Contains(t, got.Value, "123", "paste must land in the focused value field")
	assert.Equal(t, topics.FormNormal, m.Mode(), "paste must not cross NORMAL into INSERT")
}

func TestConfigEdit_PasteIsDroppedWhileConfirmOpen(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "leader.replication.throttled.replicas",
		CurrentValue: "before",
	})

	_ = m.Update(keyPress("s"))
	require.True(t, m.ConfirmOpen())

	// paste-while-modal-open used to overwrite the value the user was
	// being asked to confirm; the broker would have applied the leaked
	// content instead of the displayed one.
	_ = m.Update(tea.PasteMsg{Content: "leak"})

	got := m.Form().FocusedField()
	assert.Equal(t, "before", got.Value, "paste must not mutate the value while the confirm owns input")
	assert.True(t, m.ConfirmOpen())
}

func TestConfigEdit_ConfirmNoDismissesWithoutSaving(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "compression.type",
		CurrentValue: "lz4",
	})

	_ = m.Update(keyPress("s"))
	require.True(t, m.ConfirmOpen())

	_ = m.Update(keyPress("n"))
	assert.False(t, m.ConfirmOpen(), "n must dismiss the confirm")
	assert.Empty(t, svc.Altered(), "n must not dispatch the RPC")
	assert.False(t, m.Saving())
}

func TestConfigEdit_SInInsertIsLiteral(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      svc,
		Topic:        "alpha",
		Key:          "leader.replication.throttled.replicas",
		CurrentValue: "",
	})
	_ = m.Update(keyPress("enter")) // INSERT on the value text field
	require.Equal(t, topics.FormInsert, m.Mode())

	_ = m.Update(keyPressRune('s'))

	assert.False(t, m.ConfirmOpen(), "s in INSERT must be typed, not open the save confirm")
	got := m.Form().FocusedField()
	assert.Equal(t, "s", got.Value)
}
