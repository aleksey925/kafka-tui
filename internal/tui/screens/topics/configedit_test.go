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

	driveEdit(t, m, m.Update(keyPress("ctrl+s")))

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

	driveEdit(t, m, m.Update(keyPress("ctrl+s")))

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

	driveEdit(t, m, m.Update(keyPress("ctrl+s")))
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

	driveEdit(t, m, m.Update(keyPress("ctrl+s")))

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
	// after Ctrl+S must be no-ops.
	_ = m.Update(keyPress("ctrl+s"))
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
	cmd := m.Update(keyPress("ctrl+s"))
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

	driveEdit(t, m, m.Update(keyPress("ctrl+s")))

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

	driveEdit(t, m, m.Update(keyPress("ctrl+s")))
	assert.Empty(t, svc.Altered(), "numeric configs must still reject empty input")
	assert.Contains(t, m.View(), "must be an integer")
}
