package components_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestSendConfirm_PendingByDefault(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders")
	assert.Equal(t, components.SendConfirmPending, c.Result())
}

func TestSendConfirm_YKeyMeansSendAndClose(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders")
	c, _ = c.Update(keyPressMsg("y"))
	assert.Equal(t, components.SendConfirmYesClose, c.Result())
}

func TestSendConfirm_KKeyMeansSendAndKeep(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders")
	c, _ = c.Update(keyPressMsg("k"))
	assert.Equal(t, components.SendConfirmYesKeep, c.Result())
}

func TestSendConfirm_NKeyMeansCancel(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders")
	c, _ = c.Update(keyPressRune('n'))
	assert.Equal(t, components.SendConfirmNo, c.Result())
}

func TestSendConfirm_EscIsCancel(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders")
	c, _ = c.Update(keyPressMsg("esc"))
	assert.Equal(t, components.SendConfirmNo, c.Result())
}

// Enter must NOT commit — that's the anti-accident contract. A reflexive
// enter on the modal is a common muscle-memory mistake; only an explicit
// y/k/n/esc may flip the result.
func TestSendConfirm_EnterIsIgnored(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders")
	c, _ = c.Update(keyPressMsg("enter"))
	assert.Equal(t, components.SendConfirmPending, c.Result())
}

func TestSendConfirm_OtherKeysIgnored(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders")
	c, _ = c.Update(keyPressRune('x'))
	assert.Equal(t, components.SendConfirmPending, c.Result())
}

func TestSendConfirm_Reset(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders")
	c, _ = c.Update(keyPressMsg("y"))
	c.Reset()
	assert.Equal(t, components.SendConfirmPending, c.Result())
}

func TestSendConfirm_ViewSurfacesClusterTopicAndKeys(t *testing.T) {
	c := components.NewSendConfirm("staging", "users.events")
	out := c.View(80, 20)
	assert.Contains(t, out, "Send")
	assert.Contains(t, out, "staging")
	assert.Contains(t, out, "users.events")
	assert.Contains(t, out, "y")
	assert.Contains(t, out, "k")
	assert.Contains(t, out, "esc")
}

func TestSendConfirm_BindingsAdvertiseAllAnswers(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders")
	keys := map[string]struct{}{}
	for _, b := range c.Bindings("Send") {
		for _, k := range b.Keys {
			keys[k] = struct{}{}
		}
	}
	for _, want := range []string{"y", "k", "esc", "n"} {
		_, ok := keys[want]
		assert.True(t, ok, "Bindings must advertise %q", want)
	}
}

func TestWithSendConfirmStyles_Applies(t *testing.T) {
	c := components.NewSendConfirm("staging", "orders", components.WithSendConfirmStyles(theme.DefaultStyles()))
	assert.NotEmpty(t, c.View(40, 0))
}
