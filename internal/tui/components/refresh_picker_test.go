package components_test

import (
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
)

// presets are the canonical preset list — tests reference them by index
// so changes to the preset list are caught here, not lazily later.
func TestRefreshPicker_PresetsAreCanonical(t *testing.T) {
	assert.Equal(t, []time.Duration{
		0,
		1 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		1 * time.Minute,
		5 * time.Minute,
	}, components.Presets())
}

func TestRefreshPicker_DigitJumpsAndConfirms(t *testing.T) {
	p := components.NewRefreshPicker(0)

	_, _ = p.Update(key("5"))

	d, ok := p.Selected()
	require.True(t, ok, "digit must confirm in one keystroke")
	assert.Equal(t, 30*time.Second, d, "digit 5 = 5th preset = 30s")
}

func TestRefreshPicker_EnterOnFocusedPresetConfirms(t *testing.T) {
	p := components.NewRefreshPicker(30 * time.Second) // cursor lands on the 30s preset

	_, _ = p.Update(key("enter"))

	d, ok := p.Selected()
	require.True(t, ok)
	assert.Equal(t, 30*time.Second, d)
}

func TestRefreshPicker_DownArrowAdvancesCursor(t *testing.T) {
	p := components.NewRefreshPicker(0) // cursor at Manual (idx 0)

	_, _ = p.Update(key("j"))
	_, _ = p.Update(key("enter"))

	d, _ := p.Selected()
	assert.Equal(t, 1*time.Second, d, "j must move to next preset (1s)")
}

func TestRefreshPicker_UpArrowWrapsAround(t *testing.T) {
	p := components.NewRefreshPicker(0) // cursor at Manual (idx 0)

	_, _ = p.Update(key("k"))
	_, _ = p.Update(key("enter"))

	d, _ := p.Selected()
	assert.Equal(t, 5*time.Minute, d, "k from first must wrap to last preset (5m)")
}

func TestRefreshPicker_EscCancels(t *testing.T) {
	p := components.NewRefreshPicker(0)

	_, _ = p.Update(key("esc"))

	_, ok := p.Selected()
	assert.False(t, ok, "esc must not confirm")
	assert.True(t, p.Canceled())
}

func TestRefreshPicker_TabSwitchesFocusToInput(t *testing.T) {
	p := components.NewRefreshPicker(0)
	// in list focus, digit "1" would jump+confirm.
	_, _ = p.Update(key("tab"))
	// now in input focus, "1" must become text, not a jump.
	_, _ = p.Update(key("1"))

	_, ok := p.Selected()
	assert.False(t, ok, "tab→input must redirect digits to the buffer, not the list")
}

func TestRefreshPicker_EnterInInputValidatesAndConfirms(t *testing.T) {
	p := components.NewRefreshPicker(0)
	_, _ = p.Update(key("tab"))
	for _, r := range "2m" {
		_, _ = p.Update(key(string(r)))
	}

	_, _ = p.Update(key("enter"))

	d, ok := p.Selected()
	require.True(t, ok)
	assert.Equal(t, 2*time.Minute, d)
}

func TestRefreshPicker_CustomPrefilledWhenCurrentIsCustom(t *testing.T) {
	// arrange — 2m30s isn't a preset, so the input should be prefilled.
	p := components.NewRefreshPicker(2*time.Minute + 30*time.Second)

	// act — tab to input and submit without typing.
	_, _ = p.Update(key("tab"))
	_, _ = p.Update(key("enter"))

	// assert — the prefilled value parses and confirms.
	d, ok := p.Selected()
	require.True(t, ok)
	assert.Equal(t, 2*time.Minute+30*time.Second, d)
}

func TestRefreshPicker_RejectsInvalidCustomInput(t *testing.T) {
	cases := []struct{ name, input string }{
		{"empty", ""},
		{"sub-second", "100ms"},
		{"micro", "5µs"},
		{"days", "2d"},
		{"junk", "foo"},
		{"naked number", "30"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := components.NewRefreshPicker(0)
			_, _ = p.Update(key("tab"))
			for _, r := range tc.input {
				_, _ = p.Update(key(string(r)))
			}

			_, _ = p.Update(key("enter"))

			_, ok := p.Selected()
			assert.False(t, ok, "invalid input %q must not confirm", tc.input)
			assert.False(t, p.Canceled())
		})
	}
}

func TestRefreshPicker_RejectsBelowMinimum(t *testing.T) {
	p := components.NewRefreshPicker(0)
	_, _ = p.Update(key("tab"))
	for _, r := range "500ms" { // below 1s minimum (and disallowed unit anyway)
		_, _ = p.Update(key(string(r)))
	}

	_, _ = p.Update(key("enter"))

	_, ok := p.Selected()
	assert.False(t, ok)
}

// Compound durations like 1m30s are valid Go duration strings and use only
// allowed units (m, s).
func TestRefreshPicker_AcceptsCompoundDuration(t *testing.T) {
	p := components.NewRefreshPicker(0)
	_, _ = p.Update(key("tab"))
	for _, r := range "1m30s" {
		_, _ = p.Update(key(string(r)))
	}

	_, _ = p.Update(key("enter"))

	d, ok := p.Selected()
	require.True(t, ok)
	assert.Equal(t, 90*time.Second, d)
}

func TestRefreshPicker_PasteRoutedToInputZone(t *testing.T) {
	p := components.NewRefreshPicker(0)
	_, _ = p.Update(key("tab")) // focus input
	_, _ = p.Update(tea.PasteMsg{Content: "45s"})

	_, _ = p.Update(key("enter"))

	d, ok := p.Selected()
	require.True(t, ok)
	assert.Equal(t, 45*time.Second, d)
}

// Paste in list focus must be a no-op so digit/list semantics stay intact.
func TestRefreshPicker_PasteIgnoredInListFocus(t *testing.T) {
	p := components.NewRefreshPicker(0)
	_, _ = p.Update(tea.PasteMsg{Content: "45s"})
	// switching to input now must show empty buffer — paste didn't sneak in.
	_, _ = p.Update(key("tab"))
	_, _ = p.Update(key("enter"))

	_, ok := p.Selected()
	assert.False(t, ok, "empty input must not confirm; paste should not have populated it")
}

func TestRefreshPicker_ResetClearsConfirmedAndCanceled(t *testing.T) {
	p := components.NewRefreshPicker(0)
	_, _ = p.Update(key("enter"))
	_, ok := p.Selected()
	require.True(t, ok)

	p.Reset()

	_, ok = p.Selected()
	assert.False(t, ok)
	assert.False(t, p.Canceled())
}

func TestRefreshPicker_BindingsListPresetsOnlyWhenListFocused(t *testing.T) {
	p := components.NewRefreshPicker(0)
	listBs := p.Bindings("Refresh")
	hasNextPreset := false
	for _, b := range listBs {
		if b.Label == "next preset" {
			hasNextPreset = true
			break
		}
	}
	assert.True(t, hasNextPreset, "list-focus bindings must include navigation entries")

	_, _ = p.Update(key("tab"))
	inputBs := p.Bindings("Refresh")
	for _, b := range inputBs {
		assert.NotEqual(t, "next preset", b.Label,
			"input focus must not advertise list navigation — those keys feed the buffer")
	}
}

// helpers

func key(s string) tea.KeyPressMsg {
	switch s {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEsc}
	case "tab":
		return tea.KeyPressMsg{Code: tea.KeyTab}
	case "shift+tab":
		return tea.KeyPressMsg{Code: tea.KeyTab, Mod: tea.ModShift}
	}
	runes := []rune(s)
	if len(runes) == 1 {
		return tea.KeyPressMsg{Code: runes[0], Text: s}
	}
	panic("unknown key: " + s)
}
