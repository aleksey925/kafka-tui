package keymap_test

import (
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

func TestDispatch_MatchesAliasAndCallsHandler(t *testing.T) {
	// arrange
	called := ""
	bs := []keymap.Binding{
		{Keys: []string{"a"}, Label: "alpha", Handler: func() tea.Cmd { called = "a"; return nil }},
		{Keys: []string{"b", "B"}, Label: "bravo", Handler: func() tea.Cmd { called = "b"; return nil }},
	}

	// act
	_, ok := keymap.Dispatch(bs, key("B"))

	// assert
	assert.True(t, ok)
	assert.Equal(t, "b", called)
}

func TestDispatch_UnknownKeyReturnsFalse(t *testing.T) {
	// arrange
	bs := []keymap.Binding{
		{Keys: []string{"a"}, Label: "alpha", Handler: func() tea.Cmd { return nil }},
	}

	// act
	cmd, ok := keymap.Dispatch(bs, key("z"))

	// assert
	assert.False(t, ok)
	assert.Nil(t, cmd)
}

func TestDispatch_AdvertiseOnlyFallsThrough(t *testing.T) {
	// advertise-only entries (no handler) must not claim the keystroke —
	// the caller relies on this to forward unhandled keys to a fallback.
	// arrange
	bs := []keymap.Binding{
		{Keys: []string{"a"}, Label: "advertise only", Category: "X"},
	}

	// act
	cmd, ok := keymap.Dispatch(bs, key("a"))

	// assert
	assert.False(t, ok)
	assert.Nil(t, cmd)
}

func TestDispatch_HandlerMsgReceivesOriginalEvent(t *testing.T) {
	// arrange
	var got tea.KeyPressMsg
	bs := []keymap.Binding{
		{Keys: []string{"a"}, Label: "alpha", HandlerMsg: func(msg tea.KeyPressMsg) tea.Cmd {
			got = msg
			return nil
		}},
	}
	in := key("a")

	// act
	_, ok := keymap.Dispatch(bs, in)

	// assert
	assert.True(t, ok)
	assert.Equal(t, in.String(), got.String())
}

func TestDispatch_HandlerMsgWinsOverHandler(t *testing.T) {
	// arrange — both set: HandlerMsg must win so the original event reaches
	// the implementation that wants it.
	called := ""
	bs := []keymap.Binding{
		{
			Keys:       []string{"a"},
			Label:      "both",
			Handler:    func() tea.Cmd { called = "plain"; return nil },
			HandlerMsg: func(_ tea.KeyPressMsg) tea.Cmd { called = "msg"; return nil },
		},
	}

	// act
	_, ok := keymap.Dispatch(bs, key("a"))

	// assert
	assert.True(t, ok)
	assert.Equal(t, "msg", called)
}

func TestValidate_DetectsDuplicateKey(t *testing.T) {
	// arrange
	bs := []keymap.Binding{
		{Keys: []string{"a"}, Label: "alpha"},
		{Keys: []string{"a"}, Label: "again"},
	}

	// act
	err := keymap.Validate(bs)

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate key a")
}

func TestValidate_DetectsEmptyLabel(t *testing.T) {
	// arrange
	bs := []keymap.Binding{
		{Keys: []string{"a"}, Label: ""},
	}

	// act
	err := keymap.Validate(bs)

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty Label")
}

func TestValidate_DetectsEmptyKeys(t *testing.T) {
	// arrange
	bs := []keymap.Binding{
		{Keys: nil, Label: "no keys"},
	}

	// act
	err := keymap.Validate(bs)

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty Keys")
}

func TestValidate_HappyPath(t *testing.T) {
	// arrange
	bs := []keymap.Binding{
		{Keys: []string{"a"}, Label: "alpha"},
		{Keys: []string{"b", "B"}, Label: "bravo"},
	}

	// act
	err := keymap.Validate(bs)

	// assert
	assert.NoError(t, err)
}

func TestBinding_Display(t *testing.T) {
	// arrange
	cases := []struct {
		name        string
		keys        []string
		displayKeys []string
		expected    string
	}{
		{"single", []string{"a"}, nil, "a"},
		{"alias", []string{"enter", "m"}, nil, "enter / m"},
		{"empty", nil, nil, ""},
		{"display_keys_override_to_subset", []string{"+", "_", "shift++", "shift+-"}, []string{"+", "_"}, "+ / _"},
		{"display_keys_override_to_single", []string{"space", " "}, []string{"space"}, "space"},
		{"display_keys_empty_falls_back_to_keys", []string{"a", "b"}, []string{}, "a / b"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := keymap.Binding{Keys: tc.keys, DisplayKeys: tc.displayKeys, Label: "x"}
			assert.Equal(t, tc.expected, b.Display())
		})
	}
}

func TestValidate_DetectsDisplayKeyNotInKeys(t *testing.T) {
	// arrange — DisplayKeys must be a subset of Keys; a typo there would
	// render a hint that nothing dispatches.
	bs := []keymap.Binding{
		{Keys: []string{"+", "_"}, DisplayKeys: []string{"+", "="}, Label: "fullscreen"},
	}

	// act
	err := keymap.Validate(bs)

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DisplayKeys = not in Keys")
}

func TestValidate_DisplayKeysSubsetOfKeysIsHappy(t *testing.T) {
	// arrange
	bs := []keymap.Binding{
		{Keys: []string{"+", "_", "shift++", "shift+-"}, DisplayKeys: []string{"+", "_"}, Label: "fullscreen"},
	}

	// act
	err := keymap.Validate(bs)

	// assert
	assert.NoError(t, err)
}

// key builds a tea.KeyPressMsg whose String() returns the given printable
// token. Sufficient for Dispatch tests that only care about string match.
func key(s string) tea.KeyPressMsg {
	return tea.KeyPressMsg{Text: s, Code: rune(s[0])}
}
