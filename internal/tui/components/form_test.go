package components_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
)

func TestForm_TabFocusCyclesForwardAndBackward(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "key", Label: "Key", Kind: components.FieldText},
		{Key: "value", Label: "Value", Kind: components.FieldText},
		{Key: "compression", Label: "Compression", Kind: components.FieldDropdown,
			Value: "none", Options: []string{"none", "gzip", "snappy"}},
	})

	assert.Equal(t, 0, f.Focused())

	f, _ = f.Update(keyPressMsg("tab"))
	assert.Equal(t, 1, f.Focused())

	f, _ = f.Update(keyPressMsg("tab"))
	assert.Equal(t, 2, f.Focused())

	f, _ = f.Update(keyPressMsg("tab"))
	assert.Equal(t, 0, f.Focused()) // wraps

	f, _ = f.Update(keyPressMsg("shift+tab"))
	assert.Equal(t, 2, f.Focused())
}

func TestForm_TextFieldEditing(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "key", Label: "Key", Kind: components.FieldText},
	})
	for _, ch := range "abc" {
		f, _ = f.Update(keyPressRune(ch))
	}
	got, _ := f.Field("key")
	assert.Equal(t, "abc", got.Value)

	f, _ = f.Update(keyPressMsg("backspace"))
	got, _ = f.Field("key")
	assert.Equal(t, "ab", got.Value)
}

func TestForm_TextFieldEnterIgnoredSinglelineButAddsNewlineForTextarea(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText},
		{Key: "v", Label: "Value", Kind: components.FieldTextarea},
	})
	for _, ch := range "ab" {
		f, _ = f.Update(keyPressRune(ch))
	}
	f, _ = f.Update(keyPressMsg("enter"))
	got, _ := f.Field("k")
	assert.Equal(t, "ab", got.Value) // newline NOT appended for FieldText

	f.FocusKey("v")
	for _, ch := range "first" {
		f, _ = f.Update(keyPressRune(ch))
	}
	f, _ = f.Update(keyPressMsg("enter"))
	for _, ch := range "second" {
		f, _ = f.Update(keyPressRune(ch))
	}
	got, _ = f.Field("v")
	assert.Equal(t, "first\nsecond", got.Value)
}

func TestForm_DropdownNavigatesWithJK(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "compression", Label: "Compression", Kind: components.FieldDropdown,
			Value: "none", Options: []string{"none", "gzip", "snappy", "lz4"}},
	})
	f, _ = f.Update(keyPressMsg("j"))
	got, _ := f.Field("compression")
	assert.Equal(t, "gzip", got.Value)

	f, _ = f.Update(keyPressMsg("j"))
	f, _ = f.Update(keyPressMsg("j"))
	got, _ = f.Field("compression")
	assert.Equal(t, "lz4", got.Value)

	f, _ = f.Update(keyPressMsg("j"))
	got, _ = f.Field("compression")
	assert.Equal(t, "none", got.Value) // wraps

	f, _ = f.Update(keyPressMsg("k"))
	got, _ = f.Field("compression")
	assert.Equal(t, "lz4", got.Value)
}

func TestForm_ListAddTypeDelete(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "headers", Label: "Headers", Kind: components.FieldList},
	})
	f, _ = f.Update(keyPressMsg("ctrl+a"))
	for _, ch := range "key1" {
		f, _ = f.Update(keyPressRune(ch))
	}
	f, _ = f.Update(keyPressMsg("ctrl+a"))
	for _, ch := range "key2" {
		f, _ = f.Update(keyPressRune(ch))
	}

	got, _ := f.Field("headers")
	assert.Equal(t, []string{"key1", "key2"}, got.List)

	// move cursor up and delete
	f, _ = f.Update(keyPressMsg("up"))
	f, _ = f.Update(keyPressMsg("ctrl+d"))

	got, _ = f.Field("headers")
	assert.Equal(t, []string{"key2"}, got.List)
}

func TestForm_ListBackspaceEditsCurrentEntry(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList, List: []string{"abc"}},
	})
	f, _ = f.Update(keyPressMsg("backspace"))
	got, _ := f.Field("h")
	assert.Equal(t, []string{"ab"}, got.List)
}

func TestForm_SetValueAndSetList(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "K", Kind: components.FieldText},
		{Key: "h", Label: "H", Kind: components.FieldList},
	})
	f.SetValue("k", "preset")
	f.SetList("h", []string{"a", "b"})

	got, _ := f.Field("k")
	assert.Equal(t, "preset", got.Value)

	got, _ = f.Field("h")
	assert.Equal(t, []string{"a", "b"}, got.List)
}

func TestForm_FocusKey(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "a", Kind: components.FieldText},
		{Key: "b", Kind: components.FieldText},
	})
	f.FocusKey("b")
	assert.Equal(t, 1, f.Focused())
	assert.Equal(t, "b", f.FocusedField().Key)
}

func TestForm_ViewIncludesAllLabels(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "key", Label: "Key", Kind: components.FieldText, Value: "k"},
		{Key: "value", Label: "Value", Kind: components.FieldTextarea, Value: "v"},
		{Key: "compression", Label: "Compression", Kind: components.FieldDropdown,
			Value: "gzip", Options: []string{"none", "gzip"}},
		{Key: "headers", Label: "Headers", Kind: components.FieldList, List: []string{"x"}},
	})
	out := f.View()
	for _, label := range []string{"Key", "Value", "Compression", "Headers"} {
		assert.Contains(t, out, label)
	}
	assert.Contains(t, out, "gzip")
	assert.Contains(t, out, "x")
}

func TestFieldKindString(t *testing.T) {
	assert.Equal(t, "text", components.FieldText.String())
	assert.Equal(t, "dropdown", components.FieldDropdown.String())
	assert.Equal(t, "list", components.FieldList.String())
	assert.Equal(t, "textarea", components.FieldTextarea.String())
}
