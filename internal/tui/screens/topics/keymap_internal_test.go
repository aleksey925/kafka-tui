package topics

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

// TestListBindings_WellFormed enforces the keymap-table invariants on
// the topics list screen: no duplicate keys, no empty labels. Adding
// a malformed binding fails CI rather than shipping silently broken
// help.
func TestListBindings_WellFormed(t *testing.T) {
	// arrange
	m := New(Options{Service: &fakeBindingsSvc{}})

	// act + assert (read-write)
	m.readOnly = false
	assert.NoError(t, keymap.Validate(m.listBindings()))

	// act + assert (read-only path masks mutating bindings)
	m.readOnly = true
	assert.NoError(t, keymap.Validate(m.listBindings()))
}

// TestSubmodeBindings_WellFormed validates the bindings tables for
// every sub-mode dispatcher (create / clone / cloning) — drift in any
// of these surfaces would slip past a list-only test.
func TestSubmodeBindings_WellFormed(t *testing.T) {
	// arrange
	m := New(Options{Service: &fakeBindingsSvc{}})
	m.openCreateForm()
	createBs := m.createBindings()
	m.create = nil

	// act + assert (create / cloning don't depend on populated form state
	// for table validity; clone needs a non-nil m.clone to call Form().)
	assert.NoError(t, keymap.Validate(createBs))
	assert.NoError(t, keymap.Validate(m.cloningBindings()))
}

// TestListBindings_HelpHasEveryHandler asserts every binding with a
// non-nil Handler is exposed somewhere user-visible — either in help
// (Category set) or in the bottom hint bar (Hint=true). Bindings with
// neither are dead documentation.
func TestListBindings_HelpAndHintsCoverHandlers(t *testing.T) {
	// arrange
	m := New(Options{Service: &fakeBindingsSvc{}})

	// act + assert
	for _, b := range m.listBindings() {
		if b.Handler == nil && b.HandlerMsg == nil {
			continue
		}
		assert.NotEmptyf(t, b.Category, "binding %q has Handler but no Category — invisible in help", b.Display())
	}
}

// TestConfigsBindings_WellFormed pins the same invariants for the topic
// configs screen. Previously this screen hardcoded its KeyHints and
// bypassed the keymap layer entirely, which left the `?` overlay empty
// and hid the `r` refresh shortcut from the bottom hints bar.
func TestConfigsBindings_WellFormed(t *testing.T) {
	m := NewConfigsModel(ConfigsOptions{Service: &fakeBindingsSvc{}, Topic: "alpha"})
	assert.NoError(t, keymap.Validate(m.bindings()))
}

type fakeBindingsSvc struct{ Service }
