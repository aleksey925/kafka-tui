package messages

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

// TestBindings_WellFormed pins the keymap-table invariants for every
// messages sub-mode. A malformed table (duplicate key, empty label)
// must fail CI rather than ship broken help.
func TestBindings_WellFormed(t *testing.T) {
	// arrange
	m := New(Options{Topic: "t"})

	// act + assert
	// drive seek into stageInput for the input-mode binding slice.
	m.openSeek()
	seekMenu := m.seekBindings()
	m.seekPopup.stage = stageInput
	m.seekPopup.chosen = SeekFromOffset
	m.seekPopup.form = m.buildSeekForm(SeekFromOffset)
	seekInput := m.seekBindings()
	m.closeSeek()

	for _, modes := range []struct {
		name string
		bs   []keymap.Binding
	}{
		{"list (rw)", listFor(m, false)},
		{"list (ro)", listFor(m, true)},
		{"seek menu", seekMenu},
		{"seek input", seekInput},
		{"partitions", m.partitionsBindings()},
		{"smart filter", m.smartFilterBindings()},
	} {
		t.Run(modes.name, func(t *testing.T) {
			assert.NoError(t, keymap.Validate(modes.bs))
		})
	}
}

func TestDetailBindings_WellFormed(t *testing.T) {
	// arrange
	d := &DetailModel{}

	// act + assert (read-write)
	d.readOnly = false
	assert.NoError(t, keymap.Validate(d.bindings()))

	// act + assert (read-only path)
	d.readOnly = true
	assert.NoError(t, keymap.Validate(d.bindings()))
}

func listFor(m *Model, readOnly bool) []keymap.Binding {
	prev := m.readOnly
	m.readOnly = readOnly
	defer func() { m.readOnly = prev }()
	return m.listBindings()
}
