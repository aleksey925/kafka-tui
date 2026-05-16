package groups

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

// TestBindings_WellFormed pins keymap-table invariants for groups
// list, detail, and the per-step reset flow.
func TestBindings_WellFormed(t *testing.T) {
	// arrange
	m := New(Options{Service: &keymapTestService{}})
	d := &DetailModel{}
	r := &ResetModel{}

	// act + assert: list mode (rw / ro)
	for _, ro := range []bool{false, true} {
		m.readOnly = ro
		require.NoErrorf(t, keymap.Validate(m.listBindings()), "list readOnly=%v", ro)
	}

	// detail (rw / ro)
	for _, ro := range []bool{false, true} {
		d.readOnly = ro
		require.NoErrorf(t, keymap.Validate(d.bindings()), "detail readOnly=%v", ro)
	}

	// reset flow steps
	for _, step := range []ResetStep{StepStrategy, StepParams, StepPreview} {
		r.step = step
		require.NoErrorf(t, keymap.Validate(r.bindings()), "reset step=%v", step)
	}
}

type keymapTestService struct{ Service }
