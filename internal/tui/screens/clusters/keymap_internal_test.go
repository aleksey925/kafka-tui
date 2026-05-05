package clusters

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

func TestBindings_WellFormed(t *testing.T) {
	m := New(Options{})
	assert.NoError(t, keymap.Validate(m.listBindings()))
	assert.NoError(t, keymap.Validate(m.editChooserBindings()))
}
