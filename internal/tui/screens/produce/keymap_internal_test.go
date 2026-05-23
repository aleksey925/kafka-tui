package produce

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

func TestBindings_WellFormed(t *testing.T) {
	m := New(Options{Topic: "t"})
	assert.NoError(t, keymap.Validate(m.bindings()))
	assert.NoError(t, keymap.Validate(m.normalBindings()))
}
