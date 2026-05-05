package logs

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

func TestBindings_WellFormed(t *testing.T) {
	m := New(Options{Path: filepath.Join(t.TempDir(), "missing.log")})
	assert.NoError(t, keymap.Validate(m.bindings()))

	// follow flag flips the label — table must stay valid in either state.
	m.follow = true
	assert.NoError(t, keymap.Validate(m.bindings()))
}
