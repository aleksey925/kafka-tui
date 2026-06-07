package messages

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
)

func TestDefaultColumns__allKnownToSchema(t *testing.T) {
	// arrange
	schema := (&Model{}).columnSchema()

	// act / assert
	for _, key := range DefaultColumns {
		assert.Truef(t, schema.Has(key), "default column %q is missing from the schema", key)
	}
}

func TestUnknownColumn__droppedAndWarned(t *testing.T) {
	// arrange / act
	m := New(Options{Columns: []string{"timestamp", "bogus", "key"}})

	// assert
	assert.Equal(t, []string{"timestamp", "key"}, m.cols.Keys())
	toast, ok := m.LatestFlash()
	assert.True(t, ok)
	assert.Equal(t, components.ToastWarning, toast.Level)
	assert.Contains(t, toast.Message, "bogus")
}
