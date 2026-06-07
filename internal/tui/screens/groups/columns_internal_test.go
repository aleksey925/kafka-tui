package groups

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultColumns__allKnownToSchema(t *testing.T) {
	// arrange
	schema := (&Model{}).columnSchema()

	// act / assert
	for _, key := range DefaultColumns {
		assert.Truef(t, schema.Has(key), "default column %q is missing from the schema", key)
	}
}
