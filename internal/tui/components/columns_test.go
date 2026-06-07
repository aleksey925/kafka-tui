package components_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
)

type colRow struct {
	name string
	size string
}

func testSchema() components.ColumnSchema[colRow] {
	return components.NewColumnSchema([]components.ColumnField[colRow]{
		{Key: "name", Col: components.Column{Title: "Name"}, Cell: func(r colRow) string { return r.name }},
		{Key: "size", Col: components.Column{Title: "Size"}, Cell: func(r colRow) string { return r.size }},
	}, []string{"name", "size"})
}

func TestSchemaResolve__empty__usesDefaults(t *testing.T) {
	// arrange
	schema := testSchema()

	// act
	sel, unknown := schema.Resolve(nil)

	// assert
	assert.Equal(t, []string{"name", "size"}, sel.Keys())
	assert.Empty(t, unknown)
}

func TestSchemaResolve__subsetInOrder__keepsConfiguredOrder(t *testing.T) {
	// arrange
	schema := testSchema()

	// act
	sel, unknown := schema.Resolve([]string{"size", "name"})

	// assert
	assert.Equal(t, []string{"size", "name"}, sel.Keys())
	assert.Empty(t, unknown)
}

func TestSchemaResolve__unknownKeys__droppedAndReported(t *testing.T) {
	// arrange
	schema := testSchema()

	// act
	sel, unknown := schema.Resolve([]string{"name", "bogus", "size"})

	// assert
	assert.Equal(t, []string{"name", "size"}, sel.Keys())
	assert.Equal(t, []string{"bogus"}, unknown)
}

func TestSchemaResolve__allUnknown__fallsBackToDefaults(t *testing.T) {
	// arrange
	schema := testSchema()

	// act
	sel, unknown := schema.Resolve([]string{"bogus", "nope"})

	// assert
	assert.Equal(t, []string{"name", "size"}, sel.Keys())
	assert.Equal(t, []string{"bogus", "nope"}, unknown)
}

func TestSelectionTableColumns__matchesSelectionOrder(t *testing.T) {
	// arrange
	schema := testSchema()
	sel, _ := schema.Resolve([]string{"size", "name"})

	// act
	cols := sel.TableColumns()

	// assert
	assert.Equal(t, []components.Column{{Title: "Size"}, {Title: "Name"}}, cols)
}

func TestSelectionRow__rendersCellsInSelectionOrder(t *testing.T) {
	// arrange
	schema := testSchema()
	sel, _ := schema.Resolve([]string{"size", "name"})

	// act
	row := sel.Row(colRow{name: "topic-a", size: "1.2MB"})

	// assert
	assert.Equal(t, []string{"1.2MB", "topic-a"}, row)
}

func TestSchemaHas__reportsKnownKeys(t *testing.T) {
	// arrange
	schema := testSchema()

	// act / assert
	assert.True(t, schema.Has("name"))
	assert.False(t, schema.Has("bogus"))
}
