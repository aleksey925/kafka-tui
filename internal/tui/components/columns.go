package components

// ColumnField declares one configurable column: the key used in config, the
// table column spec, and how to render a cell for a row of type T.
type ColumnField[T any] struct {
	Key  string
	Col  Column
	Cell func(T) string
}

// ColumnSchema is every column a screen can display, in canonical order, plus
// the default selection used when config names none. One schema per list screen
// is the single source for column resolution, validation, and cell rendering, so
// a new screen wires columns the same way instead of re-deriving the contract.
type ColumnSchema[T any] struct {
	fields   map[string]ColumnField[T]
	defaults []string
}

// NewColumnSchema builds a schema from its fields and the default key selection.
func NewColumnSchema[T any](fields []ColumnField[T], defaults []string) ColumnSchema[T] {
	byKey := make(map[string]ColumnField[T], len(fields))
	for _, f := range fields {
		byKey[f.Key] = f
	}
	return ColumnSchema[T]{fields: byKey, defaults: defaults}
}

// Has reports whether key names a column the schema knows.
func (s ColumnSchema[T]) Has(key string) bool {
	_, ok := s.fields[key]
	return ok
}

// Resolve turns a configured key list into a ColumnSelection. Empty input
// selects the defaults. Unknown keys are dropped and returned so the caller can
// warn; if no configured key survives, the defaults are used so the screen
// always renders columns rather than an empty header.
func (s ColumnSchema[T]) Resolve(configured []string) (ColumnSelection[T], []string) {
	keys := configured
	if len(keys) == 0 {
		keys = s.defaults
	}
	fields := make([]ColumnField[T], 0, len(keys))
	var unknown []string
	for _, k := range keys {
		f, ok := s.fields[k]
		if !ok {
			unknown = append(unknown, k)
			continue
		}
		fields = append(fields, f)
	}
	if len(fields) == 0 {
		for _, k := range s.defaults {
			if f, ok := s.fields[k]; ok {
				fields = append(fields, f)
			}
		}
	}
	return ColumnSelection[T]{fields: fields}, unknown
}

// ColumnSelection is a resolved, validated ordered subset of a schema's columns.
type ColumnSelection[T any] struct {
	fields []ColumnField[T]
}

// TableColumns returns the column specs in selection order, for NewTable.
func (sel ColumnSelection[T]) TableColumns() []Column {
	out := make([]Column, len(sel.fields))
	for i, f := range sel.fields {
		out[i] = f.Col
	}
	return out
}

// Row renders one data row's cells in selection order.
func (sel ColumnSelection[T]) Row(row T) []string {
	out := make([]string, len(sel.fields))
	for i, f := range sel.fields {
		out[i] = f.Cell(row)
	}
	return out
}

// Keys returns the selected column keys in order.
func (sel ColumnSelection[T]) Keys() []string {
	out := make([]string, len(sel.fields))
	for i, f := range sel.fields {
		out[i] = f.Key
	}
	return out
}
