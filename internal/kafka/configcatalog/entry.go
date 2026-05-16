// Package configcatalog provides bundled metadata (category, type,
// documentation, enum values) for Kafka topic-level configuration keys.
//
// The data is sourced from Redpanda Console's topic configuration endpoint
// and embedded at build time so the TUI works against any Kafka-compatible
// broker without a network dependency on Redpanda Console.
package configcatalog

import (
	"fmt"
	"sort"
)

// Type classifies a config value so the UI can pick a suitable input
// control and label. The names mirror Redpanda Console's frontendFormat.
type Type int

const (
	TypeString Type = iota
	TypeInteger
	TypeBoolean
	TypeSelect
	TypeByteSize
	TypeDuration
	TypeRatio
)

// String returns the human-readable label used in the help overlay.
// New Type variants must be added here explicitly; an unknown value is
// surfaced visibly instead of masquerading as "string".
func (t Type) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeInteger:
		return "integer"
	case TypeBoolean:
		return "boolean"
	case TypeSelect:
		return "select"
	case TypeByteSize:
		return "bytes"
	case TypeDuration:
		return "duration (ms)"
	case TypeRatio:
		return "ratio"
	}
	return fmt.Sprintf("unknown(%d)", int(t))
}

// Entry is the bundled metadata for one topic-level config key.
type Entry struct {
	Key        string
	Category   string
	Type       Type
	Doc        string
	EnumValues []string
}

// Lookup returns the entry for the given config key, if known.
func Lookup(key string) (Entry, bool) {
	e, ok := entries[key]
	return e, ok
}

// All returns all bundled entries sorted by (category, key) for
// deterministic rendering.
func All() []Entry {
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Category != out[j].Category {
			return out[i].Category < out[j].Category
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// Categories returns unique category names sorted alphabetically.
func Categories() []string {
	seen := make(map[string]struct{})
	for _, e := range entries {
		seen[e.Category] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// CategoryFallback is the bucket used by the UI for keys absent from
// the bundled catalog (e.g. cluster-specific extensions).
const CategoryFallback = "Other"
