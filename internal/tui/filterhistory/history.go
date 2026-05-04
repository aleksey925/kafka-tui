// Package filterhistory implements a bounded, case-insensitive ring of
// recently-used filter queries. It backs the `/` search prompt's ghost
// suggestion: opening the prompt over an empty buffer surfaces the most
// recent entry; typing narrows the list to prefix matches; Up/Down cycle.
//
// One History instance lives per searchable screen so queries don't bleed
// across unrelated domains (topics vs consumer groups vs clusters etc.).
package filterhistory

import "strings"

// History is an LRU of lowercased query strings. Push deduplicates against
// the full list (rotates an existing entry to the head) and evicts the
// oldest entry when capacity would be exceeded.
type History struct {
	size    int
	entries []string // newest at index 0
}

// New returns an empty History capped at the given size. A non-positive
// size collapses to 1 so the type is always usable.
func New(size int) *History {
	if size < 1 {
		size = 1
	}
	return &History{size: size}
}

// Push records a query. Empty / whitespace-only strings are ignored.
// Comparison is case-insensitive — the stored value is lowercased so ghost
// rendering (which trims `strings.ToLower(buffer)` from the suggestion)
// lines up. Internal whitespace is preserved verbatim — the caller's query
// is what gets re-applied as a filter when the user accepts the suggestion.
func (h *History) Push(s string) {
	if strings.TrimSpace(s) == "" {
		return
	}
	s = strings.ToLower(s)
	for i, e := range h.entries {
		if e == s {
			h.entries = append(h.entries[:i], h.entries[i+1:]...)
			break
		}
	}
	h.entries = append([]string{s}, h.entries...)
	if len(h.entries) > h.size {
		h.entries = h.entries[:h.size]
	}
}

// Matches returns entries that start with prefix (case-insensitive),
// newest first. A nil/empty prefix returns the whole history. The result
// is a fresh slice — callers can mutate it freely.
func (h *History) Matches(prefix string) []string {
	prefix = strings.ToLower(prefix)
	out := make([]string, 0, len(h.entries))
	for _, e := range h.entries {
		if strings.HasPrefix(e, prefix) {
			out = append(out, e)
		}
	}
	return out
}

// Len reports the number of stored entries.
func (h *History) Len() int { return len(h.entries) }
