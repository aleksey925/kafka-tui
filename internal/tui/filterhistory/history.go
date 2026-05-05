// Package filterhistory implements a bounded, case-insensitive ring of
// recently-used filter queries backing the `/` search prompt's ghost
// suggestion. One History instance lives per searchable screen so queries
// don't bleed across unrelated domains.
package filterhistory

import "strings"

// History is an LRU of lowercased query strings.
type History struct {
	size    int
	entries []string // newest at index 0
}

// New returns an empty History capped at size (clamped to >=1).
func New(size int) *History {
	if size < 1 {
		size = 1
	}
	return &History{size: size}
}

// Push records a query. Empty / whitespace-only strings are ignored. The
// stored value is lowercased so ghost rendering (which trims
// `strings.ToLower(buffer)` from the suggestion) lines up.
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

// Matches returns entries starting with prefix (case-insensitive), newest
// first. Empty prefix returns the whole history.
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

func (h *History) Len() int { return len(h.entries) }
