package layout

import "fmt"

// Counter renders the standard count + filter suffix used in screen
// titles: "[N]" when filter is empty, "[M/N] </filter>" otherwise,
// where M is the number of rows matching the filter.
// Callers (list-style screens) own the prefix part of the title and
// just append the suffix.
func Counter(filter string, matching, total int) string {
	if filter == "" {
		return fmt.Sprintf("[%d]", total)
	}
	return fmt.Sprintf("[%d/%d] </%s>", matching, total, filter)
}
