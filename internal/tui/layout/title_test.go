package layout_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

func TestCounter(t *testing.T) {
	cases := []struct {
		name     string
		filter   string
		matching int
		total    int
		want     string
	}{
		{"no filter, zero", "", 0, 0, "[0]"},
		{"no filter, total only", "", 0, 12, "[12]"},
		{"filter active", "foo", 3, 12, "[3/12] </foo>"},
		{"filter active, no matches", "xyz", 0, 5, "[0/5] </xyz>"},
		{"filter with spaces", "ab cd", 1, 2, "[1/2] </ab cd>"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, layout.Counter(c.filter, c.matching, c.total))
		})
	}
}
