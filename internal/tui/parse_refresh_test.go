package tui

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseRefresh(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"  ", 0},
		{"off", 0},
		{"OFF", 0},
		{"5s", 5 * time.Second},
		{"30s", 30 * time.Second},
		{"1m", time.Minute},
		{"-1s", 0},
		{"garbage", 0},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.want, parseRefresh(tc.in))
		})
	}
}
