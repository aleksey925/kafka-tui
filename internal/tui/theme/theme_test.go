package theme_test

import (
	"image/color"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestPalette_ClusterColor(t *testing.T) {
	p := theme.Default()

	tests := []struct {
		name string
		want color.Color
	}{
		{theme.ClusterRed, p.Red},
		{theme.ClusterYellow, p.Yellow},
		{theme.ClusterGreen, p.Green},
		{theme.ClusterGray, p.Gray},
		{theme.ClusterWhite, p.White},
		{"unknown", p.Foreground},
		{"", p.Foreground},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, p.ClusterColor(tc.name))
		})
	}
}

func TestAllowedClusterColors(t *testing.T) {
	assert.Equal(t, []string{"red", "yellow", "green", "gray", "white"}, theme.AllowedClusterColors)
}

func TestNewStyles_AppliesPalette(t *testing.T) {
	p := theme.Default()
	s := theme.New(p)

	assert.Equal(t, p, s.Palette)
	assert.NotEmpty(t, s.Header.Render("hi"))
	assert.NotEmpty(t, s.HintKey.Render(":"))
}

func TestDefaultStyles_HasPaletteFilled(t *testing.T) {
	s := theme.DefaultStyles()
	assert.NotNil(t, s.Palette.Foreground)
	assert.NotNil(t, s.Palette.Red)
}
