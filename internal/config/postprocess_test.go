package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeEnum(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		allowed   []string
		wantNorm  string
		wantMatch bool
	}{
		{"empty", "", []string{"a", "b"}, "", false},
		{"whitespace_only", "   ", []string{"a", "b"}, "", false},
		{"exact_lowercase_match", "red", []string{"red", "blue"}, "red", true},
		{"uppercase_input_lowercase_canonical", "RED", []string{"red", "blue"}, "red", true},
		{"padded_input", "  Blue ", []string{"red", "blue"}, "blue", true},
		{"lowercase_input_uppercase_canonical", "plain", []string{"PLAIN", "SCRAM-SHA-256"}, "PLAIN", true},
		{"unknown", "magenta", []string{"red", "blue"}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// act
			norm, ok := NormalizeEnum(tc.input, tc.allowed)

			// assert
			assert.Equal(t, tc.wantNorm, norm)
			assert.Equal(t, tc.wantMatch, ok)
		})
	}
}

func TestPostProcessConfig__validValues__normalizedInPlace(t *testing.T) {
	// arrange
	cfg := Config{
		Logging:   LoggingConfig{Level: "DEBUG"},
		Produce:   ProduceConfig{DefaultCompression: " Gzip "},
		Clipboard: ClipboardConfig{Method: "auto"},
	}

	// act
	warnings := postProcessConfig(&cfg)

	// assert
	assert.Empty(t, warnings)
	assert.Equal(t, "debug", cfg.Logging.Level)
	assert.Equal(t, "gzip", cfg.Produce.DefaultCompression)
	assert.Equal(t, "auto", cfg.Clipboard.Method)
}

func TestPostProcessConfig__invalidValues__resetWithWarning(t *testing.T) {
	// arrange
	cfg := Config{
		Logging:   LoggingConfig{Level: "spam"},
		Produce:   ProduceConfig{DefaultCompression: "brotli"},
		Clipboard: ClipboardConfig{Method: "xclip"},
	}

	// act
	warnings := postProcessConfig(&cfg)

	// assert
	assert.Empty(t, cfg.Logging.Level)
	assert.Empty(t, cfg.Produce.DefaultCompression)
	assert.Empty(t, cfg.Clipboard.Method)
	require.Len(t, warnings, 3)
	assert.Contains(t, warnings[0], "logging.level")
	assert.Contains(t, warnings[0], `"spam"`)
	assert.Contains(t, warnings[1], "produce.default_compression")
	assert.Contains(t, warnings[2], "clipboard.method")
}

func TestPostProcessClustersSoft__validColors__normalizedInPlace(t *testing.T) {
	// arrange
	clusters := []Cluster{
		{Name: "a", Color: "RED"},
		{Name: "b", Color: " Yellow "},
		{Name: "c", Color: ""},
	}

	// act
	warnings := postProcessClustersSoft(clusters)

	// assert
	assert.Empty(t, warnings)
	assert.Equal(t, []Cluster{
		{Name: "a", Color: "red"},
		{Name: "b", Color: "yellow"},
		{Name: "c", Color: ""},
	}, clusters)
}

func TestPostProcessClustersSoft__invalidColor__resetWithWarning(t *testing.T) {
	// arrange
	clusters := []Cluster{
		{Name: "good", Color: "red"},
		{Name: "typo", Color: "magenta"},
	}

	// act
	warnings := postProcessClustersSoft(clusters)

	// assert
	assert.Equal(t, []Cluster{
		{Name: "good", Color: "red"},
		{Name: "typo", Color: ""},
	}, clusters)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], `cluster "typo"`)
	assert.Contains(t, warnings[0], `"magenta"`)
}

func TestNormalizeClusterHard__validMechanism__upperCased(t *testing.T) {
	// arrange
	c := Cluster{SASL: &SASLConfig{Mechanism: "plain", Username: "u", Password: "p"}}

	// act
	err := normalizeClusterHard(&c)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "PLAIN", c.SASL.Mechanism)
}

func TestNormalizeClusterHard__invalidMechanism__errors(t *testing.T) {
	// arrange
	c := Cluster{Name: "p", SASL: &SASLConfig{Mechanism: "kerberos", Username: "u", Password: "p"}}

	// act
	err := normalizeClusterHard(&c)

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kerberos")
	assert.Contains(t, err.Error(), "PLAIN")
}

func TestNormalizeClusterHard__noSASL__noOp(t *testing.T) {
	// arrange
	c := Cluster{SASL: nil}

	// act
	err := normalizeClusterHard(&c)

	// assert
	require.NoError(t, err)
}
