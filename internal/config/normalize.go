package config

import "strings"

// Canonical allowlists for every enum-shaped field the config schema
// accepts. Each list is the single source of truth — downstream packages
// (theme, kafka, clipboard, logging) reference these rather than
// redeclaring, so adding a value happens in one place. Casing in each
// entry IS the canonical form — NormalizeEnum returns the matched entry
// as-is, so a YAML "plain" becomes "PLAIN" for SASL but YAML "RED"
// becomes "red" for color.
var (
	AllowedClusterColors  = []string{"red", "yellow", "green", "gray", "white"}
	AllowedLogLevels      = []string{"debug", "info", "warn", "error"}
	AllowedCompressions   = []string{"none", "gzip", "snappy", "lz4", "zstd"}
	AllowedClipboard      = []string{"auto", "native", "osc52", "off"}
	AllowedSASLMechanisms = []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
)

// NormalizeEnum trims and case-folds value, then matches it
// case-insensitively against allowed. Returns the matched entry from
// allowed in its declared casing — that's the canonical form downstream
// consumers expect (e.g. "PLAIN" for SASL, "red" for color). Empty or
// unmatched input returns ("", false) so callers can decide between
// hard error (CLI) and soft fallback (YAML).
func NormalizeEnum(value string, allowed []string) (string, bool) {
	norm := strings.ToLower(strings.TrimSpace(value))
	if norm == "" {
		return "", false
	}
	for _, a := range allowed {
		if strings.EqualFold(a, norm) {
			return a, true
		}
	}
	return "", false
}
