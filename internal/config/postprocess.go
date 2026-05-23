package config

import (
	"fmt"
	"log/slog"
	"strings"
)

// postProcessConfig normalizes soft-enum fields on cfg (logging.level,
// produce.default_compression, clipboard.method). Invalid values are
// warned about and reset to "" so the consumer's existing default kicks
// in — the app keeps running even when a YAML typo would otherwise leave
// a feature mis-configured. Returns warnings to attach to Loaded.Warnings.
func postProcessConfig(cfg *Config) []string {
	warnings := make([]string, 0)
	cfg.Logging.Level, warnings = applySoftEnum(cfg.Logging.Level, AllowedLogLevels, "logging.level", warnings)
	cfg.Produce.DefaultCompression, warnings = applySoftEnum(cfg.Produce.DefaultCompression, AllowedCompressions, "produce.default_compression", warnings)
	cfg.Clipboard.Method, warnings = applySoftEnum(cfg.Clipboard.Method, AllowedClipboard, "clipboard.method", warnings)
	return warnings
}

// postProcessClustersSoft normalizes soft-enum cluster fields (currently
// just color). Invalid values are warned about and reset; the cluster
// keeps loading.
func postProcessClustersSoft(clusters []Cluster) []string {
	warnings := make([]string, 0, len(clusters))
	for i := range clusters {
		c := &clusters[i]
		c.Color, warnings = applySoftEnum(c.Color, AllowedClusterColors,
			fmt.Sprintf("cluster %q: color", c.Name), warnings)
	}
	return warnings
}

// normalizeClusterHard validates per-cluster enum fields where a wrong
// value would lead to silent runtime failures (currently just SASL
// mechanism — a bogus mechanism aborts the SASL handshake with a
// confusing error far from the config file). Returns an error so the
// per-cluster pipeline quarantines the cluster rather than letting it
// reach dial time.
func normalizeClusterHard(c *Cluster) error {
	if c.SASL == nil || c.SASL.Mechanism == "" {
		return nil
	}
	norm, ok := NormalizeEnum(c.SASL.Mechanism, AllowedSASLMechanisms)
	if !ok {
		return fmt.Errorf("invalid sasl.mechanism %q (allowed: %s)",
			c.SASL.Mechanism, strings.Join(AllowedSASLMechanisms, ", "))
	}
	c.SASL.Mechanism = norm
	return nil
}

// applySoftEnum normalizes value and either writes the canonical form
// back, or zeros it out and appends a warning. Empty input passes
// through untouched so the consumer's "" → default branch keeps working.
func applySoftEnum(value string, allowed []string, fieldLabel string, warnings []string) (string, []string) {
	if value == "" {
		return "", warnings
	}
	if norm, ok := NormalizeEnum(value, allowed); ok {
		return norm, warnings
	}
	warnings = append(warnings, fmt.Sprintf(
		"%s: invalid value %q (allowed: %s); using default",
		fieldLabel, value, strings.Join(allowed, ", "),
	))
	slog.Warn("config: invalid value, falling back to default",
		slog.String("field", fieldLabel),
		slog.String("value", value),
	)
	return "", warnings
}
