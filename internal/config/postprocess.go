package config

import (
	"fmt"
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
//
// Warnings go only into the returned slice — slog is intentionally NOT
// called here because postprocess runs before logging.Init (the log
// level itself comes from this config), so a slog write would land in
// the pre-init default handler and corrupt the TUI screen at startup.
// The cluster picker surfaces warnings via its toast queue, which then
// mirrors to slog after the logger is wired up.
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
	return "", warnings
}

// checkYAMLCredentialExposure inspects cluster.sasl.password and
// cfg.vault.token for literal values. See CLAUDE.md § Credentials: storage and
// exposure warnings. Must run BEFORE the env+file phase resolves
// placeholders.
func checkYAMLCredentialExposure(vault VaultConfig, clusters []Cluster) []string {
	var out []string
	if w := literalCredentialWarning("vault.token", vault.Token); w != "" {
		out = append(out, w)
	}
	for _, c := range clusters {
		if c.SASL == nil {
			continue
		}
		label := fmt.Sprintf("cluster %q: sasl.password", c.Name)
		if w := literalCredentialWarning(label, c.SASL.Password); w != "" {
			out = append(out, w)
		}
	}
	return out
}

func literalCredentialWarning(label string, value Secret) string {
	if !IsLiteralCredential(value) {
		return ""
	}
	return fmt.Sprintf(
		"%s is a literal value; prefer ${env:VAR}, ${file:/path}, or "+
			"${vault:path#key} — literal secrets in YAML end up in "+
			"backups, git, and shared filesystems",
		label,
	)
}
