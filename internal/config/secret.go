package config

import (
	"encoding/json"
	"log/slog"
)

// Secret wraps a credential so accidental formatting (%v / %s / %+v /
// slog.Any / fmt.Errorf with the holder struct as an arg) emits a
// redaction marker instead of the value. See CLAUDE.md § Credentials:
// storage and exposure warnings.
//
// Implements [fmt.Stringer], [fmt.GoStringer], [slog.LogValuer], and
// [json.Marshaler]. All redact non-empty values.
//
// Empty values render as empty rather than as the marker so an
// operator reading `password=` in a log knows the field is unset (and
// can debug a missing-credential failure) instead of seeing
// `password=[REDACTED]` and assuming a value is present.
//
// YAML marshaling is intentionally NOT redacted: remarshalInto in the
// loader round-trips Secret values through yaml.Marshal + Unmarshal,
// and a redacted marshal would erase the secret before the kafka
// client sees it.
type Secret string

// RedactedMarker is the string emitted in place of a non-empty
// underlying value by every formatting path on [Secret]. Exposed so
// tests and callers can compare without hard-coding the literal.
const RedactedMarker = "[REDACTED]"

// marker returns RedactedMarker for non-empty values and "" for empty
// — shared by String / GoString / LogValue / MarshalJSON so the
// empty-handling rule has one home.
func (s Secret) marker() string {
	if s == "" {
		return ""
	}
	return RedactedMarker
}

// String satisfies [fmt.Stringer] — covers `%s`, `%v` on the value, and
// any string concatenation through Stringer-aware paths.
func (s Secret) String() string { return s.marker() }

// GoString satisfies [fmt.GoStringer] — covers `%#v` so debug dumps
// (`spew.Dump`, `litter.Sdump`, etc.) also redact.
func (s Secret) GoString() string { return `Secret("` + s.marker() + `")` }

// LogValue satisfies [slog.LogValuer] — slog.Any / slog.Group / any
// structured logging call routes through this method, so the value
// never reaches the handler.
func (s Secret) LogValue() slog.Value { return slog.StringValue(s.marker()) }

// MarshalJSON satisfies [json.Marshaler]. Unlike YAML, JSON output of
// config structs is not round-tripped anywhere in the loader, so
// redacting here is pure defense — a debug dump via json.MarshalIndent
// won't leak the value.
//
// Delegates to [json.Marshal] for the actual escaping so a future
// change to [RedactedMarker] (e.g. one containing a quote or control
// character) cannot produce invalid JSON via naive concatenation.
//
//nolint:wrapcheck // json.Marshal error on a plain string is impossible.
func (s Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.marker())
}

// Reveal returns the underlying secret. Every call is a deliberate
// escape from the redaction contract — keep them at the API boundary
// (kafka SASL auth, vault HTTP request, TLS key parser) and never
// closer to the surface. Easy to grep for in code review.
func (s Secret) Reveal() string { return string(s) }
