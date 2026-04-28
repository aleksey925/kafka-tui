// Package messages implements the messages browsing screen and its detail
// view. The list view (§7.3) renders one row per Kafka record with
// configurable columns, follow-mode tailing, and vim-style navigation
// (`g o`, `g t`, `g p`, `[`, `]`). The detail view (§7.4) shows the full
// record payload in JSON / Raw / HEX format, supports prev/next navigation
// without leaving the screen, and exposes copy / save / edit-in-$EDITOR /
// resend hotkeys. All Kafka I/O flows through the [Service] interface so
// the screen is unit-testable without a broker.
package messages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/aleksey925/kafka-tui/internal/kafka"
)

// ValueView is the rendered display mode for a record value.
type ValueView int

const (
	// ViewAuto reuses [kafka.DetectValueFormat] to pick JSON, raw, or HEX.
	ViewAuto ValueView = iota
	// ViewJSON renders the value as pretty-printed JSON. Falls back to raw
	// when the bytes are not valid JSON.
	ViewJSON
	// ViewRaw renders the value as UTF-8 text. Invalid bytes are replaced
	// with U+FFFD.
	ViewRaw
	// ViewHex renders the value as `hexdump -C`-style output.
	ViewHex
)

// String returns a short label for the view (used in the detail header).
func (v ValueView) String() string {
	switch v {
	case ViewJSON:
		return "json"
	case ViewRaw:
		return "raw"
	case ViewHex:
		return "hex"
	default:
		return "auto"
	}
}

// AutoView resolves [ViewAuto] into the concrete view that
// [kafka.DetectValueFormat] would pick. Other inputs are returned unchanged.
func AutoView(v ValueView, value []byte) ValueView {
	if v != ViewAuto {
		return v
	}
	switch kafka.DetectValueFormat(value) {
	case kafka.ValueFormatJSON:
		return ViewJSON
	case kafka.ValueFormatBinary:
		return ViewHex
	default:
		return ViewRaw
	}
}

// FormatValue returns the rendered body for a Kafka record value in the
// requested view. ViewAuto resolves through [AutoView] before formatting.
func FormatValue(v ValueView, value []byte) string {
	view := AutoView(v, value)
	switch view {
	case ViewJSON:
		if pretty, ok := prettyJSON(value); ok {
			return pretty
		}
		return rawText(value)
	case ViewHex:
		return HexDump(value)
	default:
		return rawText(value)
	}
}

// PreviewLine renders a one-line value preview for the messages table.
// JSON inputs are collapsed to a single line (whitespace removed); other
// inputs are stripped of newlines. The result is truncated to maxWidth
// runes and suffixed with "..." when truncated.
func PreviewLine(value []byte, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	var line string
	if json.Valid(value) {
		var compact bytes.Buffer
		if err := json.Compact(&compact, value); err == nil {
			line = compact.String()
		}
	}
	if line == "" {
		line = strings.Map(func(r rune) rune {
			if r == '\n' || r == '\r' || r == '\t' {
				return ' '
			}
			return r
		}, rawText(value))
	}
	return truncateRunes(line, maxWidth)
}

func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	const ellipsis = "..."
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	if maxRunes <= len(ellipsis) {
		runes := []rune(s)
		return string(runes[:maxRunes])
	}
	keep := maxRunes - len(ellipsis)
	runes := []rune(s)
	return string(runes[:keep]) + ellipsis
}

// HexDump renders bytes in a `hexdump -C`-style three-column layout: the
// leftmost column is the offset, the middle column is space-separated hex
// bytes split into two groups of 8, and the right column is the printable
// ASCII representation.
func HexDump(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	const cols = 16
	var out strings.Builder
	for off := 0; off < len(b); off += cols {
		chunk := b[off:min(off+cols, len(b))]
		fmt.Fprintf(&out, "%08x  ", off)
		for i := range cols {
			if i == 8 {
				out.WriteByte(' ')
			}
			if i < len(chunk) {
				fmt.Fprintf(&out, "%02x ", chunk[i])
			} else {
				out.WriteString("   ")
			}
		}
		out.WriteString(" |")
		for _, c := range chunk {
			if c >= 0x20 && c < 0x7f {
				out.WriteByte(c)
			} else {
				out.WriteByte('.')
			}
		}
		out.WriteString("|\n")
	}
	return strings.TrimRight(out.String(), "\n")
}

func prettyJSON(b []byte) (string, bool) {
	if !json.Valid(b) {
		return "", false
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, b, "", "  "); err != nil {
		return "", false
	}
	return pretty.String(), true
}

// rawText returns b decoded as UTF-8, replacing invalid bytes with U+FFFD
// and stripping ASCII control bytes other than \t, \n, \r.
func rawText(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	var out strings.Builder
	for i := 0; i < len(b); {
		r, size := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && size == 1 {
			out.WriteRune('�')
			i++
			continue
		}
		if r == '\t' || r == '\n' || r == '\r' || unicode.IsPrint(r) {
			out.WriteRune(r)
		} else {
			out.WriteRune('�')
		}
		i += size
	}
	return out.String()
}
