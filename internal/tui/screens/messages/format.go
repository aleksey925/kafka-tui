// Package messages implements the messages browsing screen and detail view.
package messages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
)

// ValueView is the rendered display mode for a record value.
type ValueView int

const (
	// ViewAuto reuses [kafka.DetectValueFormat] to pick JSON, raw, or HEX.
	ViewAuto ValueView = iota
	ViewJSON
	ViewRaw
	ViewHex
)

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

// AutoView resolves [ViewAuto] via [kafka.DetectValueFormat]; other inputs
// pass through.
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

// PreviewLine renders a one-line preview, truncated to fit maxWidth visual
// cells. CJK / emoji-width is counted correctly via the shared truncate
// helper, so wide characters don't silently overflow the column.
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
	return components.TruncateText(line, maxWidth)
}

// HexDump renders bytes in `hexdump -C`-style layout.
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

// rawText decodes UTF-8, replacing invalid bytes and most controls with U+FFFD.
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
