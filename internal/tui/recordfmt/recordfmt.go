// Package recordfmt encodes and decodes Kafka records in a
// human-readable text format used by the TUI's editor handoff and
// "save record" flows.
//
// The buffer has three labeled sections in fixed order:
//
//	# Key
//	<key — single line>
//
//	# Headers
//	<name>=<value>
//	<name>=<value>
//
//	# Value
//	<value bytes, verbatim>
//
// Once Parse enters the Value section it stops looking for markers, so
// any "#…" or blank line inside the value body round-trips. Key and
// Header values that themselves match a section-marker line do not
// round-trip — a documented limitation.
//
// [EncodeWithMetadata] prepends a block of `#`-comment lines carrying
// topic / partition / offset / timestamp; the parser's preamble
// tolerance ignores them on load, so save→load is lossless for the
// record content.
package recordfmt

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aleksey925/kafka-tui/internal/kafka"
)

var (
	utf8BOM         = []byte{0xEF, 0xBB, 0xBF}
	sectionMarkerRE = regexp.MustCompile(`(?i)^#\s*(Key|Headers|Value)\s*$`)
)

// Metadata describes a stored Kafka record for the "save with metadata"
// flow. The timestamp is rendered as raw milliseconds since the Unix
// epoch (the unit used by Kafka's wire protocol and by tools like
// kcat / kafka-console-consumer) so the saved value round-trips
// through downstream pipelines without timezone or format ambiguity.
type Metadata struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

// Encode renders the record into the section-only buffer. An empty key
// or empty headers section is rendered without an empty content line so
// two blank lines don't stack visually; the parser tolerates either
// shape.
func Encode(key string, headers []kafka.Header, value []byte) []byte {
	var b bytes.Buffer
	writeEncoded(&b, key, headers, value)
	return b.Bytes()
}

// EncodeWithMetadata is like [Encode] but prepends a `#`-comment block
// of metadata in the fixed order topic / partition / offset /
// timestamp, separated from the record by a blank line.
func EncodeWithMetadata(key string, headers []kafka.Header, value []byte, meta Metadata) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "# topic: %s\n", meta.Topic)
	fmt.Fprintf(&b, "# partition: %d\n", meta.Partition)
	fmt.Fprintf(&b, "# offset: %d\n", meta.Offset)
	fmt.Fprintf(&b, "# timestamp: %d\n", meta.Timestamp.UnixMilli())
	b.WriteByte('\n')
	writeEncoded(&b, key, headers, value)
	return b.Bytes()
}

// writeEncoded streams the record sections into b. Shared by [Encode]
// and [EncodeWithMetadata] so large values aren't copied twice in the
// with-metadata path.
func writeEncoded(b *bytes.Buffer, key string, headers []kafka.Header, value []byte) {
	b.WriteString("# Key\n")
	if key != "" {
		b.WriteString(key)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString("# Headers\n")
	for _, h := range headers {
		b.WriteString(h.Key)
		b.WriteByte('=')
		b.Write(h.Value)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString("# Value\n")
	b.Write(value)
}

// Parse is the inverse of [Encode]. It walks the buffer line by line
// tracking the current section; once it hits the "# Value" marker the
// rest of the buffer is taken verbatim as the value bytes, with no
// further parsing. Metadata in the preamble (any `#`-prefixed lines
// before `# Key`) is tolerated and silently dropped.
func Parse(buf []byte) (key string, headers []kafka.Header, value []byte, err error) {
	buf = bytes.TrimPrefix(buf, utf8BOM)
	if len(buf) == 0 {
		return "", nil, nil, errors.New("buffer is empty")
	}

	type sectionState int
	const (
		statePreamble sectionState = iota
		stateKey
		stateHeaders
	)
	st := statePreamble

	var keyLines, headerLines []string

	for i := 0; i < len(buf); {
		var line string
		var lineEnd int
		if j := bytes.IndexByte(buf[i:], '\n'); j < 0 {
			line = string(buf[i:])
			lineEnd = len(buf)
		} else {
			line = string(buf[i : i+j])
			lineEnd = i + j + 1
		}
		// CRLF: strip the trailing \r so marker regex / trim work uniformly.
		line = strings.TrimSuffix(line, "\r")

		if m := sectionMarkerRE.FindStringSubmatch(line); m != nil {
			switch {
			case strings.EqualFold(m[1], "Key"):
				if st != statePreamble {
					return "", nil, nil, errors.New("unexpected '# Key' (already seen)")
				}
				st = stateKey
			case strings.EqualFold(m[1], "Headers"):
				switch st {
				case stateKey:
					st = stateHeaders
				case stateHeaders:
					return "", nil, nil, errors.New("unexpected '# Headers' (already seen)")
				case statePreamble:
					return "", nil, nil, errors.New("unexpected '# Headers' (must come after '# Key')")
				}
			case strings.EqualFold(m[1], "Value"):
				if st != stateHeaders {
					return "", nil, nil, errors.New("unexpected '# Value' (must come after '# Headers')")
				}
				// everything after the marker line is the value, verbatim.
				value = append([]byte(nil), buf[lineEnd:]...)
				return finalize(keyLines, headerLines, value)
			}
			i = lineEnd
			continue
		}

		switch st {
		case statePreamble:
			trimmed := strings.TrimSpace(line)
			if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
				return "", nil, nil, fmt.Errorf("unexpected content before '# Key': %q", line)
			}
		case stateKey:
			keyLines = append(keyLines, line)
		case stateHeaders:
			headerLines = append(headerLines, line)
		}
		i = lineEnd
	}

	// reached EOF without seeing # Value — report which section is missing.
	switch st {
	case statePreamble:
		return "", nil, nil, errors.New("missing '# Key' section")
	case stateKey:
		return "", nil, nil, errors.New("missing '# Headers' section")
	default:
		return "", nil, nil, errors.New("missing '# Value' section")
	}
}

func finalize(keyLines, headerLines []string, value []byte) (string, []kafka.Header, []byte, error) {
	key := strings.TrimSpace(strings.Join(keyLines, "\n"))
	if strings.Contains(key, "\n") {
		return "", nil, nil, errors.New("key must be single-line")
	}

	rows := make([]string, 0, len(headerLines))
	for _, l := range headerLines {
		t := strings.TrimSpace(l)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		rows = append(rows, t)
	}
	headers, err := ParseHeaderRows(rows)
	if err != nil {
		return "", nil, nil, fmt.Errorf("headers: %w", err)
	}
	return key, headers, value, nil
}

// ParseHeaderRows converts a list of `key=value` strings into
// [kafka.Header] values. Empty / whitespace-only entries are skipped.
// Each non-empty entry must pass [ValidateHeaderRow].
func ParseHeaderRows(entries []string) ([]kafka.Header, error) {
	out := make([]kafka.Header, 0, len(entries))
	for _, e := range entries {
		entry := strings.TrimSpace(e)
		if entry == "" {
			continue
		}
		if err := ValidateHeaderRow(entry); err != nil {
			return nil, err
		}
		idx := strings.IndexByte(entry, '=')
		out = append(out, kafka.Header{
			Key:   strings.TrimSpace(entry[:idx]),
			Value: []byte(entry[idx+1:]),
		})
	}
	return out, nil
}

// ValidateHeaderRow checks that a single header row is shaped
// `key=value` with a non-empty key. Used both as a [ParseHeaderRows]
// precondition and as the produce form's per-row validator.
func ValidateHeaderRow(entry string) error {
	trimmed := strings.TrimSpace(entry)
	idx := strings.IndexByte(trimmed, '=')
	if idx < 0 {
		return errors.New("must be key=value")
	}
	if strings.TrimSpace(trimmed[:idx]) == "" {
		return errors.New("key is empty")
	}
	return nil
}
