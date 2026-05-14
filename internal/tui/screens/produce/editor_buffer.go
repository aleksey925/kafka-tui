package produce

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/aleksey925/kafka-tui/internal/kafka"
)

// editor_buffer.go implements the encode/decode for the external editor
// session. The buffer format is three labeled sections in fixed order:
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
// once the parser enters the Value section it stops looking for markers,
// so any "#…" or blank line inside the value body round-trips. The Key
// and Headers sections do not roundtrip values that themselves match a
// section-marker line — a documented limitation, see plan.

var (
	utf8BOM         = []byte{0xEF, 0xBB, 0xBF}
	sectionMarkerRE = regexp.MustCompile(`(?i)^#\s*(Key|Headers|Value)\s*$`)
)

// encodeEditorBuffer renders the record into the editor format. An empty
// key or empty headers section is rendered without an empty content line
// so two blank lines don't stack visually; the parser tolerates either
// shape.
func encodeEditorBuffer(key string, headers []kafka.Header, value []byte) []byte {
	var b bytes.Buffer
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
	return b.Bytes()
}

// parseEditorBuffer is the inverse of [encodeEditorBuffer]. It walks the
// buffer line by line tracking the current section; once it hits the
// "# Value" marker the rest of the buffer is taken verbatim as the value
// bytes, with no further parsing.
func parseEditorBuffer(buf []byte) (key string, headers []kafka.Header, value []byte, err error) {
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
				return finalizeEditorBuffer(keyLines, headerLines, value)
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

func finalizeEditorBuffer(keyLines, headerLines []string, value []byte) (string, []kafka.Header, []byte, error) {
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
	headers, err := parseHeaders(rows)
	if err != nil {
		return "", nil, nil, fmt.Errorf("headers: %w", err)
	}
	return key, headers, value, nil
}
