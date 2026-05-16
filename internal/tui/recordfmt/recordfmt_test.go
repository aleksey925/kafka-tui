package recordfmt_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/recordfmt"
)

func TestEncode_RoundtripASCII(t *testing.T) {
	// arrange
	key := "order-42"
	headers := []kafka.Header{
		{Key: "source", Value: []byte("web")},
		{Key: "trace-id", Value: []byte("abc123")},
	}
	value := []byte("{\n  \"id\": 42\n}\n")

	// act
	buf := recordfmt.Encode(key, headers, value)
	gotKey, gotHeaders, gotValue, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, key, gotKey)
	assert.Equal(t, headers, gotHeaders)
	assert.Equal(t, value, gotValue)
}

func TestEncode_RoundtripAllEmpty(t *testing.T) {
	// arrange / act
	buf := recordfmt.Encode("", nil, nil)
	key, headers, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Empty(t, key)
	assert.Equal(t, []kafka.Header{}, headers)
	assert.Empty(t, value)
}

func TestEncode_EmptyKey_OmitsContentLine(t *testing.T) {
	// arrange / act
	buf := recordfmt.Encode("", nil, []byte("v"))

	// assert
	expected := "# Key\n\n# Headers\n\n# Value\nv"
	assert.Equal(t, expected, string(buf))
}

func TestEncode_WithHeaders(t *testing.T) {
	// arrange
	headers := []kafka.Header{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2=extra")},
	}

	// act
	buf := recordfmt.Encode("k", headers, []byte("body"))

	// assert
	expected := "# Key\nk\n\n# Headers\na=1\nb=2=extra\n\n# Value\nbody"
	assert.Equal(t, expected, string(buf))
}

func TestEncodeWithMetadata_LiteralBytes(t *testing.T) {
	// arrange
	meta := recordfmt.Metadata{
		Topic:     "orders",
		Partition: 3,
		Offset:    1234,
		Timestamp: time.Date(2026, 5, 14, 12, 34, 56, 0, time.UTC),
	}
	headers := []kafka.Header{{Key: "source", Value: []byte("web")}}

	// act
	buf := recordfmt.EncodeWithMetadata("order-42", headers, []byte("body"), meta)

	// assert
	expected := "# topic: orders\n" +
		"# partition: 3\n" +
		"# offset: 1234\n" +
		"# timestamp: 1778762096000\n" +
		"\n" +
		"# Key\norder-42\n\n# Headers\nsource=web\n\n# Value\nbody"
	assert.Equal(t, expected, string(buf))
}

func TestEncodeWithMetadata_RoundtripsThroughParse(t *testing.T) {
	// arrange
	meta := recordfmt.Metadata{
		Topic:     "orders",
		Partition: 7,
		Offset:    999,
		Timestamp: time.Date(2026, 5, 14, 8, 0, 0, 0, time.UTC),
	}
	headers := []kafka.Header{{Key: "x", Value: []byte("y")}}

	// act
	buf := recordfmt.EncodeWithMetadata("k", headers, []byte("body"), meta)
	key, gotHeaders, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "k", key)
	assert.Equal(t, headers, gotHeaders)
	assert.Equal(t, []byte("body"), value)
}

func TestEncodeWithMetadata_ZeroValues(t *testing.T) {
	// arrange
	meta := recordfmt.Metadata{}

	// act
	buf := recordfmt.EncodeWithMetadata("", nil, nil, meta)

	// assert
	expected := "# topic: \n" +
		"# partition: 0\n" +
		"# offset: 0\n" +
		"# timestamp: -62135596800000\n" +
		"\n" +
		"# Key\n\n# Headers\n\n# Value\n"
	assert.Equal(t, expected, string(buf))
}

func TestParse_MultilineValuePreserved(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\n\n# Value\nline1\nline2\n\nline4\n")

	// act
	_, _, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, []byte("line1\nline2\n\nline4\n"), value)
}

func TestParse_ValueContainingHeadersMarker_Preserved(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\nfoo=bar\n\n# Value\n# Headers\nnot a marker\n")

	// act
	_, headers, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, []kafka.Header{{Key: "foo", Value: []byte("bar")}}, headers)
	assert.Equal(t, []byte("# Headers\nnot a marker\n"), value)
}

func TestParse_ValueBeginningWithBlankLine_Preserved(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\n\n# Value\n\nactual body\n")

	// act
	_, _, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, []byte("\nactual body\n"), value)
}

func TestParse_CRLFLineEndings(t *testing.T) {
	// arrange
	buf := []byte("# Key\r\nk\r\n\r\n# Headers\r\na=1\r\n\r\n# Value\r\nbody\r\n")

	// act
	key, headers, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "k", key)
	assert.Equal(t, []kafka.Header{{Key: "a", Value: []byte("1")}}, headers)
	// value section preserves bytes verbatim, including the CRLF.
	assert.Equal(t, []byte("body\r\n"), value)
}

func TestParse_UTF8BOMStripped(t *testing.T) {
	// arrange
	buf := append([]byte{0xEF, 0xBB, 0xBF}, "# Key\nk\n\n# Headers\n\n# Value\nv"...)

	// act
	key, _, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "k", key)
	assert.Equal(t, []byte("v"), value)
}

func TestParse_CaseInsensitiveMarkers(t *testing.T) {
	// arrange
	buf := []byte("# key\nk\n\n#HEADERS\na=1\n\n#  Value\nv")

	// act
	key, headers, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "k", key)
	assert.Equal(t, []kafka.Header{{Key: "a", Value: []byte("1")}}, headers)
	assert.Equal(t, []byte("v"), value)
}

func TestParse_ValueSectionEmpty(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\n\n# Value\n")

	// act
	_, _, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Empty(t, value)
}

func TestParse_ValueMarkerWithoutTrailingNewline(t *testing.T) {
	// arrange
	buf := []byte("# Key\n\n# Headers\n\n# Value")

	// act
	_, _, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Empty(t, value)
}

func TestParse_HeaderCommentLinesSkipped(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\n# disabled=true\nactive=1\n\n# Value\nv")

	// act
	_, headers, _, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, []kafka.Header{{Key: "active", Value: []byte("1")}}, headers)
}

func TestParse_PreambleAllowsBlankAndComment(t *testing.T) {
	// arrange — metadata-like preamble (the shape EncodeWithMetadata produces).
	buf := []byte("# topic: orders\n# partition: 0\n# offset: 1\n\n# Key\nk\n\n# Headers\n\n# Value\nv")

	// act
	key, _, value, err := recordfmt.Parse(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "k", key)
	assert.Equal(t, []byte("v"), value)
}

func TestParse_Errors(t *testing.T) {
	tests := []struct {
		name string
		buf  string
		want string
	}{
		{
			name: "empty buffer",
			buf:  "",
			want: "buffer is empty",
		},
		{
			name: "missing # Key",
			buf:  "# Headers\nx=y\n\n# Value\nv",
			want: "must come after '# Key'",
		},
		{
			name: "missing # Headers",
			buf:  "# Key\nk\n\n# Value\nv",
			want: "must come after '# Headers'",
		},
		{
			name: "missing # Value",
			buf:  "# Key\nk\n\n# Headers\nx=y\n",
			want: "missing '# Value' section",
		},
		{
			name: "duplicate # Key",
			buf:  "# Key\nk\n# Key\nk2\n\n# Headers\n\n# Value\nv",
			want: "already seen",
		},
		{
			name: "duplicate # Headers",
			buf:  "# Key\nk\n# Headers\n# Headers\n# Value\nv",
			want: "already seen",
		},
		{
			name: "out-of-order Value before Headers",
			buf:  "# Key\nk\n# Value\nv\n# Headers\nx=y",
			want: "must come after '# Headers'",
		},
		{
			name: "multi-line key",
			buf:  "# Key\nfoo\nbar\n\n# Headers\n\n# Value\nv",
			want: "key must be single-line",
		},
		{
			name: "header line without =",
			buf:  "# Key\nk\n\n# Headers\nnosep\n\n# Value\nv",
			want: "must be key=value",
		},
		{
			name: "header line with empty name",
			buf:  "# Key\nk\n\n# Headers\n=v\n\n# Value\nv",
			want: "key is empty",
		},
		{
			name: "preamble has non-comment content",
			buf:  "noise\n# Key\nk\n\n# Headers\n\n# Value\nv",
			want: "unexpected content before '# Key'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := recordfmt.Parse([]byte(tc.buf))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestValidateHeaderRow(t *testing.T) {
	tests := []struct {
		name  string
		entry string
		ok    bool
	}{
		{name: "well-formed", entry: "a=1", ok: true},
		{name: "value with =", entry: "a=1=2", ok: true},
		{name: "empty value allowed", entry: "a=", ok: true},
		{name: "whitespace key trimmed", entry: "  a  =1", ok: true},
		{name: "no =", entry: "no-eq", ok: false},
		{name: "empty key", entry: "=v", ok: false},
		{name: "whitespace-only key", entry: "   =v", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := recordfmt.ValidateHeaderRow(tc.entry)
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
