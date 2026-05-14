package produce

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
)

func TestEditorBuffer_RoundtripASCII(t *testing.T) {
	// arrange
	key := "order-42"
	headers := []kafka.Header{
		{Key: "source", Value: []byte("web")},
		{Key: "trace-id", Value: []byte("abc123")},
	}
	value := []byte("{\n  \"id\": 42\n}\n")

	// act
	buf := encodeEditorBuffer(key, headers, value)
	gotKey, gotHeaders, gotValue, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, key, gotKey)
	assert.Equal(t, headers, gotHeaders)
	assert.Equal(t, value, gotValue)
}

func TestEditorBuffer_RoundtripAllEmpty(t *testing.T) {
	// arrange / act
	buf := encodeEditorBuffer("", nil, nil)
	key, headers, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Empty(t, key)
	assert.Equal(t, []kafka.Header{}, headers)
	assert.Empty(t, value)
}

func TestEditorBuffer_EncodeEmptyKey_OmitsContentLine(t *testing.T) {
	// arrange / act
	buf := encodeEditorBuffer("", nil, []byte("v"))

	// assert
	expected := "# Key\n\n# Headers\n\n# Value\nv"
	assert.Equal(t, expected, string(buf))
}

func TestEditorBuffer_EncodeWithHeaders(t *testing.T) {
	// arrange
	headers := []kafka.Header{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2=extra")},
	}

	// act
	buf := encodeEditorBuffer("k", headers, []byte("body"))

	// assert
	expected := "# Key\nk\n\n# Headers\na=1\nb=2=extra\n\n# Value\nbody"
	assert.Equal(t, expected, string(buf))
}

func TestParseEditorBuffer_MultilineValuePreserved(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\n\n# Value\nline1\nline2\n\nline4\n")

	// act
	_, _, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, []byte("line1\nline2\n\nline4\n"), value)
}

func TestParseEditorBuffer_ValueContainingHeadersMarker_Preserved(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\nfoo=bar\n\n# Value\n# Headers\nnot a marker\n")

	// act
	_, headers, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, []kafka.Header{{Key: "foo", Value: []byte("bar")}}, headers)
	assert.Equal(t, []byte("# Headers\nnot a marker\n"), value)
}

func TestParseEditorBuffer_ValueBeginningWithBlankLine_Preserved(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\n\n# Value\n\nactual body\n")

	// act
	_, _, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, []byte("\nactual body\n"), value)
}

func TestParseEditorBuffer_CRLFLineEndings(t *testing.T) {
	// arrange
	buf := []byte("# Key\r\nk\r\n\r\n# Headers\r\na=1\r\n\r\n# Value\r\nbody\r\n")

	// act
	key, headers, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "k", key)
	assert.Equal(t, []kafka.Header{{Key: "a", Value: []byte("1")}}, headers)
	// value section preserves bytes verbatim, including the CRLF.
	assert.Equal(t, []byte("body\r\n"), value)
}

func TestParseEditorBuffer_UTF8BOMStripped(t *testing.T) {
	// arrange
	buf := append([]byte{0xEF, 0xBB, 0xBF}, "# Key\nk\n\n# Headers\n\n# Value\nv"...)

	// act
	key, _, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "k", key)
	assert.Equal(t, []byte("v"), value)
}

func TestParseEditorBuffer_CaseInsensitiveMarkers(t *testing.T) {
	// arrange
	buf := []byte("# key\nk\n\n#HEADERS\na=1\n\n#  Value\nv")

	// act
	key, headers, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "k", key)
	assert.Equal(t, []kafka.Header{{Key: "a", Value: []byte("1")}}, headers)
	assert.Equal(t, []byte("v"), value)
}

func TestParseEditorBuffer_ValueSectionEmpty(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\n\n# Value\n")

	// act
	_, _, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Empty(t, value)
}

func TestParseEditorBuffer_ValueMarkerWithoutTrailingNewline(t *testing.T) {
	// arrange
	buf := []byte("# Key\n\n# Headers\n\n# Value")

	// act
	_, _, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Empty(t, value)
}

func TestParseEditorBuffer_HeaderCommentLinesSkipped(t *testing.T) {
	// arrange
	buf := []byte("# Key\nk\n\n# Headers\n# disabled=true\nactive=1\n\n# Value\nv")

	// act
	_, headers, _, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, []kafka.Header{{Key: "active", Value: []byte("1")}}, headers)
}

func TestParseEditorBuffer_PreambleAllowsBlankAndComment(t *testing.T) {
	// arrange
	buf := []byte("\n# random banner\n\n# Key\nk\n\n# Headers\n\n# Value\nv")

	// act
	key, _, value, err := parseEditorBuffer(buf)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "k", key)
	assert.Equal(t, []byte("v"), value)
}

func TestParseEditorBuffer_Errors(t *testing.T) {
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
			_, _, _, err := parseEditorBuffer([]byte(tc.buf))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
