package messages_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
)

func TestAutoView_PicksJSONForJSONInput(t *testing.T) {
	// arrange
	in := []byte(`{"a":1}`)
	// act
	got := messages.AutoView(messages.ViewAuto, in)
	// assert
	assert.Equal(t, messages.ViewJSON, got)
}

func TestAutoView_PicksRawForUTF8Text(t *testing.T) {
	in := []byte("hello world")
	got := messages.AutoView(messages.ViewAuto, in)
	assert.Equal(t, messages.ViewRaw, got)
}

func TestAutoView_PicksHexForBinary(t *testing.T) {
	in := []byte{0x00, 0x01, 0xff, 0x10}
	got := messages.AutoView(messages.ViewAuto, in)
	assert.Equal(t, messages.ViewHex, got)
}

func TestAutoView_LeavesExplicitViewUnchanged(t *testing.T) {
	in := []byte(`{"a":1}`)
	assert.Equal(t, messages.ViewRaw, messages.AutoView(messages.ViewRaw, in))
	assert.Equal(t, messages.ViewHex, messages.AutoView(messages.ViewHex, in))
}

func TestFormatValue_JSONPrettyPrints(t *testing.T) {
	in := []byte(`{"a":1,"b":[2,3]}`)
	out := messages.FormatValue(messages.ViewJSON, in)
	assert.Contains(t, out, "\"a\": 1")
	assert.Contains(t, out, "\n")
}

func TestFormatValue_HexHasOffsetAndAscii(t *testing.T) {
	in := []byte("ABCD\x00")
	out := messages.FormatValue(messages.ViewHex, in)
	assert.Contains(t, out, "00000000")
	assert.Contains(t, out, "41 42 43 44 00")
	assert.Contains(t, out, "|ABCD.|")
}

func TestFormatValue_HexEmptyInput(t *testing.T) {
	assert.Empty(t, messages.FormatValue(messages.ViewHex, nil))
}

func TestFormatValue_AutoFallsBackToHexForBinary(t *testing.T) {
	in := []byte{0x00, 0x01, 0xff}
	out := messages.FormatValue(messages.ViewAuto, in)
	assert.Contains(t, out, "00000000")
}

func TestFormatValue_RawShowsControlBytesAsReplacement(t *testing.T) {
	in := []byte("a\x01b")
	out := messages.FormatValue(messages.ViewRaw, in)
	assert.Contains(t, out, "a")
	assert.Contains(t, out, "b")
	assert.NotContains(t, out, "\x01")
}

func TestPreviewLine_CompactsJSON(t *testing.T) {
	in := []byte(`{
  "a": 1,
  "b": [2, 3]
}`)
	out := messages.PreviewLine(in, 80)
	assert.JSONEq(t, `{"a":1,"b":[2,3]}`, out)
}

func TestPreviewLine_TruncatesWithEllipsis(t *testing.T) {
	in := []byte(strings.Repeat("x", 100))
	out := messages.PreviewLine(in, 20)
	assert.Len(t, []rune(out), 20)
	assert.True(t, strings.HasSuffix(out, "..."))
}

func TestPreviewLine_StripsNewlines(t *testing.T) {
	in := []byte("line1\nline2")
	out := messages.PreviewLine(in, 80)
	assert.Equal(t, "line1 line2", out)
}

func TestPreviewLine_ZeroWidth(t *testing.T) {
	assert.Empty(t, messages.PreviewLine([]byte("anything"), 0))
}

func TestHexDump_SecondRowOffset(t *testing.T) {
	in := make([]byte, 20)
	for i := range in {
		in[i] = byte(i + 0x40)
	}
	out := messages.HexDump(in)
	lines := strings.Split(out, "\n")
	assert.Len(t, lines, 2)
	assert.True(t, strings.HasPrefix(lines[1], "00000010"))
}
