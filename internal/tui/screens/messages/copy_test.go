package messages_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestCopyMenu_OpenAndIsOpen(t *testing.T) {
	// arrange
	cm := messages.NewCopyMenu(&recordingClipboard{}, theme.Styles{})

	// act
	assert.False(t, cm.IsOpen())
	cm.Open()

	// assert
	assert.True(t, cm.IsOpen())
}

func TestCopyMenu_OpenIsIdempotent(t *testing.T) {
	// arrange
	cb := &recordingClipboard{}
	cm := messages.NewCopyMenu(cb, theme.Styles{})
	msg := kafka.Message{Key: []byte("k")}
	cm.Open()
	// land the cursor on "Key" (index 1) — re-Open must not reset it.
	_ = cm.Update(keyPressRune('j'), msg)

	// act: second Open while already open, then confirm with enter.
	cm.Open()
	_ = cm.Update(keyPress("enter"), msg)

	// assert: the "Key" item dispatched, proving the cursor survived the
	// idempotent Open. A reset would have selected "Record" (index 0).
	require.Len(t, cb.payloads, 1)
	assert.Equal(t, "k", cb.payloads[0])
}

func TestCopyMenu_EscCancelsAndClosesWithoutCopy(t *testing.T) {
	// arrange
	cb := &recordingClipboard{}
	cm := messages.NewCopyMenu(cb, theme.Styles{})
	cm.Open()

	// act
	res := cm.Update(keyPress("esc"), kafka.Message{Key: []byte("k")})

	// assert
	assert.False(t, cm.IsOpen())
	assert.Empty(t, res.Toast)
	assert.Empty(t, res.Warn)
	assert.Empty(t, cb.payloads)
}

func TestCopyMenu_DispatchesEachItem(t *testing.T) {
	msg := kafka.Message{
		Topic:     "orders",
		Partition: 3,
		Offset:    11,
		Key:       []byte("order-42"),
		Value:     []byte("payload"),
		Headers: []kafka.Header{
			{Key: "source", Value: []byte("web")},
			{Key: "trace-id", Value: []byte("abc")},
		},
	}
	headersExpected := "source=web\ntrace-id=abc\n"

	cases := []struct {
		name        string
		digit       rune
		wantPayload string
		wantLabel   string
	}{
		{"record", '1', "", "record"}, // record format checked separately via Contains
		{"key", '2', "order-42", "key"},
		{"value", '3', "payload", "value"},
		{"headers", '4', headersExpected, "headers"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// arrange
			cb := &recordingClipboard{}
			cm := messages.NewCopyMenu(cb, theme.Styles{})
			cm.Open()

			// act
			res := cm.Update(keyPressRune(tc.digit), msg)

			// assert
			assert.False(t, cm.IsOpen())
			assert.Empty(t, res.Warn)
			assert.Contains(t, res.Toast, "copied "+tc.wantLabel)
			require.Len(t, cb.payloads, 1)
			if tc.wantPayload != "" {
				assert.Equal(t, tc.wantPayload, cb.payloads[0])
			} else {
				// record payload carries the metadata frame + all fields.
				got := cb.payloads[0]
				assert.True(t, strings.HasPrefix(got, "# topic: orders\n"))
				assert.Contains(t, got, "# partition: 3")
				assert.Contains(t, got, "# offset: 11")
				assert.Contains(t, got, "order-42")
				assert.Contains(t, got, "payload")
				assert.Contains(t, got, "source=web")
			}
		})
	}
}

func TestCopyMenu_NilClipboardReturnsWarn(t *testing.T) {
	// arrange
	cm := messages.NewCopyMenu(nil, theme.Styles{})
	cm.Open()

	// act
	res := cm.Update(keyPressRune('2'), kafka.Message{Key: []byte("k")})

	// assert
	assert.False(t, cm.IsOpen())
	assert.Empty(t, res.Toast)
	assert.Equal(t, "copy key: clipboard unavailable", res.Warn)
}

func TestCopyMenu_ClipboardErrorReturnsWarn(t *testing.T) {
	// arrange
	cb := &recordingClipboard{err: errors.New("osc52 refused")}
	cm := messages.NewCopyMenu(cb, theme.Styles{})
	cm.Open()

	// act
	res := cm.Update(keyPressRune('3'), kafka.Message{Value: []byte("v")})

	// assert
	assert.False(t, cm.IsOpen())
	assert.Empty(t, res.Toast)
	assert.Equal(t, "copy value: osc52 refused", res.Warn)
}

func TestCopyMenu_UpdateOnClosedIsNoOp(t *testing.T) {
	// arrange
	cb := &recordingClipboard{}
	cm := messages.NewCopyMenu(cb, theme.Styles{})

	// act: never opened
	res := cm.Update(keyPressRune('1'), kafka.Message{Value: []byte("v")})

	// assert
	assert.Equal(t, messages.CopyResult{}, res)
	assert.Empty(t, cb.payloads)
}

// recordingClipboard captures payloads handed to Copy; err lets a test
// simulate a failing clipboard backend. keyPress / keyPressRune helpers
// come from messages_test.go (same package).
type recordingClipboard struct {
	payloads []string
	err      error
}

func (r *recordingClipboard) Copy(_ context.Context, payload string) error {
	if r.err != nil {
		return r.err
	}
	r.payloads = append(r.payloads, payload)
	return nil
}
