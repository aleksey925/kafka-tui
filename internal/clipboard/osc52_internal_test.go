package clipboard

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expectedFrame builds the on-the-wire OSC 52 sequence the tests assert
// against, mirroring what [OSC52.Copy] writes internally.
func expectedFrame(payload string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(payload))
	return osc52Prefix + encoded + osc52Terminator
}

func TestOSC52Copy__producesOSC52Frame(t *testing.T) {
	var buf bytes.Buffer
	c := &OSC52{writer: &buf}

	require.NoError(t, c.Copy(context.Background(), "hello world"))

	seq := buf.String()
	require.True(t, strings.HasPrefix(seq, "\x1b]52;c;"), "missing OSC 52 prefix: %q", seq)
	require.True(t, strings.HasSuffix(seq, "\x07"), "missing BEL terminator: %q", seq)
	body := strings.TrimSuffix(strings.TrimPrefix(seq, "\x1b]52;c;"), "\x07")
	decoded, err := base64.StdEncoding.DecodeString(body)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(decoded))
}

func TestOSC52Copy__emptyPayload__emitsValidFrameWithEmptyBody(t *testing.T) {
	var buf bytes.Buffer
	c := &OSC52{writer: &buf}

	require.NoError(t, c.Copy(context.Background(), ""))

	// empty payload still produces a well-formed frame so the terminal can
	// clear the clipboard if it chooses to.
	assert.Equal(t, "\x1b]52;c;\x07", buf.String())
}

func TestOSC52Copy__writesEncodedPayloadToInjectedWriter(t *testing.T) {
	var buf bytes.Buffer
	c := &OSC52{writer: &buf}

	require.NoError(t, c.Copy(context.Background(), "secret"))

	assert.Equal(t, expectedFrame("secret"), buf.String())
}

func TestOSC52Copy__rejectsOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	c := &OSC52{writer: &buf}
	payload := strings.Repeat("a", OSC52MaxBytes+1)

	err := c.Copy(context.Background(), payload)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "too large")
	assert.Empty(t, buf.String(), "no bytes should be written when validation fails")
}

func TestOSC52Copy__cancelledContextReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf bytes.Buffer
	c := &OSC52{writer: &buf}

	err := c.Copy(ctx, "payload")

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, buf.String())
}

func TestOSC52Copy__propagatesWriterError(t *testing.T) {
	c := &OSC52{writer: failingWriter{err: errors.New("io down")}}

	err := c.Copy(context.Background(), "x")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "io down")
}

func TestOSC52_Close_ReleasesAcquiredWriter(t *testing.T) {
	var buf bytes.Buffer
	c := &OSC52{writer: &buf}
	require.NoError(t, c.Copy(context.Background(), "hi"))

	// close on an injected (non-owned) writer must not surface an error.
	require.NoError(t, c.Close())
}

type failingWriter struct{ err error }

func (f failingWriter) Write(_ []byte) (int, error) { return 0, f.err }

// Regression: the OSC52 fallback used to write the base64 payload to
// os.Stderr unconditionally. When stderr was redirected to a regular
// file (`kafka-tui 2>err.log`), the copied content leaked there. The
// isTerminal guard prevents the fallback in that scenario.
func TestIsTerminal_RejectsRegularFile(t *testing.T) {
	// arrange — a regular file is definitely not a character device.
	path := filepath.Join(t.TempDir(), "regular.log")
	f, err := os.Create(path) //nolint:gosec // test-controlled path under t.TempDir
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	// act
	got := isTerminal(f)

	// assert
	assert.False(t, got, "isTerminal must return false for redirected stderr")
}

func TestIsTerminal_HandlesClosedFile(t *testing.T) {
	// arrange — Stat() on a closed file errors; the guard must treat
	// that as "not a terminal", not panic.
	f, err := os.Create(filepath.Join(t.TempDir(), "x"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// act
	got := isTerminal(f)

	// assert
	assert.False(t, got)
}
