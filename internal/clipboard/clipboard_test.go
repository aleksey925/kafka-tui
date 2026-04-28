package clipboard_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/clipboard"
)

func TestParseMethod(t *testing.T) {
	cases := []struct {
		in      string
		want    clipboard.Method
		wantErr bool
	}{
		{in: "", want: clipboard.MethodAuto},
		{in: "auto", want: clipboard.MethodAuto},
		{in: "AUTO", want: clipboard.MethodAuto},
		{in: "  native ", want: clipboard.MethodNative},
		{in: "osc52", want: clipboard.MethodOSC52},
		{in: "off", want: clipboard.MethodOff},
		{in: "none", want: clipboard.MethodOff},
		{in: "disabled", want: clipboard.MethodOff},
		{in: "bogus", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := clipboard.ParseMethod(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestNew__off__noopReturnsNilAndIgnoresPayload(t *testing.T) {
	// arrange
	native := &recordingClipboard{}
	osc52 := &recordingClipboard{}

	// act
	c := clipboard.New(clipboard.Options{Method: clipboard.MethodOff, Native: native, OSC52: osc52})
	err := c.Copy(context.Background(), "hello")

	// assert
	require.NoError(t, err)
	assert.Equal(t, []string(nil), native.payloads())
	assert.Equal(t, []string(nil), osc52.payloads())
}

func TestNew__native__forwardsToNativeOnly(t *testing.T) {
	// arrange
	native := &recordingClipboard{}
	osc52 := &recordingClipboard{}

	// act
	c := clipboard.New(clipboard.Options{Method: clipboard.MethodNative, Native: native, OSC52: osc52})
	require.NoError(t, c.Copy(context.Background(), "abc"))

	// assert
	assert.Equal(t, []string{"abc"}, native.payloads())
	assert.Equal(t, []string(nil), osc52.payloads())
}

func TestNew__osc52__forwardsToOsc52Only(t *testing.T) {
	// arrange
	native := &recordingClipboard{}
	osc52 := &recordingClipboard{}

	// act
	c := clipboard.New(clipboard.Options{Method: clipboard.MethodOSC52, Native: native, OSC52: osc52})
	require.NoError(t, c.Copy(context.Background(), "abc"))

	// assert
	assert.Equal(t, []string(nil), native.payloads())
	assert.Equal(t, []string{"abc"}, osc52.payloads())
}

func TestNew__auto__runsBothInParallel(t *testing.T) {
	// arrange
	native := &recordingClipboard{}
	osc52 := &recordingClipboard{}

	// act
	c := clipboard.New(clipboard.Options{Method: clipboard.MethodAuto, Native: native, OSC52: osc52})
	require.NoError(t, c.Copy(context.Background(), "payload"))

	// assert
	assert.Equal(t, []string{"payload"}, native.payloads())
	assert.Equal(t, []string{"payload"}, osc52.payloads())
}

func TestNew__auto__succeedsWhenOnlyOneTransportSucceeds(t *testing.T) {
	// arrange
	native := &recordingClipboard{err: errors.New("native broken")}
	osc52 := &recordingClipboard{}

	// act
	c := clipboard.New(clipboard.Options{Method: clipboard.MethodAuto, Native: native, OSC52: osc52})
	err := c.Copy(context.Background(), "payload")

	// assert
	require.NoError(t, err)
	assert.Equal(t, []string{"payload"}, osc52.payloads())
}

func TestNew__auto__joinsErrorsWhenBothFail(t *testing.T) {
	// arrange
	native := &recordingClipboard{err: errors.New("native broken")}
	osc52 := &recordingClipboard{err: errors.New("osc52 broken")}

	// act
	c := clipboard.New(clipboard.Options{Method: clipboard.MethodAuto, Native: native, OSC52: osc52})
	err := c.Copy(context.Background(), "x")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "native broken")
	assert.Contains(t, err.Error(), "osc52 broken")
}

func TestNew__auto__concurrencyDoesNotSerializeTransports(t *testing.T) {
	// arrange — each transport's Copy blocks on a shared rendezvous wait
	// group: it calls Done then Wait. If MethodAuto ran them sequentially
	// the first Copy would block forever (count=1, waiting for count=0
	// which only happens after the second Copy is entered).
	rendezvous := &sync.WaitGroup{}
	rendezvous.Add(2)
	native := &rendezvousClipboard{wg: rendezvous}
	osc52 := &rendezvousClipboard{wg: rendezvous}

	c := clipboard.New(clipboard.Options{Method: clipboard.MethodAuto, Native: native, OSC52: osc52})

	// act
	done := make(chan error, 1)
	go func() { done <- c.Copy(context.Background(), "x") }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Copy did not return — transports ran sequentially")
	}

	// assert
	assert.Equal(t, []string{"x"}, native.payloads())
	assert.Equal(t, []string{"x"}, osc52.payloads())
}

func TestEncodeSequence__producesOSC52Frame(t *testing.T) {
	// act
	seq := clipboard.EncodeSequence("hello world")

	// assert
	require.True(t, strings.HasPrefix(seq, "\x1b]52;c;"), "missing OSC 52 prefix: %q", seq)
	require.True(t, strings.HasSuffix(seq, "\x07"), "missing BEL terminator: %q", seq)
	body := strings.TrimSuffix(strings.TrimPrefix(seq, "\x1b]52;c;"), "\x07")
	decoded, err := base64.StdEncoding.DecodeString(body)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(decoded))
}

func TestEncodeSequence__emptyPayload__emitsValidFrameWithEmptyBody(t *testing.T) {
	// act
	seq := clipboard.EncodeSequence("")

	// assert — empty payload still produces a well-formed frame so the
	// terminal can clear the clipboard if it chooses to.
	assert.Equal(t, "\x1b]52;c;\x07", seq)
}

func TestOSC52Copy__writesEncodedPayloadToInjectedWriter(t *testing.T) {
	// arrange
	var buf bytes.Buffer
	c := clipboard.NewOSC52(clipboard.OSC52Options{Writer: &buf})

	// act
	require.NoError(t, c.Copy(context.Background(), "secret"))

	// assert
	assert.Equal(t, clipboard.EncodeSequence("secret"), buf.String())
}

func TestOSC52Copy__rejectsOversizedPayload(t *testing.T) {
	// arrange
	var buf bytes.Buffer
	c := clipboard.NewOSC52(clipboard.OSC52Options{Writer: &buf})
	payload := strings.Repeat("a", clipboard.OSC52MaxBytes+1)

	// act
	err := c.Copy(context.Background(), payload)

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too large")
	assert.Empty(t, buf.String(), "no bytes should be written when validation fails")
}

func TestOSC52Copy__cancelledContextReturnsError(t *testing.T) {
	// arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf bytes.Buffer
	c := clipboard.NewOSC52(clipboard.OSC52Options{Writer: &buf})

	// act
	err := c.Copy(ctx, "payload")

	// assert
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, buf.String())
}

func TestOSC52Copy__propagatesWriterError(t *testing.T) {
	// arrange
	w := failingWriter{err: errors.New("io down")}
	c := clipboard.NewOSC52(clipboard.OSC52Options{Writer: w})

	// act
	err := c.Copy(context.Background(), "x")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "io down")
}

func TestNative__darwin__usesPbcopy(t *testing.T) {
	// arrange
	runner := &fakeRunner{}
	n := clipboard.NewNative(clipboard.NativeOptions{
		GOOS:     "darwin",
		Env:      func(string) string { return "" },
		LookPath: lookPathAlways(),
		Runner:   runner.Run,
	})

	// act
	require.NoError(t, n.Copy(context.Background(), "hi"))

	// assert
	assert.Equal(t, "pbcopy", runner.lastName)
	assert.Equal(t, []string(nil), runner.lastArgs)
	assert.Equal(t, "hi", string(runner.lastStdin))
}

func TestNative__linuxWayland__prefersWlCopy(t *testing.T) {
	// arrange
	runner := &fakeRunner{}
	n := clipboard.NewNative(clipboard.NativeOptions{
		GOOS:     "linux",
		Env:      mapEnv(map[string]string{"WAYLAND_DISPLAY": "wayland-0"}),
		LookPath: lookPathAlways(),
		Runner:   runner.Run,
	})

	// act
	require.NoError(t, n.Copy(context.Background(), "hi"))

	// assert
	assert.Equal(t, "wl-copy", runner.lastName)
}

func TestNative__linuxX11__prefersXclip(t *testing.T) {
	// arrange
	runner := &fakeRunner{}
	n := clipboard.NewNative(clipboard.NativeOptions{
		GOOS:     "linux",
		Env:      func(string) string { return "" },
		LookPath: lookPathAlways(),
		Runner:   runner.Run,
	})

	// act
	require.NoError(t, n.Copy(context.Background(), "hi"))

	// assert
	assert.Equal(t, "xclip", runner.lastName)
	assert.Equal(t, []string{"-selection", "clipboard"}, runner.lastArgs)
}

func TestNative__linux__fallsBackToXselThenWlCopy(t *testing.T) {
	// arrange — only xsel is on PATH; xclip is missing.
	runner := &fakeRunner{}
	n := clipboard.NewNative(clipboard.NativeOptions{
		GOOS:     "linux",
		Env:      func(string) string { return "" },
		LookPath: lookPathOnly("xsel"),
		Runner:   runner.Run,
	})

	// act
	require.NoError(t, n.Copy(context.Background(), "hi"))

	// assert
	assert.Equal(t, "xsel", runner.lastName)
	assert.Equal(t, []string{"--clipboard", "--input"}, runner.lastArgs)
}

func TestNative__noToolFound__returnsError(t *testing.T) {
	// arrange
	runner := &fakeRunner{}
	n := clipboard.NewNative(clipboard.NativeOptions{
		GOOS:     "linux",
		Env:      func(string) string { return "" },
		LookPath: lookPathOnly(),
		Runner:   runner.Run,
	})

	// act
	err := n.Copy(context.Background(), "hi")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no native clipboard tool")
	assert.Equal(t, int32(0), runner.calls.Load())
}

func TestNative__runnerError__wrappedWithToolName(t *testing.T) {
	// arrange
	runner := &fakeRunner{err: errors.New("permission denied")}
	n := clipboard.NewNative(clipboard.NativeOptions{
		GOOS:     "darwin",
		Env:      func(string) string { return "" },
		LookPath: lookPathAlways(),
		Runner:   runner.Run,
	})

	// act
	err := n.Copy(context.Background(), "hi")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pbcopy")
	assert.Contains(t, err.Error(), "permission denied")
}

// recordingClipboard is a Clipboard test double that records each payload
// it receives.
type recordingClipboard struct {
	mu    sync.Mutex
	saved []string
	err   error
}

func (r *recordingClipboard) Copy(_ context.Context, payload string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.saved = append(r.saved, payload)
	return nil
}

func (r *recordingClipboard) payloads() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saved == nil {
		return nil
	}
	out := make([]string, len(r.saved))
	copy(out, r.saved)
	return out
}

// rendezvousClipboard requires every concurrent Copy invocation to reach
// the rendezvous before any single Copy can return. Sequential execution
// cannot satisfy the wait, so a deadlock means the transport serialized.
type rendezvousClipboard struct {
	recordingClipboard
	wg *sync.WaitGroup
}

func (g *rendezvousClipboard) Copy(ctx context.Context, payload string) error {
	g.wg.Done()
	g.wg.Wait()
	return g.recordingClipboard.Copy(ctx, payload)
}

// failingWriter always returns its configured error. Used for OSC 52
// writer-error tests.
type failingWriter struct{ err error }

func (f failingWriter) Write(_ []byte) (int, error) { return 0, f.err }

// fakeRunner captures invocations of the native command runner.
type fakeRunner struct {
	mu        sync.Mutex
	calls     atomic.Int32
	err       error
	lastName  string
	lastArgs  []string
	lastStdin []byte
}

func (f *fakeRunner) Run(_ context.Context, name string, args []string, stdin []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls.Add(1)
	f.lastName = name
	if len(args) > 0 {
		f.lastArgs = append([]string(nil), args...)
	} else {
		f.lastArgs = nil
	}
	f.lastStdin = append([]byte(nil), stdin...)
	return f.err
}

// lookPathAlways returns a LookPath that pretends every requested tool
// exists at /fake/<name>.
func lookPathAlways() clipboard.LookPath {
	return func(name string) (string, error) {
		return "/fake/" + name, nil
	}
}

// lookPathOnly returns a LookPath that resolves only the specified set
// of tools and reports os.ErrNotExist for everything else.
func lookPathOnly(names ...string) clipboard.LookPath {
	allowed := make(map[string]struct{}, len(names))
	for _, n := range names {
		allowed[n] = struct{}{}
	}
	return func(name string) (string, error) {
		if _, ok := allowed[name]; ok {
			return "/fake/" + name, nil
		}
		return "", fmt.Errorf("not found: %s", name)
	}
}

func mapEnv(env map[string]string) func(string) string {
	return func(name string) string { return env[name] }
}
