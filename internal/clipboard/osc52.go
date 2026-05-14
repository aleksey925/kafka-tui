package clipboard

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// OSC 52 framing constants. The sequence is:
//
//	ESC ] 52 ; <selection> ; <base64 payload> BEL
//
// We use the BEL terminator (rather than ST = ESC \) because it is
// supported by every modern terminal we care about and keeps the sequence
// a single byte shorter.
const (
	osc52Prefix     = "\x1b]52;c;"
	osc52Terminator = "\x07"
	// OSC52MaxBytes is the conservative payload limit observed across
	// xterm / iTerm2 / Kitty / Alacritty (they typically accept ~100 KB
	// of base64). We refuse longer payloads with a clear error rather
	// than silently emitting a truncated sequence.
	OSC52MaxBytes = 75_000
)

type OSC52 struct {
	mu     sync.Mutex
	writer io.Writer
	owned  io.Closer // when non-nil, closed by Close()
}

// DefaultOSC52 opens /dev/tty lazily on first use, falling back to os.Stderr
// if the tty is unavailable.
func DefaultOSC52() *OSC52 {
	return &OSC52{}
}

func (o *OSC52) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.owned == nil {
		return nil
	}
	err := o.owned.Close()
	o.owned = nil
	o.writer = nil
	if err != nil {
		return fmt.Errorf("clipboard: close osc52 writer: %w", err)
	}
	return nil
}

func (o *OSC52) Copy(ctx context.Context, payload string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("clipboard: osc52: %w", err)
	}
	if len(payload) > OSC52MaxBytes {
		return fmt.Errorf("clipboard: osc52 payload too large (%d > %d bytes)", len(payload), OSC52MaxBytes)
	}
	// hold the mutex across both the writer acquisition AND the write so two
	// concurrent Copy calls (e.g. parallelClipboard fanout, or a user mashing
	// the copy hotkey) can't interleave their base64 payloads into a single
	// corrupt OSC sequence. Writing the full sequence usually fits in one
	// syscall, so the critical section stays short.
	o.mu.Lock()
	defer o.mu.Unlock()
	w, err := o.acquireWriterLocked()
	if err != nil {
		return err
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(payload))
	if _, err := io.WriteString(w, osc52Prefix+encoded+osc52Terminator); err != nil {
		return fmt.Errorf("clipboard: osc52 write: %w", err)
	}
	return nil
}

// acquireWriterLocked must be called with o.mu held.
func (o *OSC52) acquireWriterLocked() (io.Writer, error) {
	if o.writer != nil {
		return o.writer, nil
	}

	tty, err := os.OpenFile("/dev/tty", os.O_WRONLY, 0)
	if err == nil {
		o.writer = tty
		o.owned = tty
		return o.writer, nil
	}
	// If /dev/tty is unavailable (Windows, sandboxed CI, redirected
	// stdin), fall back to stderr — but only when stderr is itself a
	// terminal. Writing the OSC52 escape + base64 payload into a
	// redirected stderr (e.g. `kafka-tui 2>err.log`) would leak the
	// copied content into a regular file where anyone with read access
	// could decode it.
	if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, os.ErrPermission) {
		// unknown failure mode — surface it
		return nil, fmt.Errorf("clipboard: open /dev/tty: %w", err)
	}
	if !isTerminal(os.Stderr) {
		return nil, errors.New("clipboard: no tty available for osc52")
	}
	o.writer = os.Stderr
	return o.writer, nil
}

// isTerminal reports whether f points at a character device — enough of a
// TTY check for our purposes. Avoids pulling in golang.org/x/term just for
// this single guard.
func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeCharDevice != 0
}
