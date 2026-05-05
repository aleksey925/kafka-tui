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

	w, err := o.acquireWriter()
	if err != nil {
		return err
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(payload))
	if _, err := io.WriteString(w, osc52Prefix+encoded+osc52Terminator); err != nil {
		return fmt.Errorf("clipboard: osc52 write: %w", err)
	}
	return nil
}

func (o *OSC52) acquireWriter() (io.Writer, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
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
	// stdin), fall back to stderr. Stderr usually shares the terminal
	// and OSC sequences are silently consumed by the terminal emulator.
	if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, os.ErrPermission) {
		// unknown failure mode — surface it
		return nil, fmt.Errorf("clipboard: open /dev/tty: %w", err)
	}
	o.writer = os.Stderr
	return o.writer, nil
}
