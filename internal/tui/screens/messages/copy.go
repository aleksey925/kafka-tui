package messages

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/recordfmt"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// CopyMenu is the shared 4-item copy popup (Record / Key / Value /
// Headers). It owns the menu state and the clipboard dispatch; the host
// just hands it keys plus the current message and renders its View. The
// list and detail screens both route through it so the popup behaves
// identically regardless of where the user pressed `c`.
//
// All methods on CopyMenu tolerate a nil receiver so hosts can hold the
// pointer as a plain field and let internal tests construct bare
// structs without going through [NewCopyMenu].
type CopyMenu struct {
	menu      *components.Menu
	clipboard Clipboard
	styles    theme.Styles
}

// CopyResult is the side-effect signaled by [CopyMenu.Update]. Toast /
// Warn carry the message the host should surface; both empty means the
// keypress was consumed without producing a user-visible event (menu
// still open, or just closed without dispatching). The host inspects
// [CopyMenu.IsOpen] when it needs to know whether the popup is still
// up.
type CopyResult struct {
	Toast string
	Warn  string
}

// copy popup item indices — kept private to the package; consumers
// don't need to know which slot maps to which payload.
const (
	copyItemRecord = iota
	copyItemKey
	copyItemValue
	copyItemHeaders
)

// copyDispatchTimeout caps a single clipboard write. OSC-52 round-trips
// are typically sub-millisecond, but a stalled terminal mustn't lock
// the cmd loop.
const copyDispatchTimeout = 2 * time.Second

func NewCopyMenu(clipboard Clipboard, styles theme.Styles) *CopyMenu {
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	return &CopyMenu{clipboard: clipboard, styles: styles}
}

// IsOpen reports whether the popup is currently mounted.
func (c *CopyMenu) IsOpen() bool {
	return c != nil && c.menu != nil
}

// Close dismisses the popup without dispatching. Idempotent.
func (c *CopyMenu) Close() {
	if c == nil {
		return
	}
	c.menu = nil
}

// Open mounts the popup. Idempotent — repeated calls while open are a
// no-op so a stuck `c` keypress can't reset cursor position.
func (c *CopyMenu) Open() {
	if c == nil || c.menu != nil {
		return
	}
	items := []components.MenuItem{
		{Label: "Record (with metadata)"},
		{Label: "Key"},
		{Label: "Value"},
		{Label: "Headers"},
	}
	c.menu = components.NewMenu(items,
		components.WithMenuStyles(c.styles),
		components.WithMenuTitle("Copy"),
	)
}

// Bindings advertises the menu's own keymap while it's open — surfaced
// in the hints bar and the help overlay. Returns nil when closed so the
// host falls back to its normal bindings.
func (c *CopyMenu) Bindings() []keymap.Binding {
	if c == nil || c.menu == nil {
		return nil
	}
	return c.menu.Bindings("Copy")
}

// View renders the popup. Width is forwarded to [components.Menu.View];
// callers typically pass 0 to let the menu size to content.
func (c *CopyMenu) View(width int) string {
	if c == nil || c.menu == nil {
		return ""
	}
	return c.menu.View(width)
}

// Update routes one keypress. cur is the message under the host's
// cursor at the moment of dispatch — supplied per-call rather than
// snapshotted at Open so we don't store stale references through live
// refreshes (the popup steals input, so cur can't move underneath us
// anyway).
func (c *CopyMenu) Update(key tea.KeyPressMsg, cur kafka.Message) CopyResult {
	if c == nil || c.menu == nil {
		return CopyResult{}
	}
	c.menu, _ = c.menu.Update(key)
	if c.menu.Canceled() {
		c.menu = nil
		return CopyResult{}
	}
	idx, _, ok := c.menu.Selected()
	if !ok {
		return CopyResult{}
	}
	c.menu = nil
	return c.dispatch(idx, cur)
}

func (c *CopyMenu) dispatch(idx int, cur kafka.Message) CopyResult {
	payload, label, ok := payloadFor(idx, cur)
	if !ok {
		return CopyResult{Warn: fmt.Sprintf("copy: unknown menu index %d", idx)}
	}
	if c.clipboard == nil {
		return CopyResult{Warn: "copy " + label + ": clipboard unavailable"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), copyDispatchTimeout)
	defer cancel()
	if err := c.clipboard.Copy(ctx, payload); err != nil {
		return CopyResult{Warn: "copy " + label + ": " + err.Error()}
	}
	return CopyResult{Toast: "copied " + label + " (" + strconv.Itoa(len(payload)) + " bytes)"}
}

// payloadFor maps a menu index onto the (clipboard payload, human label)
// pair. The bool reports whether the index is recognized — guards
// against new menu items being added without a corresponding case.
func payloadFor(idx int, cur kafka.Message) (string, string, bool) {
	switch idx {
	case copyItemRecord:
		meta := recordfmt.Metadata{
			Topic:     cur.Topic,
			Partition: cur.Partition,
			Offset:    cur.Offset,
			Timestamp: cur.Timestamp,
		}
		return string(recordfmt.EncodeWithMetadata(string(cur.Key), cur.Headers, cur.Value, meta)), "record", true
	case copyItemKey:
		return string(cur.Key), "key", true
	case copyItemValue:
		return string(cur.Value), "value", true
	case copyItemHeaders:
		return copyHeadersPayload(cur.Headers), "headers", true
	}
	return "", "", false
}

// copyHeadersPayload renders the headers section as `name=value\n` per
// line — same shape as the `# Headers` section in the recordfmt format,
// so a paste into the produce form / editor lands cleanly. Distinct
// from the on-screen [headersText] which quotes values for display.
func copyHeadersPayload(headers []kafka.Header) string {
	var b strings.Builder
	for _, h := range headers {
		b.WriteString(h.Key)
		b.WriteByte('=')
		b.Write(h.Value)
		b.WriteByte('\n')
	}
	return b.String()
}
