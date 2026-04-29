package components

import (
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ToastLevel categorizes a toast for color and lifetime decisions
// (specification §7.11).
type ToastLevel int

const (
	// ToastSuccess is a green confirmation, default lifetime 3s.
	ToastSuccess ToastLevel = iota
	// ToastInfo is a neutral notice, default lifetime 3s.
	ToastInfo
	// ToastWarning is yellow, default lifetime 5s.
	ToastWarning
	// ToastError is red and sticky (no auto-dismiss). Cleared on esc/key.
	ToastError
)

// DefaultToastLifetimes maps each level to its auto-dismiss duration. An
// error toast has zero lifetime, meaning "sticky".
var DefaultToastLifetimes = map[ToastLevel]time.Duration{
	ToastSuccess: 3 * time.Second,
	ToastInfo:    3 * time.Second,
	ToastWarning: 5 * time.Second,
	ToastError:   0,
}

// Toast is one in-flight notification. The Toasts list owns lifecycle.
type Toast struct {
	Level     ToastLevel
	Message   string
	CreatedAt time.Time
	Lifetime  time.Duration // 0 = sticky
}

// Sticky reports whether the toast must be explicitly dismissed.
func (t Toast) Sticky() bool { return t.Lifetime == 0 }

// expired reports whether `now` is past the toast's lifetime.
func (t Toast) expired(now time.Time) bool {
	if t.Sticky() {
		return false
	}
	return now.Sub(t.CreatedAt) >= t.Lifetime
}

// Toasts is the toast queue rendered as a stack in a screen corner.
//
// Update prunes expired entries given the current clock; Push appends a new
// toast. Sticky toasts (errors) are dismissed via DismissTopSticky on any
// keypress, per the spec.
type Toasts struct {
	items  []Toast
	now    func() time.Time
	styles theme.Styles
}

// NewToasts constructs an empty toast queue.
func NewToasts(opts ...ToastsOption) *Toasts {
	t := &Toasts{
		now:    time.Now,
		styles: theme.DefaultStyles(),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// ToastsOption configures the queue.
type ToastsOption func(*Toasts)

// WithToastClock injects a clock (handy for deterministic tests).
func WithToastClock(now func() time.Time) ToastsOption {
	return func(t *Toasts) {
		if now != nil {
			t.now = now
		}
	}
}

// WithToastStyles overrides the theme styles.
func WithToastStyles(s theme.Styles) ToastsOption {
	return func(t *Toasts) { t.styles = s }
}

// Push appends a toast. The lifetime defaults to DefaultToastLifetimes[level]
// when zero.
func (t *Toasts) Push(level ToastLevel, message string) {
	t.PushWithLifetime(level, message, DefaultToastLifetimes[level])
}

// PushWithLifetime is the explicit-lifetime variant of Push. Pass 0 to make
// the toast sticky.
func (t *Toasts) PushWithLifetime(level ToastLevel, message string, lifetime time.Duration) {
	t.items = append(t.items, Toast{
		Level:     level,
		Message:   message,
		CreatedAt: t.now(),
		Lifetime:  lifetime,
	})
}

// Items returns the current toast list (defensive copy).
func (t *Toasts) Items() []Toast {
	out := make([]Toast, len(t.items))
	copy(out, t.items)
	return out
}

// Len returns the number of live toasts.
func (t *Toasts) Len() int { return len(t.items) }

// Tick prunes expired non-sticky toasts. Call from the screen Update loop on
// every tea.Msg (including timers).
func (t *Toasts) Tick() {
	now := t.now()
	kept := t.items[:0]
	for _, item := range t.items {
		if !item.expired(now) {
			kept = append(kept, item)
		}
	}
	t.items = kept
}

// DismissTopSticky removes the most recent sticky (error) toast. Returns
// true if one was removed. Used on any keypress per §7.11.
func (t *Toasts) DismissTopSticky() bool {
	for i := len(t.items) - 1; i >= 0; i-- {
		if t.items[i].Sticky() {
			t.items = append(t.items[:i], t.items[i+1:]...)
			return true
		}
	}
	return false
}

// Update routes a key message: any keypress dismisses one sticky toast.
// Non-key messages are ignored.
func (t *Toasts) Update(msg tea.Msg) (*Toasts, tea.Cmd) {
	t.Tick()
	if _, ok := msg.(tea.KeyPressMsg); ok {
		t.DismissTopSticky()
	}
	return t, nil
}

// View renders all live toasts stacked vertically.
func (t *Toasts) View() string {
	if len(t.items) == 0 {
		return ""
	}
	lines := make([]string, 0, len(t.items))
	for _, item := range t.items {
		lines = append(lines, t.renderToast(item))
	}
	return strings.Join(lines, "\n")
}

func (t *Toasts) renderToast(item Toast) string {
	color := t.styles.Palette.Foreground
	tag := "INFO"
	switch item.Level {
	case ToastSuccess:
		color = t.styles.Palette.StatusOK
		tag = "OK"
	case ToastWarning:
		color = t.styles.Palette.StatusWarn
		tag = "WARN"
	case ToastError:
		color = t.styles.Palette.StatusError
		tag = "ERR"
	case ToastInfo:
		// keep defaults (foreground + INFO tag)
	}
	style := lipgloss.NewStyle().Foreground(color).Bold(true)
	return style.Render("["+tag+"]") + " " + t.styles.Command.Render(item.Message)
}
