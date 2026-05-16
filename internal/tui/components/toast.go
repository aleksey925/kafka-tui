package components

import (
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ToastLevel categorizes a toast for color and lifetime decisions.
type ToastLevel int

const (
	ToastSuccess ToastLevel = iota
	ToastInfo
	ToastWarning
	// ToastError is red and sticky (no auto-dismiss). Cleared on key/esc.
	ToastError
)

// DefaultToastLifetimes maps each level to its auto-dismiss duration. Zero
// means sticky.
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

func (t Toast) Sticky() bool { return t.Lifetime == 0 }

func (t Toast) expired(now time.Time) bool {
	if t.Sticky() {
		return false
	}
	return now.Sub(t.CreatedAt) >= t.Lifetime
}

// Toasts is the toast queue rendered as a stack in a screen corner. Sticky
// toasts (errors) are dismissed via DismissTopSticky on any keypress.
type Toasts struct {
	items  []Toast
	now    func() time.Time
	styles theme.Styles
}

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

type ToastsOption func(*Toasts)

func WithToastClock(now func() time.Time) ToastsOption {
	return func(t *Toasts) {
		if now != nil {
			t.now = now
		}
	}
}

func WithToastStyles(s theme.Styles) ToastsOption {
	return func(t *Toasts) { t.styles = s }
}

// Push appends a toast with the level's default lifetime.
func (t *Toasts) Push(level ToastLevel, message string) {
	t.PushWithLifetime(level, message, DefaultToastLifetimes[level])
}

// PushWithLifetime is the explicit-lifetime variant. Pass 0 for sticky.
func (t *Toasts) PushWithLifetime(level ToastLevel, message string, lifetime time.Duration) {
	t.items = append(t.items, Toast{
		Level:     level,
		Message:   message,
		CreatedAt: t.now(),
		Lifetime:  lifetime,
	})
}

func (t *Toasts) Items() []Toast {
	out := make([]Toast, len(t.items))
	copy(out, t.items)
	return out
}

func (t *Toasts) Len() int { return len(t.items) }

// Latest returns the most recently pushed live toast (after pruning expired
// non-sticky entries).
func (t *Toasts) Latest() (Toast, bool) {
	t.Tick()
	if len(t.items) == 0 {
		return Toast{}, false
	}
	return t.items[len(t.items)-1], true
}

// Tick prunes expired non-sticky toasts. Call from the screen Update loop.
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

// DismissTopSticky removes the most recent sticky (error) toast.
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
func (t *Toasts) Update(msg tea.Msg) (*Toasts, tea.Cmd) {
	t.Tick()
	if _, ok := msg.(tea.KeyPressMsg); ok {
		t.DismissTopSticky()
	}
	return t, nil
}

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
	}
	style := lipgloss.NewStyle().Foreground(color).Bold(true)
	return style.Render("["+tag+"]") + " " + t.styles.Command.Render(item.Message)
}
