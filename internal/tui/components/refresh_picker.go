package components

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/lineedit"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// RefreshPickerMinInterval is the smallest custom interval users can pick.
// Sub-second cadences are not meaningful for an auto-refresh of remote
// Kafka state — they'd hammer the cluster for no benefit.
const RefreshPickerMinInterval = time.Second

// shared key names — extracted as constants so [goconst] doesn't trip when
// these literals appear in both Update switch cases and Bindings entries.
const (
	keyDown = "down"
	keyUp   = "up"
)

// refreshPresets is the fixed preset list rendered as the picker's top
// zone. 0 is the "Manual" sentinel — no ticks at all. The rest are the
// canonical cadences for refreshable screens.
var refreshPresets = []time.Duration{
	0,
	1 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	1 * time.Minute,
	5 * time.Minute,
}

type refreshFocus int

const (
	refreshFocusList refreshFocus = iota
	refreshFocusInput
)

// RefreshPicker is a popup combining a preset list and a free-text custom
// interval input. Tab toggles focus between the two zones; the list takes
// digit shortcuts 1..7 (jump+confirm in one keystroke); the input takes
// readline-emacs editing and validates with [time.ParseDuration] (units
// limited to s/m/h, minimum [RefreshPickerMinInterval]). Like [Menu], the
// picker does not close itself — the host inspects [Selected] / [Canceled]
// after each Update and renders [View] until one returns true.
type RefreshPicker struct {
	title   string
	current time.Duration

	cursor int
	focus  refreshFocus
	input  lineedit.State

	inputErr string

	confirmed bool
	canceled  bool
	picked    time.Duration

	styles theme.Styles
}

// RefreshPickerOption configures a [RefreshPicker] at construction.
type RefreshPickerOption func(*RefreshPicker)

// WithRefreshPickerStyles overrides the default theme.
func WithRefreshPickerStyles(s theme.Styles) RefreshPickerOption {
	return func(p *RefreshPicker) { p.styles = s }
}

// WithRefreshPickerTitle overrides the default "Refresh interval" header.
func WithRefreshPickerTitle(title string) RefreshPickerOption {
	return func(p *RefreshPicker) { p.title = title }
}

// NewRefreshPicker constructs a picker initialized with the currently
// applied interval. When current matches a preset, the cursor lands on it;
// otherwise the input field is prefilled with the current value so the
// user can see and edit it.
func NewRefreshPicker(current time.Duration, opts ...RefreshPickerOption) *RefreshPicker {
	if current < 0 {
		current = 0
	}
	p := &RefreshPicker{
		title:   "Refresh interval",
		current: current,
		styles:  theme.DefaultStyles(),
	}
	for _, opt := range opts {
		opt(p)
	}
	if idx, ok := presetIndex(current); ok {
		p.cursor = idx
	} else {
		// custom value — prefill input so the user can edit it.
		p.input = lineedit.FromString(formatPickerDuration(current), false)
		p.input.Cursor = len(p.input.Runes)
	}
	return p
}

// Presets returns the fixed preset list rendered by the picker. Exposed for
// tests and host-side bindings — the slice is a fresh copy.
func Presets() []time.Duration {
	out := make([]time.Duration, len(refreshPresets))
	copy(out, refreshPresets)
	return out
}

// Selected returns the picked interval after the user confirmed either a
// preset (enter / digit) or a valid custom input (enter). ok is false until
// confirmation; the picker stays open and the host keeps rendering [View].
func (p *RefreshPicker) Selected() (time.Duration, bool) {
	if !p.confirmed {
		return 0, false
	}
	return p.picked, true
}

// Canceled reports whether the user pressed Esc.
func (p *RefreshPicker) Canceled() bool { return p.canceled }

// Reset clears the confirmed / canceled flags so the same picker instance
// can be reused. Most hosts discard the instance after consuming a result;
// Reset is provided for parity with [Menu].
func (p *RefreshPicker) Reset() {
	p.confirmed = false
	p.canceled = false
	p.picked = 0
}

// Update routes a key or paste message. Returns the picker (for chaining)
// and a nil cmd — the picker is purely state-mutating.
func (p *RefreshPicker) Update(msg tea.Msg) (*RefreshPicker, tea.Cmd) {
	if paste, ok := msg.(tea.PasteMsg); ok {
		if p.focus == refreshFocusInput {
			p.input = lineedit.InsertText(p.input, paste.Content)
			p.inputErr = ""
		}
		return p, nil
	}
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return p, nil
	}
	switch key.String() {
	case "esc":
		p.canceled = true
		return p, nil
	case "tab", "shift+tab":
		p.toggleFocus()
		return p, nil
	}
	if p.focus == refreshFocusList {
		p.updateList(key)
	} else {
		p.updateInput(key)
	}
	return p, nil
}

func (p *RefreshPicker) toggleFocus() {
	if p.focus == refreshFocusList {
		p.focus = refreshFocusInput
	} else {
		p.focus = refreshFocusList
	}
}

func (p *RefreshPicker) updateList(key tea.KeyPressMsg) {
	switch key.String() {
	case keyDown, "j":
		p.cursor = (p.cursor + 1) % len(refreshPresets)
	case keyUp, "k":
		p.cursor = (p.cursor - 1 + len(refreshPresets)) % len(refreshPresets)
	case "home":
		p.cursor = 0
	case "end":
		p.cursor = len(refreshPresets) - 1
	case "enter":
		p.picked = refreshPresets[p.cursor]
		p.confirmed = true
	default:
		// digit shortcut: jump+confirm on the matching preset.
		if t := key.Text; len(t) == 1 && t[0] >= '1' && t[0] <= '9' {
			idx, err := strconv.Atoi(t)
			if err == nil && idx >= 1 && idx <= len(refreshPresets) {
				p.cursor = idx - 1
				p.picked = refreshPresets[p.cursor]
				p.confirmed = true
			}
		}
	}
}

func (p *RefreshPicker) updateInput(key tea.KeyPressMsg) {
	if key.String() == "enter" {
		d, err := parseRefreshDuration(p.input.String())
		if err != nil {
			p.inputErr = err.Error()
			return
		}
		p.picked = d
		p.confirmed = true
		return
	}
	state, ok := lineedit.Apply(p.input, key)
	if ok {
		if state.String() != p.input.String() {
			p.inputErr = ""
		}
		p.input = state
	}
}

// parseRefreshDuration validates a custom duration string from the input.
// Returns an error message phrased for inline display under the field.
func parseRefreshDuration(raw string) (time.Duration, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("required (e.g. 30s, 2m, 1h)")
	}
	if !allowedRefreshUnits(s) {
		return 0, errors.New("use s, m, or h (e.g. 30s, 2m, 1h)")
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, errors.New("invalid duration (e.g. 30s, 2m, 1h)")
	}
	if d < RefreshPickerMinInterval {
		return 0, fmt.Errorf("minimum %s", formatPickerDuration(RefreshPickerMinInterval))
	}
	return d, nil
}

// allowedRefreshUnits rejects sub-second units (ns/us/µs/ms) and unsupported
// suffixes (d, w) before [time.ParseDuration] gets a chance — they're either
// too fine-grained for an auto-refresh of remote state or not valid Go
// durations at all. Only s / m / h are allowed.
func allowedRefreshUnits(s string) bool {
	for i := range len(s) {
		c := s[i]
		if c >= '0' && c <= '9' {
			continue
		}
		// reject anything that isn't a digit and isn't one of s/m/h. This
		// catches ns/us/ms (the n/u/m prefix), µs (the µ), and stray junk.
		// "m" alone is valid (minutes), but "ms" is caught by the next-char
		// check below.
		switch c {
		case 's', 'h':
			// last allowed-unit char; anything after it (other than digit
			// starting a new clause) is rejected by this loop iteration.
		case 'm':
			// distinguish "m" (minutes) from "ms" (milliseconds).
			if i+1 < len(s) && s[i+1] == 's' {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// presetIndex returns (idx, true) when d matches a preset exactly.
func presetIndex(d time.Duration) (int, bool) {
	for i, p := range refreshPresets {
		if p == d {
			return i, true
		}
	}
	return 0, false
}

// formatPickerDuration renders a duration for display. Strips trailing
// "0s" / "0m" that [time.Duration.String] leaves behind ("2m0s" → "2m").
// Zero is rendered as "Manual" so the preset list can share this formatter.
func formatPickerDuration(d time.Duration) string {
	if d == 0 {
		return "Manual"
	}
	s := d.String()
	// strip trailing 0s / 0m segments (in that order — 2m0s → 2m → 2m).
	for _, suffix := range []string{"0s", "0m"} {
		if strings.HasSuffix(s, suffix) && len(s) > len(suffix) {
			c := s[len(s)-len(suffix)-1]
			if c < '0' || c > '9' {
				s = strings.TrimSuffix(s, suffix)
			}
		}
	}
	return s
}

// Bindings returns the advertise-only help entries for the picker. Tab and
// Enter / Esc surface in the hint bar; navigation and digits are listed in
// the help overlay only.
func (p *RefreshPicker) Bindings(category string) []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"tab", "shift+tab"}, Label: "switch focus (list ↔ input)", Category: category, Hint: true},
		{Keys: []string{"enter"}, Label: "apply", Category: category, Hint: true},
		{Keys: []string{"esc"}, Label: "cancel", Category: category, Hint: true},
	}
	if p.focus == refreshFocusList {
		bs = append(bs,
			keymap.Binding{Keys: []string{"j", keyDown}, Label: "next preset", Category: category},
			keymap.Binding{Keys: []string{"k", keyUp}, Label: "previous preset", Category: category},
			keymap.Binding{Keys: []string{"home"}, Label: "first preset", Category: category},
			keymap.Binding{Keys: []string{"end"}, Label: "last preset", Category: category},
		)
		n := min(len(refreshPresets), 9)
		if n > 0 {
			digits := make([]string, n)
			for i := range n {
				digits[i] = strconv.Itoa(i + 1)
			}
			bs = append(bs, keymap.Binding{
				Keys:     digits,
				Label:    "jump to preset by index",
				Category: category,
			})
		}
	}
	return bs
}

// View renders the picker body. width<=0 means natural width; otherwise the
// box is horizontally centered in width.
func (p *RefreshPicker) View(width int) string {
	parts := make([]string, 0, len(refreshPresets)+8)
	if p.title != "" {
		parts = append(parts, p.styles.HelpTitle.Render(p.title), "")
	}

	listLabel := "  presets"
	if p.focus == refreshFocusList {
		listLabel = p.styles.HintKey.Render("▸ presets")
	} else {
		listLabel = p.styles.HintLabel.Render(listLabel)
	}
	parts = append(parts, listLabel)
	for i, d := range refreshPresets {
		focused := p.focus == refreshFocusList && i == p.cursor
		prefix := "  "
		labelStyle := p.styles.Command
		if focused {
			prefix = "▸ "
			labelStyle = p.styles.CommandHL
		}
		digit := p.styles.HintKey.Render(strconv.Itoa(i + 1))
		row := "  " + prefix + digit + ". " + labelStyle.Render(formatPickerDuration(d))
		if d == p.current {
			row += "  " + p.styles.HintKey.Render("●")
		}
		parts = append(parts, row)
	}
	parts = append(parts, "")

	customLabel := "  custom"
	if p.focus == refreshFocusInput {
		customLabel = p.styles.HintKey.Render("▸ custom")
	} else {
		customLabel = p.styles.HintLabel.Render(customLabel)
	}
	if _, matched := presetIndex(p.current); !matched && p.current > 0 {
		customLabel += "  " + p.styles.HintKey.Render("●")
	}
	parts = append(parts, customLabel, p.renderInputField())
	if p.inputErr != "" {
		parts = append(parts, "    "+p.styles.StatusErr.Render(p.inputErr))
	}

	parts = append(parts, "",
		p.styles.HintLabel.Render("tab switch · enter apply · esc cancel"))

	body := strings.Join(parts, "\n")
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Foreground(p.styles.Palette.Foreground).
		Render(body)
	if width <= 0 {
		return box
	}
	return lipgloss.PlaceHorizontal(width, lipgloss.Center, box)
}

func (p *RefreshPicker) renderInputField() string {
	if p.focus != refreshFocusInput {
		if len(p.input.Runes) == 0 {
			return "    " + p.styles.HintLabel.Render("(empty)")
		}
		return "    " + p.styles.Command.Render(p.input.String())
	}
	runes := p.input.Runes
	cur := max(0, min(p.input.Cursor, len(runes)))
	before := string(runes[:cur])
	var underCursor, after string
	if cur >= len(runes) {
		underCursor = " "
	} else {
		underCursor = string(runes[cur])
		after = string(runes[cur+1:])
	}
	return "    " + p.styles.Command.Render(before) + p.styles.Cursor.Render(underCursor) + p.styles.Command.Render(after)
}
