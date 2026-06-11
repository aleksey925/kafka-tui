package components

import (
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/charmbracelet/x/ansi"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// modalContentWidth is the comfortable floor for a confirm modal's inner
// content width: hints fit on a single row and titles have a known canvas
// to center against. The value clears the longest hint line in the app
// ("y send  k send & keep  esc cancel") with breathing room on both sides.
// The actual width grows past this when context values are wider, capped
// proportionally to the terminal (see [modalContentWidthFor]).
const modalContentWidth = 44

// modalMaxWidthRatioNum / modalMaxWidthRatioDen bound the modal to a
// fraction of the terminal width so a wide screen yields a proportionally
// wider popup instead of a fixed small box, while still leaving margin on
// both sides.
const (
	modalMaxWidthRatioNum = 3
	modalMaxWidthRatioDen = 5 // 0.6 of the terminal width
)

// modalTopBias lifts the modal this many rows above the center of the
// region it is placed in. That region sits below layout.HeaderRows of header
// and above a 1-row flash bar, so dead center lands ~2 rows low; biasing up
// re-centers it on screen with a touch of headroom.
const modalTopBias = 3

// modalField is one labeled context line in a confirm-style modal (Topic:
// <name>, Cluster: <name>, From/To, ...). It is the single shape both
// [Confirm] and [SendConfirm] feed into the shared renderer so long values
// wrap and short values stay inline identically across every modal.
type modalField struct {
	Label string
	Value string
}

// renderModal lays out a confirm-style modal as a centered rounded box and
// is the single place box geometry / centering lives (see § "Confirm for
// destructive actions" and the single-source rule in CLAUDE.md). Every body
// line is sized to the same content width, so the title and hints center
// against the real box width instead of a fixed canvas — a long context
// value can no longer shift them off center. note is the optional trailing
// sentence (e.g. "This cannot be undone."); hint is the pre-rendered hint
// line. width and height are the body-area dimensions to center within;
// pass 0 on an axis to skip placement there.
func renderModal(s theme.Styles, title string, fields []modalField, note, hint string, width, height int) string {
	cw := modalContentWidthFor(title, fields, note, hint, width)

	body := []string{lipgloss.PlaceHorizontal(cw, lipgloss.Center, s.HelpTitle.Render(title)), ""}

	labelCol := modalLabelColumn(fields)
	for _, f := range fields {
		body = append(body, modalFieldLines(s, f, labelCol, cw)...)
	}

	if note != "" {
		if len(fields) > 0 {
			body = append(body, "")
		}
		body = append(body, padRight(s.Command.Render(note), cw))
	}

	body = append(body, "", lipgloss.PlaceHorizontal(cw, lipgloss.Center, hint))

	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Foreground(s.Palette.Foreground).
		Render(strings.Join(body, "\n"))

	placed := box
	if width > 0 {
		placed = lipgloss.PlaceHorizontal(width, lipgloss.Center, placed)
	}
	if height > 0 {
		placed = placeVerticalBiased(placed, height, modalTopBias)
	}
	return placed
}

// placeVerticalBiased centers content within height but shifts it up by bias
// rows (clamped to the available slack), padding above and below with blank
// lines so the result is exactly height rows tall.
func placeVerticalBiased(content string, height, bias int) string {
	slack := height - lipgloss.Height(content)
	if slack <= 0 {
		return content
	}
	above := min(max(slack/2-bias, 0), slack)
	return strings.Repeat("\n", above) + content + strings.Repeat("\n", slack-above)
}

// modalContentWidthFor resolves the inner content width: it grows with the
// widest inline element, never drops below the floor that keeps hints on one
// row, and is capped at a fraction of the terminal width so the popup scales
// with the screen instead of staying fixed. screenWidth <= 0 (no placement,
// e.g. in tests) returns the natural width uncapped.
func modalContentWidthFor(title string, fields []modalField, note, hint string, screenWidth int) int {
	floor := max(modalContentWidth, lipgloss.Width(title), lipgloss.Width(hint))

	labelCol := modalLabelColumn(fields)
	natural := floor
	for _, f := range fields {
		natural = max(natural, lipgloss.Width(modalFieldInline(f, labelCol)))
	}
	natural = max(natural, lipgloss.Width(note))

	if screenWidth <= 0 {
		return natural
	}

	ceiling := max(screenWidth*modalMaxWidthRatioNum/modalMaxWidthRatioDen, floor)
	return min(max(natural, floor), ceiling)
}

// modalLabelColumn is the width the "Label:" prefix is padded to so values
// align in a column when several fields render inline.
func modalLabelColumn(fields []modalField) int {
	col := 0
	for _, f := range fields {
		col = max(col, lipgloss.Width(f.Label+":"))
	}
	return col
}

// modalFieldInline renders a field on a single line: "Label:" padded to the
// shared label column, then two spaces, then the value.
func modalFieldInline(f modalField, labelCol int) string {
	label := f.Label + ":"
	if pad := labelCol - lipgloss.Width(label); pad > 0 {
		label += strings.Repeat(" ", pad)
	}
	return label + "  " + f.Value
}

// modalFieldLines renders a field, picking the layout by whether it fits:
// short values stay inline ("Topic:  orders"); a value too wide for the
// content drops to its own wrapped block under a bare "Label:" line so the
// box never has to stretch to the full length of a long identifier.
func modalFieldLines(s theme.Styles, f modalField, labelCol, contentWidth int) []string {
	if inline := modalFieldInline(f, labelCol); lipgloss.Width(inline) <= contentWidth {
		return []string{padRight(s.Command.Render(inline), contentWidth)}
	}

	lines := []string{padRight(s.Command.Render(f.Label+":"), contentWidth)}
	for _, vl := range wrapIdentifier(f.Value, contentWidth) {
		lines = append(lines, padRight(s.Command.Render(vl), contentWidth))
	}
	return lines
}

// wrapIdentifier breaks value to fit width, preferring to break after the
// `.`, `-`, `_` separators common in Kafka topic / group names so line
// boundaries line up with the logical parts of the name. A single segment
// longer than width falls back to a hard character break.
func wrapIdentifier(value string, width int) []string {
	if width <= 0 || lipgloss.Width(value) <= width {
		return []string{value}
	}

	var lines []string
	cur := ""
	flush := func(s string) {
		if lipgloss.Width(cur)+lipgloss.Width(s) > width && cur != "" {
			lines = append(lines, cur)
			cur = ""
		}
		if cur == "" && lipgloss.Width(s) > width {
			parts := strings.Split(ansi.Hardwrap(s, width, false), "\n")
			lines = append(lines, parts[:len(parts)-1]...)
			cur = parts[len(parts)-1]
			return
		}
		cur += s
	}
	for _, tok := range tokenizeIdentifier(value) {
		flush(tok)
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	return lines
}

// tokenizeIdentifier splits s into runs that each end with one of the
// `.`, `-`, `_` separators (the trailing run keeps no separator), so the
// separator stays attached to its preceding segment and a break after it
// reads naturally.
func tokenizeIdentifier(s string) []string {
	var toks []string
	var b strings.Builder
	for _, r := range s {
		b.WriteRune(r)
		if r == '.' || r == '-' || r == '_' {
			toks = append(toks, b.String())
			b.Reset()
		}
	}
	if b.Len() > 0 {
		toks = append(toks, b.String())
	}
	return toks
}

// padRight right-pads s with spaces to width so every body line shares the
// box's content width, keeping centered lines (title, hints) aligned to the
// real box edges. Lines already at or over width are returned unchanged.
func padRight(s string, width int) string {
	if pad := width - lipgloss.Width(s); pad > 0 {
		return s + strings.Repeat(" ", pad)
	}
	return s
}
