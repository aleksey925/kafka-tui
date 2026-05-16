package layout

import "charm.land/lipgloss/v2"

// PlaceCenteredTop horizontally centers popup inside a region of the
// given width, anchoring it to the top of a region of the given height.
// width<=0 returns popup unchanged; bodyHeight<=0 skips vertical
// placement (popup is only centered horizontally). Used by screens that
// overlay modal menus / forms onto a fixed body area.
func PlaceCenteredTop(width, bodyHeight int, popup string) string {
	if width <= 0 {
		return popup
	}
	centered := lipgloss.PlaceHorizontal(width, lipgloss.Center, popup)
	if bodyHeight <= 0 {
		return centered
	}
	return lipgloss.PlaceVertical(bodyHeight, lipgloss.Top, centered)
}
