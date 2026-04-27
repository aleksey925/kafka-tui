package version

import "fmt"

// Format renders the human-readable version string in the form "v0.7.3 (a1b2c3d)".
// If commit is empty, only the version is rendered.
func Format(version, commit string) string {
	if commit == "" {
		return version
	}
	return fmt.Sprintf("%s (%s)", version, commit)
}
