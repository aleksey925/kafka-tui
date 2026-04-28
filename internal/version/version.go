package version

import (
	"fmt"
	"runtime/debug"
)

const shortHashLen = 7

// BuildInfo describes the binary's identity (semver + VCS hash).
type BuildInfo struct {
	Version string
	Commit  string
}

// NewBuildInfo creates a BuildInfo with the given version and the commit hash
// auto-extracted from Go VCS build settings.
func NewBuildInfo(ver string) BuildInfo {
	return BuildInfo{Version: ver, Commit: vcsRevision()}
}

// Display returns "x.y.z (hash)" or "x.y.z" if Commit is empty.
func (b BuildInfo) Display() string {
	if b.Commit == "" {
		return b.Version
	}
	return fmt.Sprintf("%s (%s)", b.Version, b.Commit)
}

func vcsRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	return extractRevision(info.Settings)
}

func extractRevision(settings []debug.BuildSetting) string {
	for _, s := range settings {
		if s.Key == "vcs.revision" && len(s.Value) >= shortHashLen {
			return s.Value[:shortHashLen]
		}
	}
	return ""
}
