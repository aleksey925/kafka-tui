package version

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildInfo_Display(t *testing.T) {
	tests := []struct {
		name  string
		build BuildInfo
		want  string
	}{
		{
			name:  "version with commit",
			build: BuildInfo{Version: "1.2.3", Commit: "abc1234"},
			want:  "1.2.3 (abc1234)",
		},
		{
			name:  "version without commit",
			build: BuildInfo{Version: "1.2.3"},
			want:  "1.2.3",
		},
		{
			name:  "default dev version without commit",
			build: BuildInfo{Version: "0.0.0"},
			want:  "0.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.build.Display())
		})
	}
}

func TestNewBuildInfo_PopulatesVersionAndCommitFromBuild(t *testing.T) {
	// vcsRevision reads the calling binary's debug build info — which under
	// `go test` may or may not contain `vcs.revision` depending on whether
	// the test binary was built from a clean tree. Either way, the version
	// passes through verbatim and the commit field is at most `shortHashLen`.
	bi := NewBuildInfo("9.9.9")

	assert.Equal(t, "9.9.9", bi.Version)
	assert.LessOrEqual(t, len(bi.Commit), shortHashLen)
}

func TestExtractRevision(t *testing.T) {
	tests := []struct {
		name     string
		settings []debug.BuildSetting
		want     string
	}{
		{
			name:     "empty settings",
			settings: nil,
			want:     "",
		},
		{
			name:     "no vcs.revision key",
			settings: []debug.BuildSetting{{Key: "GOOS", Value: "linux"}},
			want:     "",
		},
		{
			name:     "valid revision is truncated to short hash",
			settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc1234567890def"}},
			want:     "abc1234",
		},
		{
			name:     "revision shorter than shortHashLen is rejected",
			settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}},
			want:     "",
		},
		{
			name:     "revision exactly shortHashLen long",
			settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc1234"}},
			want:     "abc1234",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, extractRevision(tt.settings))
		})
	}
}
