package cli

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuplicateBinaries__otherBinariesOnPath__returnsThem(t *testing.T) {
	// arrange
	dir1, dir2, dir3 := newBinaryDir(t), newBinaryDir(t), newBinaryDir(t)
	current := filepath.Join(dir1, binaryName)
	t.Setenv("PATH", strings.Join([]string{dir1, dir2, dir3}, string(os.PathListSeparator)))

	// act
	got := duplicateBinaries(current)

	// assert
	want := []string{filepath.Join(dir2, binaryName), filepath.Join(dir3, binaryName)}
	slices.Sort(got)
	slices.Sort(want)
	assert.Equal(t, want, got)
}

func TestDuplicateBinaries__noDuplicates__returnsEmpty(t *testing.T) {
	// arrange
	dir := newBinaryDir(t)
	t.Setenv("PATH", dir)

	// act
	got := duplicateBinaries(filepath.Join(dir, binaryName))

	// assert
	assert.Empty(t, got)
}

func newBinaryDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, binaryName), []byte("binary"), 0o755))
	return dir
}
