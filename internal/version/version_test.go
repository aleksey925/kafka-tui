package version

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormat__withCommit__includesParens(t *testing.T) {
	// arrange + act
	got := Format("v0.7.3", "a1b2c3d")

	// assert
	assert.Equal(t, "v0.7.3 (a1b2c3d)", got)
}

func TestFormat__withoutCommit__returnsVersionOnly(t *testing.T) {
	// arrange + act
	got := Format("v0.7.3", "")

	// assert
	assert.Equal(t, "v0.7.3", got)
}

func TestFormat__devVersion(t *testing.T) {
	// arrange + act
	got := Format("dev", "abcdef0")

	// assert
	assert.Equal(t, "dev (abcdef0)", got)
}
