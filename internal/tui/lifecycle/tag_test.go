package lifecycle_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/lifecycle"
)

func TestTag_IsForMatchesBothFields(t *testing.T) {
	tag := lifecycle.NewTag(3, "group-a")

	assert.True(t, tag.IsFor(3, "group-a"), "exact match")
	assert.False(t, tag.IsFor(2, "group-a"), "gen mismatch rejected")
	assert.False(t, tag.IsFor(3, "group-b"), "identity mismatch rejected")
	assert.False(t, tag.IsFor(2, "group-b"), "both mismatched rejected")
}

func TestTag_ZeroValueRejectsLiveCheck(t *testing.T) {
	var tag lifecycle.Tag

	// the trap: a re-instantiated sub-model has gen=0 and identity="";
	// a stale result with Gen=0 must still be rejected because identity
	// won't match the live sub-model's "group-a"/"topic-x"/...
	assert.False(t, tag.IsFor(0, "group-a"))
}
