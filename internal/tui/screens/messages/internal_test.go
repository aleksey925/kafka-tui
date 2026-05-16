package messages

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiveSpinnerFrame_HandlesAllInts(t *testing.T) {
	cases := []int{
		0, 1, len(liveSpinnerFrames) - 1, len(liveSpinnerFrames), len(liveSpinnerFrames) + 1,
		-1, -len(liveSpinnerFrames), -len(liveSpinnerFrames) - 1,
		math.MaxInt, math.MinInt,
	}
	for _, i := range cases {
		out := liveSpinnerFrame(i)
		require.NotEmpty(t, out, "frame %d returned empty string", i)
		// every result must be one of the predefined frames.
		assert.Contains(t, string(liveSpinnerFrames), out, "frame %d returned %q which is not in the spinner sequence", i, out)
	}
}

func TestLiveSpinnerFrame_AdvancesByOnePerStep(t *testing.T) {
	// classic monotonic check: starting from 0, each successive index
	// returns the next frame in the sequence and wraps after the last.
	for i := range len(liveSpinnerFrames) * 2 {
		want := string(liveSpinnerFrames[i%len(liveSpinnerFrames)])
		assert.Equal(t, want, liveSpinnerFrame(i))
	}
}
