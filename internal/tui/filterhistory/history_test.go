package filterhistory_test

import (
	"reflect"
	"testing"

	"github.com/aleksey925/kafka-tui/internal/tui/filterhistory"
)

func TestPush_DropsEmptyAndWhitespace(t *testing.T) {
	h := filterhistory.New(5)

	h.Push("")
	h.Push("   ")
	h.Push("\t")

	if got := h.Len(); got != 0 {
		t.Fatalf("expected empty history, got len=%d", got)
	}
}

func TestPush_NewestFirstAndLowercased(t *testing.T) {
	h := filterhistory.New(5)

	h.Push("Foo")
	h.Push("BAR")
	h.Push("baz")

	if got := h.Matches(""); !reflect.DeepEqual(got, []string{"baz", "bar", "foo"}) {
		t.Fatalf("unexpected order: %v", got)
	}
}

func TestPush_DedupRotatesToHead(t *testing.T) {
	h := filterhistory.New(5)

	h.Push("foo")
	h.Push("bar")
	h.Push("baz")
	h.Push("BAR") // duplicate of "bar" (case-insensitive) — must rotate, not append

	if got := h.Matches(""); !reflect.DeepEqual(got, []string{"bar", "baz", "foo"}) {
		t.Fatalf("dedup should rotate to head: %v", got)
	}
}

func TestPush_EvictsOldestWhenOverCap(t *testing.T) {
	h := filterhistory.New(3)

	h.Push("a")
	h.Push("b")
	h.Push("c")
	h.Push("d") // evicts "a"

	if got := h.Matches(""); !reflect.DeepEqual(got, []string{"d", "c", "b"}) {
		t.Fatalf("LRU should evict oldest: %v", got)
	}
}

func TestMatches_CaseInsensitivePrefix(t *testing.T) {
	h := filterhistory.New(5)

	h.Push("kafka")
	h.Push("kraken")
	h.Push("redis")

	if got := h.Matches("K"); !reflect.DeepEqual(got, []string{"kraken", "kafka"}) {
		t.Fatalf("prefix match should be case-insensitive and newest-first: %v", got)
	}
	if got := h.Matches("z"); len(got) != 0 {
		t.Fatalf("expected no matches, got %v", got)
	}
}

func TestMatches_EmptyPrefixReturnsAll(t *testing.T) {
	h := filterhistory.New(5)

	h.Push("a")
	h.Push("b")

	if got := h.Matches(""); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("empty prefix should return whole history: %v", got)
	}
}

func TestNew_RejectsNonPositiveCap(t *testing.T) {
	h := filterhistory.New(0)

	h.Push("a")
	h.Push("b")

	if got := h.Len(); got != 1 {
		t.Fatalf("non-positive cap should collapse to 1, got len=%d", got)
	}
	if got := h.Matches(""); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("expected only newest entry: %v", got)
	}
}
