package lifecycle

// Tag is the (generation, identity) envelope every result destined for
// a re-instantiated sub-model must carry — see CLAUDE.md "Async
// lifecycle and stale results" / "Generation counters in re-instantiated
// sub-models".
//
// A bare generation counter is not enough on its own here. Sub-models
// (DetailModel, popups) that get re-instantiated on entry start their
// counter at zero, so a stale `Gen=3` from the previous instance can
// collide with a fresh `Gen=3` from the new one. The Identity field
// (group name, topic name, …) disambiguates: when it does not match
// the live sub-model's identity, the result is from a previous instance
// and must be dropped.
//
// Embed Tag into the message type and construct via [NewTag]. Handlers
// then make one [Tag.IsFor] call to validate both dimensions.
type Tag struct {
	Gen      uint64
	Identity string
}

// NewTag is the canonical constructor. Calling it instead of a struct
// literal documents that both fields are deliberate.
func NewTag(gen uint64, identity string) Tag {
	return Tag{Gen: gen, Identity: identity}
}

// IsFor reports whether this tag belongs to the given (gen, identity)
// pair. Handlers should consult it to drop stale results that survived
// either a counter mismatch or a sub-model re-instantiation.
func (t Tag) IsFor(gen uint64, identity string) bool {
	return t.Gen == gen && t.Identity == identity
}
