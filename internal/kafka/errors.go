package kafka

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// ErrKind classifies a broker error for the UI's per-topic batch-failure
// path — see § Per-topic batch failures in CLAUDE.md.
type ErrKind int

const (
	ErrKindOther ErrKind = iota
	ErrKindACL
)

// UnwrapKadmAuthErr peels *kadm.AuthError off err's chain. kadm hides
// per-topic auth denials behind this top-level wrapper; without
// unwrapping, our batch callers misclassify the whole RPC as failed
// and lose the per-topic marker / dedup path.
func UnwrapKadmAuthErr(err error) (error, bool) {
	var ae *kadm.AuthError
	if !errors.As(err, &ae) {
		return nil, false
	}
	return ae.Err, true
}

// ClassifyError maps a broker error to an [ErrKind]. nil maps to
// [ErrKindOther] — callers should branch on err != nil first.
func ClassifyError(err error) ErrKind {
	if err == nil {
		return ErrKindOther
	}
	switch {
	case errors.Is(err, kerr.TopicAuthorizationFailed),
		errors.Is(err, kerr.GroupAuthorizationFailed),
		errors.Is(err, kerr.ClusterAuthorizationFailed),
		errors.Is(err, kerr.TransactionalIDAuthorizationFailed),
		errors.Is(err, kerr.DelegationTokenAuthorizationFailed):
		return ErrKindACL
	}
	return ErrKindOther
}

// BatchResult is the per-topic outcome of a batch RPC: a value or the
// error explaining why it's missing.
type BatchResult[T any] struct {
	Value T
	Err   error
}

// RPCKind tags which family of batch RPC a [Denial] came from so the
// same topic denied across two RPCs surfaces as two events.
type RPCKind int

const (
	RPCKindConfigs RPCKind = iota
	RPCKindSize
	RPCKindWatermarks
)

func (k RPCKind) String() string {
	switch k {
	case RPCKindConfigs:
		return "configs"
	case RPCKindSize:
		return "size"
	case RPCKindWatermarks:
		return "watermarks"
	}
	return "unknown"
}

// Denial is the cache key for one per-topic batch-RPC failure.
type Denial struct {
	RPC   RPCKind
	Topic string
	Err   ErrKind
}

// CollectDenials emits one [Denial] per per-topic ACL failure. Non-ACL
// errors are skipped: they surface through the top-level batch-warning
// path (see [Client.RegisterDenials] for the dedup contract).
func CollectDenials[T any](rpc RPCKind, results map[string]BatchResult[T]) []Denial {
	out := make([]Denial, 0, len(results))
	for topic, r := range results {
		if r.Err == nil {
			continue
		}
		if ClassifyError(r.Err) != ErrKindACL {
			continue
		}
		out = append(out, Denial{RPC: rpc, Topic: topic, Err: ErrKindACL})
	}
	return out
}
