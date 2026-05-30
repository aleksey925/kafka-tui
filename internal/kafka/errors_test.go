package kafka

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestClassifyError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   error
		want ErrKind
	}{
		{name: "nil", in: nil, want: ErrKindOther},
		{name: "plain error", in: errors.New("boom"), want: ErrKindOther},
		{name: "topic auth failed", in: kerr.TopicAuthorizationFailed, want: ErrKindACL},
		{name: "group auth failed", in: kerr.GroupAuthorizationFailed, want: ErrKindACL},
		{name: "cluster auth failed", in: kerr.ClusterAuthorizationFailed, want: ErrKindACL},
		{name: "transactional id auth failed", in: kerr.TransactionalIDAuthorizationFailed, want: ErrKindACL},
		{name: "delegation token auth failed", in: kerr.DelegationTokenAuthorizationFailed, want: ErrKindACL},
		{name: "wrapped topic auth", in: fmt.Errorf("describe configs: %w", kerr.TopicAuthorizationFailed), want: ErrKindACL},
		{name: "non-auth kerr", in: kerr.UnknownTopicOrPartition, want: ErrKindOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ClassifyError(tc.in))
		})
	}
}

func TestUnwrapKadmAuthErr(t *testing.T) {
	t.Parallel()

	denied := kerr.TopicAuthorizationFailed
	authErr := &kadm.AuthError{Err: denied}

	gotErr, ok := UnwrapKadmAuthErr(authErr)
	assert.True(t, ok)
	assert.Same(t, denied, gotErr)

	wrapped := fmt.Errorf("kadm describe: %w", authErr)
	gotErr, ok = UnwrapKadmAuthErr(wrapped)
	assert.True(t, ok)
	assert.Same(t, denied, gotErr)

	_, ok = UnwrapKadmAuthErr(errors.New("boom"))
	assert.False(t, ok)

	_, ok = UnwrapKadmAuthErr(nil)
	assert.False(t, ok)
}

func TestCollectDenials__filtersToACL(t *testing.T) {
	t.Parallel()

	results := map[string]BatchResult[[]TopicConfig]{
		"ok":      {Value: []TopicConfig{}},
		"acl":     {Err: kerr.TopicAuthorizationFailed},
		"network": {Err: errors.New("dial tcp: connection refused")},
	}

	got := CollectDenials(RPCKindConfigs, results)
	assert.Equal(t, []Denial{{RPC: RPCKindConfigs, Topic: "acl", Err: ErrKindACL}}, got)
}

func TestClient_RegisterDenials__returnsOnlyFresh(t *testing.T) {
	t.Parallel()

	c := &Client{}
	denials := []Denial{
		{RPC: RPCKindConfigs, Topic: "a", Err: ErrKindACL},
		{RPC: RPCKindConfigs, Topic: "b", Err: ErrKindACL},
	}

	first := c.RegisterDenials(denials)
	sortDenials(first)
	assert.Equal(t, denials, first)

	second := c.RegisterDenials(denials)
	assert.Empty(t, second)

	extended := append([]Denial{}, denials...)
	extended = append(extended, Denial{RPC: RPCKindSize, Topic: "a", Err: ErrKindACL})
	third := c.RegisterDenials(extended)
	assert.Equal(t, []Denial{{RPC: RPCKindSize, Topic: "a", Err: ErrKindACL}}, third)
}

func TestClient_RegisterDenials__empty(t *testing.T) {
	t.Parallel()

	c := &Client{}
	assert.Nil(t, c.RegisterDenials(nil))
}

func sortDenials(ds []Denial) {
	sort.Slice(ds, func(i, j int) bool {
		if ds[i].RPC != ds[j].RPC {
			return ds[i].RPC < ds[j].RPC
		}
		return ds[i].Topic < ds[j].Topic
	})
}
