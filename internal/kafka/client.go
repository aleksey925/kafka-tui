package kafka

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksey925/kafka-tui/internal/config"
)

// ErrReadOnly is returned by every mutating method when the underlying cluster
// is marked read_only. UI screens also short-circuit destructive actions
// before mounting modals, but this is the load-bearing guard: a CLI path or
// future direct call still cannot mutate a read-only cluster.
var ErrReadOnly = errors.New("kafka: cluster is read-only")

// ErrTopicNotFound and ErrGroupNotFound are returned by methods that look
// up an entity by name when the broker says the entity isn't there. They
// are sentinels — callers MAY use errors.Is to render a more specific
// message ("topic deleted between refreshes") instead of the generic
// error text. Current UI surfaces the wrapped text as-is.
var (
	ErrTopicNotFound = errors.New("kafka: topic not found")
	ErrGroupNotFound = errors.New("kafka: group not found")
)

// Client wraps a franz-go client and its kadm admin counterpart.
type Client struct {
	kc       *kgo.Client
	adm      *kadm.Client
	cluster  config.Cluster
	protocol Protocol
}

func newClient(kc *kgo.Client, cluster config.Cluster, proto Protocol) *Client {
	return &Client{
		kc:       kc,
		adm:      kadm.NewClient(kc),
		cluster:  cluster,
		protocol: proto,
	}
}

func (c *Client) Cluster() config.Cluster { return c.cluster }

func (c *Client) Protocol() Protocol { return c.protocol }

func (c *Client) Kgo() *kgo.Client { return c.kc }

func (c *Client) Admin() *kadm.Client { return c.adm }

// ReadOnly reports whether the underlying cluster is marked read_only.
func (c *Client) ReadOnly() bool { return c.cluster.ReadOnly }

// ensureWritable returns ErrReadOnly when the cluster is read_only. Every
// mutating method must call this before issuing the request.
func (c *Client) ensureWritable() error {
	if c.cluster.ReadOnly {
		return ErrReadOnly
	}
	return nil
}

// Close shuts down the underlying clients. Safe to call multiple times.
func (c *Client) Close() {
	if c == nil || c.kc == nil {
		return
	}
	c.kc.Close()
	c.kc = nil
	c.adm = nil
}
