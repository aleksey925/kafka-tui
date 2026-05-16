package kafka

import (
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksey925/kafka-tui/internal/config"
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

// Close shuts down the underlying clients. Safe to call multiple times.
func (c *Client) Close() {
	if c == nil || c.kc == nil {
		return
	}
	c.kc.Close()
	c.kc = nil
	c.adm = nil
}
