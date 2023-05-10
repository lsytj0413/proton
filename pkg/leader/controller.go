// Package leader ...
package leader

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Controller ...
type Controller struct {
	key      string
	value    string
	cli      *clientv3.Client
	leaderCh chan bool
}

// NewController ...
func NewController(cli *clientv3.Client, key string, value string, leaderCh chan bool) *Controller {
	return &Controller{
		key:      key,
		value:    value,
		cli:      cli,
		leaderCh: leaderCh,
	}
}

// Run ...
func (c *Controller) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		sess, err := concurrency.NewSession(c.cli, concurrency.WithContext(ctx), concurrency.WithTTL(10))
		if err != nil {
			panic(err)
		}

		elec := concurrency.NewElection(sess, c.key)

		go func() {
			resCh := elec.Observe(ctx)
			for resp := range resCh {
				if elec.Key() == string(resp.Kvs[0].Key) {
					c.leaderCh <- true
				} else {
					c.leaderCh <- false
				}
			}
		}()

		err = elec.Campaign(ctx, c.value)
		if err != nil {
			panic(err)
		}
	}
}
