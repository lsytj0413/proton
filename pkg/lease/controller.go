// Package lease ...
package lease

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/exp/slog"
)

// AllocerLeaseController ...
type AllocerLeaseController struct {
	cli *clientv3.Client
	d   time.Duration

	key   string
	value string
}

// NewAllocerLeaseController ...
func NewAllocerLeaseController(
	cli *clientv3.Client,
	key string,
	value string,
	d time.Duration,
) *AllocerLeaseController {
	return &AllocerLeaseController{
		cli:   cli,
		d:     d,
		key:   key,
		value: value,
	}
}

// Run ...
func (c *AllocerLeaseController) Run(ctx context.Context) {
	if c.cli == nil {
		panic("nil cli")
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		sess, err := concurrency.NewSession(c.cli, concurrency.WithContext(ctx), concurrency.WithTTL(int(c.d.Seconds())))
		if err != nil {
			panic(err)
		}

		slog.InfoCtx(ctx, "grant new lease success", slog.Int64("LeaseID", int64(sess.Lease())))

		// 不做 if not exists（CreateRevision=0）、不做更新
		resp, err := c.cli.Put(ctx, c.key, c.value, clientv3.WithLease(sess.Lease()))
		if err != nil {
			panic(err)
		}

		slog.InfoCtx(ctx, "put key success",
			slog.Uint64("ClusterId", resp.Header.ClusterId),
			slog.Uint64("MemberId", resp.Header.MemberId),
			slog.Int64("Rivision", resp.Header.Revision),
		)

		select {
		case <-ctx.Done():
			return
		case <-sess.Done():
			slog.InfoCtx(ctx, "sess has expired, renew it")
		}
	}
}
