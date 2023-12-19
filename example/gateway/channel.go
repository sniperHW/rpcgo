package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
)

type rcpChannel struct {
	socket *netgo.AsynSocket
}

func (c *rcpChannel) RequestWithContext(ctx context.Context, request *rpcgo.RequestMsg) error {
	return c.socket.SendWithContext(ctx, request)
}

func (c *rcpChannel) Request(request *rpcgo.RequestMsg) error {
	return c.socket.Send(request, time.Time{})
}

func (c *rcpChannel) Reply(response *rpcgo.ResponseMsg) error {
	return c.socket.Send(response)
}

func (c *rcpChannel) Name() string {
	return fmt.Sprintf("%v <-> %v", c.socket.LocalAddr(), c.socket.RemoteAddr())
}

func (c *rcpChannel) IsRetryAbleError(_ error) bool {
	return false
}
