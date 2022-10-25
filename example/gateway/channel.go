package main

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
)

type rcpChannel struct {
	socket *netgo.AsynSocket
}

func (c *rcpChannel) SendRequest(request *rpcgo.RequestMsg, deadline time.Time) error {
	return c.socket.Send(request, deadline)
}

func (c *rcpChannel) SendRequestWithContext(ctx context.Context, request *rpcgo.RequestMsg) error {
	return c.socket.SendWithContext(ctx, request)
}

func (c *rcpChannel) Reply(response *rpcgo.ResponseMsg) error {
	return c.socket.Send(response)
}

func (c *rcpChannel) Name() string {
	return fmt.Sprintf("%v <-> %v", c.socket.LocalAddr(), c.socket.RemoteAddr())
}

func (c *rcpChannel) Identity() uint64 {
	return *(*uint64)(unsafe.Pointer(c.socket))
}
