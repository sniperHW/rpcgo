package main

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
)

type rcpChannel struct {
	socket *netgo.AsynSocket
}

func (c *rcpChannel) SendRequest(ctx context.Context, request *rpcgo.RequestMsg) error {
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

func (c *rcpChannel) IsRetryAbleError(_ error) bool {
	return false
}
