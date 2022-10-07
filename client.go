package rpcgo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type RPCResponseHandler func(interface{}, *Error)

type callContext struct {
	seq           uint64
	onResponse    RPCResponseHandler
	deadlineTimer atomic.Value
	fired         int32
}

func (this *callContext) callOnResponse(response interface{}, err *Error) {
	if atomic.CompareAndSwapInt32(&this.fired, 0, 1) {
		this.onResponse(response, err)
	}
}

func (this *callContext) stopTimer() {
	if t, ok := this.deadlineTimer.Load().(*time.Timer); ok {
		t.Stop()
	}
}

type RPCClient struct {
	nextSequence uint64
	pendingCall  []sync.Map
}

func NewClient() *RPCClient {
	return &RPCClient{
		pendingCall: make([]sync.Map, 32),
	}
}

func (c *RPCClient) OnRPCMessage(resp *RPCResponseMessage) {
	idx := int(resp.Seq) % len(c.pendingCall)
	if ctx, ok := c.pendingCall[idx].LoadAndDelete(resp.Seq); ok {
		ctx.(*callContext).stopTimer()
		ctx.(*callContext).callOnResponse(resp.Ret, resp.Err)
	} else {
		logger.Sugar().Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (c *RPCClient) AsynCall(channel RPCChannel, req *RPCRequestMessage, deadline time.Time, cb RPCResponseHandler) func() bool {
	req.Seq = atomic.AddUint64(&c.nextSequence, 1)

	timeout := deadline.Sub(time.Now())
	ctx := &callContext{
		onResponse: cb,
		seq:        req.Seq,
	}

	idx := int(req.Seq) % len(c.pendingCall)

	ctx.deadlineTimer.Store(time.AfterFunc(timeout, func() {
		c.pendingCall[idx].Delete(req.Seq)
		ctx.callOnResponse(nil, NewError(ErrTimeout, "timeout"))
	}))

	c.pendingCall[idx].Store(req.Seq, ctx)

	if err := channel.SendRequest(req, deadline); err != nil {
		if _, ok := c.pendingCall[idx].LoadAndDelete(req.Seq); ok {
			ctx.stopTimer()
			go ctx.callOnResponse(nil, NewError(ErrSend, err.Error()))
		}
		return func() bool { return false }
	} else {
		return func() bool {
			if _, ok := c.pendingCall[idx].LoadAndDelete(req.Seq); ok {
				ctx.stopTimer()
				return true
			} else {
				return false
			}
		}
	}
}

func (c *RPCClient) Call(ctx context.Context, channel RPCChannel, req *RPCRequestMessage) (ret interface{}, err *Error) {
	req.Seq = atomic.AddUint64(&c.nextSequence, 1)
	waitC := make(chan []interface{})
	idx := int(req.Seq) % len(c.pendingCall)
	c.pendingCall[idx].Store(req.Seq, &callContext{
		onResponse: func(v interface{}, e *Error) {
			ret = v
			err = e
			close(waitC)
		},
		seq: req.Seq,
	})

	if sendError := channel.SendRequestWithContext(ctx, req); nil != sendError {
		c.pendingCall[idx].Delete(req.Seq)
		return nil, NewError(ErrSend, fmt.Sprintf("send error:%v", err))
	}

	select {
	case <-waitC:
	case <-ctx.Done():
		c.pendingCall[idx].Delete(req.Seq)
		err = NewError(ErrSend, fmt.Sprintf("send error:%v", err))
	}
	return ret, err
}
