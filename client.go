package rpcgo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type callContext struct {
	seq           uint64
	onResponse    func(interface{}, *Error)
	deadlineTimer atomic.Value
	fired         int32
	respReceiver  interface{}
}

func (this *callContext) callOnResponse(codec Codec, resp []byte, err *Error) {
	if atomic.CompareAndSwapInt32(&this.fired, 0, 1) {
		if err == nil {
			if e := codec.Decode(resp, this.respReceiver); e != nil {
				err = &Error{Code: ErrDecode, Description: e.Error()}
			}
		}
		this.onResponse(this.respReceiver, err)
	}
}

func (this *callContext) stopTimer() {
	if t, ok := this.deadlineTimer.Load().(*time.Timer); ok {
		t.Stop()
	}
}

type RPCClient struct {
	nextSequence uint64
	codec        Codec
	pendingCall  []sync.Map
}

func NewClient(codec Codec) *RPCClient {
	return &RPCClient{
		codec:       codec,
		pendingCall: make([]sync.Map, 32),
	}
}

func (c *RPCClient) OnRPCMessage(resp *RPCResponseMessage) {
	idx := int(resp.Seq) % len(c.pendingCall)
	if ctx, ok := c.pendingCall[idx].LoadAndDelete(resp.Seq); ok {
		ctx.(*callContext).stopTimer()
		ctx.(*callContext).callOnResponse(c.codec, resp.Ret, resp.Err)
	} else {
		logger.Sugar().Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (c *RPCClient) AsynCall(channel RPCChannel, deadline time.Time, method string, arg interface{}, ret interface{}, respCb func(interface{}, *Error)) func() bool {
	if b, e := c.codec.Encode(arg); e != nil {
		go respCb(nil, &Error{Code: ErrEncode, Description: e.Error()})
		return func() bool { return false }
	} else {
		req := &RPCRequestMessage{
			Seq:    atomic.AddUint64(&c.nextSequence, 1),
			Method: method,
			Arg:    b,
		}

		timeout := deadline.Sub(time.Now())
		ctx := &callContext{
			onResponse:   respCb,
			seq:          req.Seq,
			respReceiver: ret,
		}

		idx := int(req.Seq) % len(c.pendingCall)

		c.pendingCall[idx].Store(req.Seq, ctx)

		ctx.deadlineTimer.Store(time.AfterFunc(timeout, func() {
			c.pendingCall[idx].Delete(req.Seq)
			ctx.callOnResponse(c.codec, nil, &Error{Code: ErrTimeout, Description: "timeout"})
		}))

		if err := channel.SendRequest(req, deadline); err != nil {
			if _, ok := c.pendingCall[idx].LoadAndDelete(req.Seq); ok {
				ctx.stopTimer()
				go ctx.callOnResponse(c.codec, nil, &Error{Code: ErrSend, Description: err.Error()})
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
}

func (c *RPCClient) Call(ctx context.Context, channel RPCChannel, method string, arg interface{}, ret interface{}) (err *Error) {
	if b, e := c.codec.Encode(arg); e != nil {
		return &Error{Code: ErrEncode, Description: e.Error()}
	} else {

		req := &RPCRequestMessage{
			Seq:    atomic.AddUint64(&c.nextSequence, 1),
			Method: method,
			Arg:    b,
		}

		waitC := make(chan []interface{})
		idx := int(req.Seq) % len(c.pendingCall)
		c.pendingCall[idx].Store(req.Seq, &callContext{
			onResponse: func(_ interface{}, e *Error) {
				err = e
				close(waitC)
			},
			seq:          req.Seq,
			respReceiver: ret,
		})

		if sendError := channel.SendRequestWithContext(ctx, req); nil != sendError {
			c.pendingCall[idx].Delete(req.Seq)
			return &Error{Code: ErrSend, Description: fmt.Sprintf("send error:%v", err)}
		}

		select {
		case <-waitC:
		case <-ctx.Done():
			c.pendingCall[idx].Delete(req.Seq)
			err = &Error{Code: ErrSend, Description: fmt.Sprintf("send error:%v", err)}
		}

		return err
	}
}
