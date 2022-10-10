package rpcgo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type callContext struct {
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
	if nil == respCb {
		if b, err := c.codec.Encode(arg); err != nil {
			logger.Sugar().Errorf("encode error:%s", err.Error())
		} else if err = channel.SendRequest(&RPCRequestMessage{
			Seq:    atomic.AddUint64(&c.nextSequence, 1),
			Method: method,
			Arg:    b,
			Oneway: true,
		}, deadline); err != nil {
			logger.Sugar().Errorf("SendRequest error:%s", err.Error())
		}
		return nil
	} else {

		if b, err := c.codec.Encode(arg); err != nil {
			go respCb(nil, &Error{Code: ErrEncode, Description: err.Error()})
			return nil
		} else {
			seq := atomic.AddUint64(&c.nextSequence, 1)
			timeout := deadline.Sub(time.Now())
			ctx := &callContext{
				onResponse:   respCb,
				respReceiver: ret,
			}

			pending := &c.pendingCall[int(seq)%len(c.pendingCall)]

			pending.Store(seq, ctx)

			ctx.deadlineTimer.Store(time.AfterFunc(timeout, func() {
				pending.Delete(seq)
				ctx.callOnResponse(c.codec, nil, &Error{Code: ErrTimeout, Description: "timeout"})
			}))

			if err = channel.SendRequest(&RPCRequestMessage{
				Seq:    seq,
				Method: method,
				Arg:    b,
			}, deadline); err != nil {
				if _, ok := pending.LoadAndDelete(seq); ok {
					ctx.stopTimer()
					go ctx.callOnResponse(c.codec, nil, &Error{Code: ErrSend, Description: err.Error()})
				}
				return nil
			} else {
				return func() bool {
					if _, ok := pending.LoadAndDelete(seq); ok {
						ctx.stopTimer()
						return true
					} else {
						return false
					}
				}
			}
		}
	}
}

func (c *RPCClient) Call(ctx context.Context, channel RPCChannel, method string, arg interface{}, ret interface{}) *Error {
	if b, e := c.codec.Encode(arg); e != nil {
		return &Error{Code: ErrEncode, Description: e.Error()}
	} else {
		seq := atomic.AddUint64(&c.nextSequence, 1)
		if ret != nil {
			waitC := make(chan *Error, 1)
			pending := &c.pendingCall[int(seq)%len(c.pendingCall)]

			pending.Store(seq, &callContext{
				respReceiver: ret,
				onResponse: func(_ interface{}, e *Error) {
					waitC <- e
				},
			})

			if e = channel.SendRequestWithContext(ctx, &RPCRequestMessage{
				Seq:    seq,
				Method: method,
				Arg:    b,
			}); nil != e {
				pending.Delete(seq)
				return &Error{Code: ErrSend, Description: fmt.Sprintf("send error:%s", e.Error())}
			}

			var err *Error
			select {
			case err = <-waitC:
			case <-ctx.Done():
				pending.Delete(seq)
				if strings.Contains(ctx.Err().Error(), "canceled") {
					err = &Error{Code: ErrCancel, Description: "canceled"}
				} else {
					err = &Error{Code: ErrTimeout, Description: "timeout"}
				}
			}
			return err
		} else {
			if e = channel.SendRequestWithContext(ctx, &RPCRequestMessage{
				Seq:    seq,
				Method: method,
				Arg:    b,
				Oneway: true,
			}); nil != e {
				return &Error{Code: ErrSend, Description: fmt.Sprintf("send error:%s", e.Error())}
			} else {
				return nil
			}
		}
	}
}
