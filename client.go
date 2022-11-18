package rpcgo

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type RespCB func(interface{}, error)

type callContext struct {
	onResponse    RespCB
	deadlineTimer atomic.Value
	fired         int32
	respReceiver  interface{}
}

func (c *callContext) callOnResponse(codec Codec, resp []byte, err *Error) {
	if atomic.CompareAndSwapInt32(&c.fired, 0, 1) {
		if err == nil {
			if e := codec.Decode(resp, c.respReceiver); e != nil {
				logger.Panicf("callOnResponse decode error:%v", e)
			}
		}

		c.onResponse(c.respReceiver, err)
	}
}

func (c *callContext) stopTimer() {
	if t, ok := c.deadlineTimer.Load().(*time.Timer); ok {
		t.Stop()
	}
}

type Client struct {
	nextSequence uint64
	codec        Codec
	pendingCall  [32]sync.Map
}

func NewClient(codec Codec) *Client {
	return &Client{
		codec: codec,
	}
}

func (c *Client) OnMessage(context context.Context, resp *ResponseMsg) {
	if ctx, ok := c.pendingCall[int(resp.Seq)%len(c.pendingCall)].LoadAndDelete(resp.Seq); ok {
		ctx.(*callContext).stopTimer()
		ctx.(*callContext).callOnResponse(c.codec, resp.Ret, resp.Err)
	} else {
		logger.Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (c *Client) CallWithCallback(channel Channel, deadline time.Time, method string, arg interface{}, ret interface{}, respCb RespCB) (func() bool, error) {
	if b, err := c.codec.Encode(arg); err != nil {
		return nil, fmt.Errorf("encode error:%v", err)
	} else {
		reqMessage := &RequestMsg{
			Seq:    atomic.AddUint64(&c.nextSequence, 1),
			Method: method,
			Arg:    b,
		}
		if respCb == nil || ret == nil {
			reqMessage.Oneway = true
			if err = channel.SendRequest(reqMessage, deadline); err != nil {
				logger.Errorf("SendRequest error:%v", err)
			}
			return nil, nil
		} else {

			ctx := &callContext{
				onResponse:   respCb,
				respReceiver: ret,
			}

			pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]

			pending.Store(reqMessage.Seq, ctx)

			ctx.deadlineTimer.Store(time.AfterFunc(time.Until(deadline), func() {
				if _, ok := pending.LoadAndDelete(reqMessage.Seq); ok {
					ctx.callOnResponse(c.codec, nil, NewError(ErrTimeout, "timeout"))
				}
			}))

			if err = channel.SendRequest(reqMessage, deadline); err != nil {
				if _, ok := pending.LoadAndDelete(reqMessage.Seq); ok {
					ctx.stopTimer()
					if e, ok := err.(net.Error); ok && e.Timeout() {
						go ctx.callOnResponse(c.codec, nil, NewError(ErrTimeout, "timeout"))
					} else {
						go ctx.callOnResponse(c.codec, nil, NewError(ErrSend, err.Error()))
					}
				}
				return nil, nil
			} else {
				return func() bool {
					if _, ok := pending.LoadAndDelete(reqMessage.Seq); ok {
						ctx.stopTimer()
						return true
					} else {
						return false
					}
				}, nil
			}
		}
	}
}

func (c *Client) Call(ctx context.Context, channel Channel, method string, arg interface{}, ret interface{}) error {
	if b, err := c.codec.Encode(arg); err != nil {
		logger.Panicf("encode error:%v", err)
		return nil
	} else {
		reqMessage := &RequestMsg{
			Seq:    atomic.AddUint64(&c.nextSequence, 1),
			Method: method,
			Arg:    b,
		}
		if ret != nil {
			waitC := make(chan error, 1)
			pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]

			pending.Store(reqMessage.Seq, &callContext{
				respReceiver: ret,
				onResponse: func(_ interface{}, err error) {
					waitC <- err
				},
			})

			if err = channel.SendRequestWithContext(ctx, reqMessage); err != nil {
				pending.Delete(reqMessage.Seq)
				if e, ok := err.(net.Error); ok && e.Timeout() {
					return NewError(ErrTimeout, "timeout")
				} else {
					return NewError(ErrSend, err.Error())
				}
			}

			select {
			case err := <-waitC:
				return err
			case <-ctx.Done():
				pending.Delete(reqMessage.Seq)
				switch ctx.Err() {
				case context.Canceled:
					return NewError(ErrCancel, "canceled")
				case context.DeadlineExceeded:
					return NewError(ErrTimeout, "timeout")
				default:
					return NewError(ErrOther, "unknow")
				}
			}
		} else {
			reqMessage.Oneway = true
			if err = channel.SendRequestWithContext(ctx, reqMessage); nil != err {
				if e, ok := err.(net.Error); ok && e.Timeout() {
					return NewError(ErrTimeout, "timeout")
				} else {
					return NewError(ErrSend, err.Error())
				}
			} else {
				return nil
			}
		}
	}
}
