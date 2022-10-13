package rpcgo

import (
	"context"
	"net"
	"strings"
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

func (this *callContext) callOnResponse(codec Codec, resp []byte, err *Error) {
	if atomic.CompareAndSwapInt32(&this.fired, 0, 1) {
		if err == nil {
			if e := codec.Decode(resp, this.respReceiver); e != nil {
				logger.Sugar().Panicf("callOnResponse decode error:%v", e)
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
		logger.Sugar().Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (c *Client) CallWithCallback(channel Channel, deadline time.Time, method string, arg interface{}, ret interface{}, respCb RespCB) func() bool {
	if b, err := c.codec.Encode(arg); err != nil {
		logger.Sugar().Panicf("encode error:%v", err)
		return nil
	} else if respCb == nil || ret == nil {
		if err = channel.SendRequest(&RequestMsg{
			Seq:    atomic.AddUint64(&c.nextSequence, 1),
			Method: method,
			Arg:    b,
			Oneway: true,
		}, deadline); err != nil {
			logger.Sugar().Errorf("SendRequest error:%v", err)
		}
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
			if _, ok := pending.LoadAndDelete(seq); ok {
				ctx.callOnResponse(c.codec, nil, newError(ErrTimeout, "timeout"))
			}
		}))

		if err = channel.SendRequest(&RequestMsg{
			Seq:    seq,
			Method: method,
			Arg:    b,
		}, deadline); err != nil {
			if _, ok := pending.LoadAndDelete(seq); ok {
				ctx.stopTimer()
				if e, ok := err.(net.Error); ok && e.Timeout() {
					go ctx.callOnResponse(c.codec, nil, newError(ErrTimeout, "timeout"))
				} else {
					go ctx.callOnResponse(c.codec, nil, newError(ErrSend, err.Error()))
				}
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

func (c *Client) Call(ctx context.Context, channel Channel, method string, arg interface{}, ret interface{}) error {
	if b, err := c.codec.Encode(arg); err != nil {
		logger.Sugar().Panicf("encode error:%v", err)
		return nil
	} else {
		seq := atomic.AddUint64(&c.nextSequence, 1)
		if ret != nil {
			waitC := make(chan error, 1)
			pending := &c.pendingCall[int(seq)%len(c.pendingCall)]

			pending.Store(seq, &callContext{
				respReceiver: ret,
				onResponse: func(_ interface{}, err error) {
					waitC <- err
				},
			})

			if err = channel.SendRequestWithContext(ctx, &RequestMsg{
				Seq:    seq,
				Method: method,
				Arg:    b,
			}); err != nil {
				pending.Delete(seq)
				if e, ok := err.(net.Error); ok && e.Timeout() {
					return newError(ErrTimeout, "timeout")
				} else {
					return newError(ErrSend, err.Error())
				}
			}

			select {
			case err := <-waitC:
				return err
			case <-ctx.Done():
				pending.Delete(seq)
				if strings.Contains(ctx.Err().Error(), "canceled") {
					return newError(ErrCancel, "canceled")
				} else {
					return newError(ErrTimeout, "timeout")
				}
			}
		} else {
			if err = channel.SendRequestWithContext(ctx, &RequestMsg{
				Seq:    seq,
				Method: method,
				Arg:    b,
				Oneway: true,
			}); nil != err {
				if e, ok := err.(net.Error); ok && e.Timeout() {
					return newError(ErrTimeout, "timeout")
				} else {
					return newError(ErrSend, err.Error())
				}
			} else {
				return nil
			}
		}
	}
}
