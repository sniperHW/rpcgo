package rpcgo

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
)

type RespCB func(interface{}, error)

type callContext struct {
	onResponse   RespCB
	fired        int32
	respReceiver interface{}
}

func (c *callContext) callOnResponse(codec Codec, resp []byte, err *Error) {
	if atomic.CompareAndSwapInt32(&c.fired, 0, 1) {
		if err == nil {
			if e := codec.Decode(resp, c.respReceiver); e != nil {
				logger.Panicf("callOnResponse decode error:%v", e)
			}
		}

		if err == nil {
			c.onResponse(c.respReceiver, nil)
		} else {
			c.onResponse(c.respReceiver, err)
		}
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
		ctx.(*callContext).callOnResponse(c.codec, resp.Ret, resp.Err)
	} else {
		logger.Infof("onResponse with no reqContext:%d", resp.Seq)
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
