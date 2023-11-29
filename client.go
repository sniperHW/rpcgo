package rpcgo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type RespCB func(interface{}, error)

type callContext struct {
	respC        chan error
	fired        int32
	respReceiver interface{}
}

func (c *callContext) callOnResponse(codec Codec, resp []byte, err *Error) {
	if atomic.CompareAndSwapInt32(&c.fired, 0, 1) {
		if err == nil {
			if e := codec.Decode(resp, c.respReceiver); e != nil {
				logger.Errorf("callOnResponse decode error:%v", e)
				c.respC <- errors.New("callOnResponse decode error")
			} else {
				c.respC <- nil
			}
		} else {
			c.respC <- err
		}
	}
}

type Client struct {
	sync.Mutex
	nextSequence uint32
	timestamp    uint32
	timeOffset   uint32
	startTime    time.Time
	codec        Codec
	pendingCall  [32]sync.Map
}

func NewClient(codec Codec) *Client {
	return &Client{
		codec:      codec,
		timeOffset: uint32(time.Now().Unix() - time.Date(2023, time.January, 1, 0, 0, 0, 0, time.Local).Unix()),
		startTime:  time.Now(),
	}
}

func (c *Client) getTimeStamp() uint32 {
	return uint32(time.Since(c.startTime)/time.Second) + c.timeOffset
}

func (c *Client) makeSequence() (seq uint64) {
	timestamp := c.getTimeStamp()
	c.Lock()
	if timestamp > c.timestamp {
		c.timestamp = timestamp
		c.nextSequence = 1
	} else {
		c.nextSequence++
	}
	seq = uint64(c.timestamp)<<32 + uint64(c.nextSequence)
	c.Unlock()
	return seq
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
			Seq:    c.makeSequence(),
			Method: method,
			Arg:    b,
		}

		if ret == nil {
			reqMessage.Oneway = true
			for {
				if err = channel.SendRequest(ctx, reqMessage); err != nil {
					if channel.IsRetryAbleError(err) {
						time.Sleep(time.Millisecond * 10)
						select {
						case <-ctx.Done():
							switch ctx.Err() {
							case context.Canceled:
								return NewError(ErrCancel, "canceled")
							case context.DeadlineExceeded:
								return NewError(ErrTimeout, "timeout")
							default:
								return NewError(ErrOther, "unknow")
							}
						default:
							//context没有超时或被取消，继续尝试发送
						}
					} else {
						return err
					}
				} else {
					return nil
				}
			}
		} else {
			pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]
			callCtx := &callContext{
				respReceiver: ret,
				respC:        make(chan error),
			}
			pending.Store(reqMessage.Seq, callCtx)
			for {
				if err = channel.SendRequest(ctx, reqMessage); err != nil {
					if channel.IsRetryAbleError(err) {
						time.Sleep(time.Millisecond * 10)
						select {
						case <-ctx.Done():
							pending.Delete(reqMessage.Seq)
							switch ctx.Err() {
							case context.Canceled:
								return NewError(ErrCancel, "canceled")
							default:
								return NewError(ErrTimeout, "timeout")
							}
						default:
							//context没有超时或被取消，继续尝试发送
						}
					} else {
						pending.Delete(reqMessage.Seq)
						return err
					}
				} else {
					select {
					case err := <-callCtx.respC:
						return err
					case <-ctx.Done():
						pending.Delete(reqMessage.Seq)
						switch ctx.Err() {
						case context.Canceled:
							return NewError(ErrCancel, "canceled")
						default:
							return NewError(ErrTimeout, "timeout")
						}
					}
				}
			}
		}
	}
}
