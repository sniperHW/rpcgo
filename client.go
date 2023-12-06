package rpcgo

import (
	"context"
	"sync"
	"time"
)

var respWaitPool = sync.Pool{
	New: func() interface{} { return make(chan *ResponseMsg, 1) },
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

func (c *Client) OnMessage(resp *ResponseMsg) {
	if ctx, ok := c.pendingCall[int(resp.Seq)%len(c.pendingCall)].LoadAndDelete(resp.Seq); ok {
		ctx.(chan *ResponseMsg) <- resp
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
				if err = channel.SendRequest(ctx, reqMessage); err == nil {
					return nil
				} else if channel.IsRetryAbleError(err) {
					time.Sleep(time.Millisecond * 10)
					select {
					case <-ctx.Done():
						err = ctx.Err()
						switch err {
						case context.Canceled:
							return NewError(ErrCancel, "canceled")
						case context.DeadlineExceeded:
							return NewError(ErrTimeout, "timeout")
						default:
							return NewError(ErrOther, err.Error())
						}
					default:
						//context没有超时或被取消，继续尝试发送
					}
				} else {
					return NewError(ErrOther, err.Error())
				}
			}
		} else {
			pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]
			wait := respWaitPool.Get().(chan *ResponseMsg)
			for {
				pending.Store(reqMessage.Seq, wait)
				if err = channel.SendRequest(ctx, reqMessage); err == nil {
					select {
					case resp := <-wait:
						respWaitPool.Put(wait)
						if resp.Err != nil {
							return resp.Err
						}
						if err = c.codec.Decode(resp.Ret, ret); err == nil {
							return nil
						} else {
							return NewError(ErrOther, err.Error())
						}
					case <-ctx.Done():
						_, ok := pending.LoadAndDelete(reqMessage.Seq)
						if ok {
							respWaitPool.Put(wait)
						}
						err = ctx.Err()
						switch err {
						case context.Canceled:
							return NewError(ErrCancel, "canceled")
						case context.DeadlineExceeded:
							return NewError(ErrTimeout, "timeout")
						default:
							return NewError(ErrOther, err.Error())
						}
					}
				} else if channel.IsRetryAbleError(err) {
					time.Sleep(time.Millisecond * 10)
					select {
					case <-ctx.Done():
						pending.Delete(reqMessage.Seq)
						err = ctx.Err()
						switch err {
						case context.Canceled:
							return NewError(ErrCancel, "canceled")
						case context.DeadlineExceeded:
							return NewError(ErrTimeout, "timeout")
						default:
							return NewError(ErrOther, err.Error())
						}
					default:
						//context没有超时或被取消，继续尝试发送
					}
				} else {
					pending.Delete(reqMessage.Seq)
					return NewError(ErrOther, err.Error())
				}
			}
		}
	}
}
