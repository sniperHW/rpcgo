package rpcgo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

var respWaitPool = sync.Pool{
	New: func() interface{} { return make(chan *ResponseMsg, 1) },
}

var asynCtxPool = sync.Pool{
	New: func() interface{} { return &asynContext{} },
}

type asynContext struct {
	onResponse func(interface{}, error)
	timer      *time.Timer
	fired      int32
	ret        interface{}
}

func (c *asynContext) callOnResponse(codec Codec, resp []byte, err *Error) {
	if atomic.CompareAndSwapInt32(&c.fired, 0, 1) {
		if err == nil {
			if e := codec.Decode(resp, c.ret); e != nil {
				logger.Errorf("callOnResponse decode error:%v", e)
				c.onResponse(nil, NewError(ErrOther, "decode resp.Ret"))
			} else {
				c.onResponse(c.ret, nil)
			}
		} else {
			c.onResponse(nil, err)
		}
	}
}

func (c *asynContext) onTimeout() {
	if atomic.CompareAndSwapInt32(&c.fired, 0, 1) {
		c.onResponse(nil, NewError(ErrTimeout, "timeout"))
		asynCtxPool.Put(c)
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

func (c *Client) OnMessage(resp *ResponseMsg) {
	if ctx, ok := c.pendingCall[int(resp.Seq)%len(c.pendingCall)].LoadAndDelete(resp.Seq); ok {
		switch v := ctx.(type) {
		case chan *ResponseMsg:
			v <- resp
		case *asynContext:
			ok := v.timer.Stop()
			v.callOnResponse(c.codec, resp.Ret, resp.Err)
			if ok {
				asynCtxPool.Put(v)
			}
		}
	} else {
		logger.Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (c *Client) AsyncCall(channel Channel, method string, arg interface{}, ret interface{}, deadline time.Time, callback func(interface{}, error)) error {
	b, err := c.codec.Encode(arg)
	if err != nil {
		return err
	}
	reqMessage := &RequestMsg{
		Seq:    c.makeSequence(),
		Method: method,
		Arg:    b,
	}
	if ret == nil || callback == nil {
		reqMessage.Oneway = true
		return channel.Request(reqMessage)
	} else {
		ctx := asynCtxPool.Get().(*asynContext)
		ctx.fired = 0
		ctx.onResponse = callback
		ctx.ret = ret
		pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]
		pending.Store(reqMessage.Seq, ctx)
		ctx.timer = time.AfterFunc(time.Until(deadline), func() {
			if _, ok := pending.LoadAndDelete(reqMessage.Seq); ok {
				ctx.onTimeout()
			}
		})
		err = channel.Request(reqMessage)
		if err != nil {
			if _, ok := pending.LoadAndDelete(reqMessage.Seq); ok {
				if ctx.timer.Stop() {
					asynCtxPool.Put(ctx)
				}
			}
		}
		return err
	}
}

func (c *Client) Call(ctx context.Context, channel Channel, method string, arg interface{}, ret interface{}) error {
	b, err := c.codec.Encode(arg)
	if err != nil {
		return err
	}

	reqMessage := &RequestMsg{
		Seq:    c.makeSequence(),
		Method: method,
		Arg:    b,
	}

	if ret == nil {
		reqMessage.Oneway = true
		for {
			if err = channel.RequestWithContext(ctx, reqMessage); err == nil {
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
			if err = channel.RequestWithContext(ctx, reqMessage); err == nil {
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
