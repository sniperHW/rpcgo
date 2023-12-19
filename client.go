package rpcgo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

var syncCtxPool = sync.Pool{
	New: func() interface{} { return &syncContext{ch: make(chan *ResponseMsg, 1)} },
}

var asynCtxPool = sync.Pool{
	New: func() interface{} { return &asynContext{} },
}

type syncContext struct {
	ch chan *ResponseMsg
}

func (sc *syncContext) OnDisconnect() {
	select {
	case sc.ch <- &ResponseMsg{
		Err: NewError(ErrDisconnet, "disconnect"),
	}:
	default:
	}
}

type asynContext struct {
	onResponse func(interface{}, error)
	timer      *time.Timer
	fired      int32
	ret        interface{}
}

func (c *asynContext) OnDisconnect() {
	if atomic.CompareAndSwapInt32(&c.fired, 0, 1) {
		c.onResponse(nil, NewError(ErrDisconnet, "disconnect"))
	}
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

func (c *Client) OnMessage(channel ChannelInterestDisconnect, resp *ResponseMsg) {
	var ctx interface{}
	var ok bool
	if channel != nil {
		ctx, ok = channel.LoadAndDeletePending(resp.Seq)
	} else {
		ctx, ok = c.pendingCall[int(resp.Seq)%len(c.pendingCall)].LoadAndDelete(resp.Seq)
	}

	if ok {
		switch v := ctx.(type) {
		case *syncContext:
			v.ch <- resp
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
		c.putPending(channel, pending, reqMessage.Seq, ctx)
		ctx.timer = time.AfterFunc(time.Until(deadline), func() {
			if _, ok := c.loadAndDeletePending(channel, pending, reqMessage.Seq); ok {
				ctx.onTimeout()
			}
		})
		err = channel.Request(reqMessage)
		if err != nil {
			if _, ok := c.loadAndDeletePending(channel, pending, reqMessage.Seq); ok {
				if ctx.timer.Stop() {
					asynCtxPool.Put(ctx)
				}
			}
		}
		return err
	}
}

func (c *Client) putPending(channel Channel, pending *sync.Map, seq uint64, ctx interface{}) {
	if cc, ok := channel.(ChannelInterestDisconnect); ok {
		cc.PutPending(seq, ctx.(Pending))
	} else {
		pending.Store(seq, ctx)
	}
}

func (c *Client) loadAndDeletePending(channel Channel, pending *sync.Map, seq uint64) (interface{}, bool) {
	if cc, ok := channel.(ChannelInterestDisconnect); ok {
		return cc.LoadAndDeletePending(seq)
	} else {
		return pending.LoadAndDelete(seq)
	}
}

func (c *Client) deletePending(channel Channel, pending *sync.Map, seq uint64) {
	if cc, ok := channel.(ChannelInterestDisconnect); ok {
		cc.LoadAndDeletePending(seq)
	} else {
		pending.Delete(seq)
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
		syncCtx := syncCtxPool.Get().(*syncContext)
		c.putPending(channel, pending, reqMessage.Seq, syncCtx)
		for {
			if err = channel.RequestWithContext(ctx, reqMessage); err == nil {
				select {
				case resp := <-syncCtx.ch:
					syncCtxPool.Put(syncCtx)
					if resp.Err != nil {
						return resp.Err
					}
					if err = c.codec.Decode(resp.Ret, ret); err == nil {
						return nil
					} else {
						return NewError(ErrOther, err.Error())
					}
				case <-ctx.Done():
					_, ok := c.loadAndDeletePending(channel, pending, reqMessage.Seq)
					if ok {
						syncCtxPool.Put(syncCtx)
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
					c.deletePending(channel, pending, reqMessage.Seq)
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
				c.deletePending(channel, pending, reqMessage.Seq)
				return NewError(ErrOther, err.Error())
			}
		}
	}
}
