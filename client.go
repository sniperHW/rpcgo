package rpcgo

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var syncCtxPool = sync.Pool{
	New: func() interface{} { return syncContext(make(chan *ResponseMsg, 1)) },
}

var asynCtxPool = sync.Pool{
	New: func() interface{} { return &asynContext{} },
}

type syncContext chan *ResponseMsg

func (sc syncContext) OnDisconnect() {
	sc <- &ResponseMsg{
		Err: NewError(ErrDisconnet, "disconnect"),
	}
}

type asynContext struct {
	onResponse func(interface{}, error)
	timer      *time.Timer
	fired      int32
	ret        interface{}
	req        *RequestMsg
	cli        *Client
}

func (c *asynContext) OnDisconnect() {
	c.callOnResponse(nil, nil, NewError(ErrDisconnet, "disconnect"))
}

func (c *asynContext) callOnResponse(codec Codec, resp []byte, err *Error) bool {
	if atomic.CompareAndSwapInt32(&c.fired, 0, 1) {
		var e error
		if err == nil {
			if e = codec.Decode(resp, c.ret); e != nil {
				logger.Errorf("callOnResponse decode error:%v", e)
				e = NewError(ErrOther, "decode resp.Ret")
			}
		} else {
			e = err
		}
		c.cli.callInInterceptor(c.req, c.ret, e)
		c.onResponse(c.ret, e)
		return true
	} else {
		return false
	}
}

func (c *asynContext) onTimeout() {
	if c.callOnResponse(nil, nil, NewError(ErrTimeout, "timeout")) {
		asynCtxPool.Put(c)
	}
}

type Client struct {
	sync.Mutex
	nextSequence   uint32
	timestamp      uint32
	timeOffset     uint32
	startTime      time.Time
	codec          Codec
	pendingCall    [32]sync.Map
	inInterceptor  []func(*RequestMsg, interface{}, error) //入站管道线
	outInterceptor []func(*RequestMsg, interface{})        //出站管道线
}

func NewClient(codec Codec) *Client {
	return &Client{
		codec:      codec,
		timeOffset: uint32(time.Now().Unix() - time.Date(2023, time.January, 1, 0, 0, 0, 0, time.Local).Unix()),
		startTime:  time.Now(),
	}
}

func (c *Client) SetInInterceptor(interceptor []func(*RequestMsg, interface{}, error)) {
	c.inInterceptor = interceptor
}

func (c *Client) SetOutInterceptor(interceptor []func(*RequestMsg, interface{})) {
	c.outInterceptor = interceptor
}

func (c *Client) callInInterceptor(req *RequestMsg, ret interface{}, err error) {
	for _, fn := range c.inInterceptor {
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("%%v: %s", r, debug.Stack())
				}
			}()
			fn(req, ret, err)
		}()
	}
}

func (c *Client) callOutInterceptor(req *RequestMsg, arg interface{}) {
	for _, fn := range c.outInterceptor {
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("%%v: %s", r, debug.Stack())
				}
			}()
			fn(req, arg)
		}()
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
		case syncContext:
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
		arg:    arg,
	}
	c.callOutInterceptor(reqMessage, arg)
	if ret == nil || callback == nil {
		reqMessage.Oneway = true
		return channel.Request(reqMessage)
	} else {
		ctx := asynCtxPool.Get().(*asynContext)
		ctx.fired = 0
		ctx.onResponse = callback
		ctx.ret = ret
		ctx.req = reqMessage
		ctx.cli = c
		c.putPending(channel, reqMessage.Seq, ctx)
		ctx.timer = time.AfterFunc(time.Until(deadline), func() {
			if _, ok := c.loadAndDeletePending(channel, reqMessage.Seq); ok {
				ctx.onTimeout()
			}
		})
		err = channel.Request(reqMessage)
		if err != nil {
			if _, ok := c.loadAndDeletePending(channel, reqMessage.Seq); ok {
				if ctx.timer.Stop() {
					asynCtxPool.Put(ctx)
				}
			}
			if atomic.CompareAndSwapInt32(&ctx.fired, 0, 1) {
				c.callInInterceptor(reqMessage, nil, err)
			}
		}
		return err
	}
}

func (c *Client) putPending(channel Channel, seq uint64, ctx interface{}) {
	if cc, ok := channel.(ChannelInterestDisconnect); ok {
		cc.PutPending(seq, ctx.(PendingCall))
	} else {
		pending := &c.pendingCall[int(seq)%len(c.pendingCall)]
		pending.Store(seq, ctx)
	}
}

func (c *Client) loadAndDeletePending(channel Channel, seq uint64) (interface{}, bool) {
	if cc, ok := channel.(ChannelInterestDisconnect); ok {
		return cc.LoadAndDeletePending(seq)
	} else {
		pending := &c.pendingCall[int(seq)%len(c.pendingCall)]
		return pending.LoadAndDelete(seq)
	}
}

func (c *Client) deletePending(channel Channel, seq uint64) {
	if cc, ok := channel.(ChannelInterestDisconnect); ok {
		cc.LoadAndDeletePending(seq)
	} else {
		pending := &c.pendingCall[int(seq)%len(c.pendingCall)]
		pending.Delete(seq)
	}
}

func rpcError(err error) *Error {
	switch err {
	case context.Canceled:
		return NewError(ErrCancel, "canceled")
	case context.DeadlineExceeded:
		return NewError(ErrTimeout, "timeout")
	default:
		return NewError(ErrOther, err.Error())
	}
}

func (c *Client) CallWithTimeout(channel Channel, method string, arg interface{}, ret interface{}, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return c.Call(ctx, channel, method, arg, ret)
}

func (c *Client) Call(ctx context.Context, channel Channel, method string, arg interface{}, ret interface{}) (err error) {
	b, err := c.codec.Encode(arg)
	if err != nil {
		return err
	}

	reqMessage := &RequestMsg{
		Seq:    c.makeSequence(),
		Method: method,
		Arg:    b,
		arg:    arg,
	}
	c.callOutInterceptor(reqMessage, arg)
	defer func() {
		c.callInInterceptor(reqMessage, ret, err)
	}()
	if ret == nil {
		reqMessage.Oneway = true
		for {
			if err = channel.RequestWithContext(ctx, reqMessage); err == nil {
				return nil
			} else if channel.IsRetryAbleError(err) {
				time.Sleep(time.Millisecond * 10)
				select {
				case <-ctx.Done():
					err = rpcError(ctx.Err())
					return err
				default:
					//context没有超时或被取消，继续尝试发送
				}
			} else {
				err = rpcError(err)
				return err
			}
		}
	} else {
		syncCtx := syncCtxPool.Get().(syncContext)
		c.putPending(channel, reqMessage.Seq, syncCtx)
		for {
			if err = channel.RequestWithContext(ctx, reqMessage); err == nil {
				select {
				case resp := <-syncCtx:
					syncCtxPool.Put(syncCtx)
					if resp.Err != nil {
						err = resp.Err
						return err
					}
					if err = c.codec.Decode(resp.Ret, ret); err == nil {
						return err
					} else {
						err = rpcError(err)
						return err
					}
				case <-ctx.Done():
					if _, ok := c.loadAndDeletePending(channel, reqMessage.Seq); ok {
						syncCtxPool.Put(syncCtx)
					}
					err = rpcError(ctx.Err())
					return err
				}
			} else if channel.IsRetryAbleError(err) {
				time.Sleep(time.Millisecond * 10)
				select {
				case <-ctx.Done():
					c.deletePending(channel, reqMessage.Seq)
					syncCtxPool.Put(syncCtx)
					err = rpcError(ctx.Err())
					return err
				default:
					//context没有超时或被取消，继续尝试发送
				}
			} else {
				c.deletePending(channel, reqMessage.Seq)
				syncCtxPool.Put(syncCtx)
				err = rpcError(err)
				return err
			}
		}
	}
}

func Post[Arg any](ctx context.Context, c *Client, channel Channel, method string, arg Arg) (err error) {
	err = c.Call(ctx, channel, method, arg, nil)
	return
}

func Call[Arg any, Ret any](ctx context.Context, c *Client, channel Channel, method string, arg Arg) (ret *Ret, err error) {
	ret = new(Ret)
	err = c.Call(ctx, channel, method, arg, ret)
	return
}

func CallWithTimeout[Arg any, Ret any](c *Client, channel Channel, method string, arg Arg, d time.Duration) (ret *Ret, err error) {
	ret = new(Ret)
	err = c.CallWithTimeout(channel, method, arg, ret, d)
	return
}

func AsyncCall[Arg any, Ret any](c *Client, channel Channel, method string, arg Arg, deadline time.Time, callback func(*Ret, error)) error {
	ret := new(Ret)
	return c.AsyncCall(channel, method, arg, ret, deadline, func(r interface{}, err error) {
		callback(r.(*Ret), err)
	})
}
