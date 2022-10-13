package rpcgo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
)

type Replyer struct {
	channel Channel
	seq     uint64
	replyed int32
	codec   Codec
	oneway  bool
}

func (this *Replyer) Reply(ret interface{}, err error) {
	if !this.oneway && atomic.CompareAndSwapInt32(&this.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq: this.seq,
		}
		if nil == err {
			if b, e := this.codec.Encode(ret); e != nil {
				logger.Sugar().Panicf("send rpc response to (%s) encode ret error:%s\n", this.channel.Name(), e.Error())
			} else {
				resp.Ret = b
			}
		} else {
			if _, ok := err.(*Error); ok {
				resp.Err = err.(*Error)
			} else {
				resp.Err = newError(ErrMethod, err.Error())
			}
		}

		if e := this.channel.Reply(resp); e != nil {
			logger.Sugar().Errorf("send rpc response to (%s) error:%s\n", this.channel.Name(), e.Error())
		}
	}
}

type methodCaller struct {
	name    string
	argType reflect.Type
	fn      interface{}
}

//接受的method func(context.Context, *Replyer,*Pointer)
func makeMethodCaller(name string, method interface{}) (*methodCaller, error) {
	if method == nil {
		return nil, errors.New("method is nil")
	}

	fnType := reflect.TypeOf(method)
	if fnType.Kind() != reflect.Func {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	if fnType.NumIn() != 3 {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	if !fnType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	if fnType.In(1) != reflect.TypeOf(&Replyer{}) {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	if fnType.In(2).Kind() != reflect.Ptr {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	caller := &methodCaller{
		argType: fnType.In(2).Elem(),
		fn:      method,
		name:    name,
	}

	return caller, nil
}

func (c *methodCaller) call(context context.Context, codec Codec, replyer *Replyer, req *RequestMsg) {
	arg := reflect.New(c.argType).Interface()
	if err := codec.Decode(req.Arg, arg); err == nil {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 65535)
				l := runtime.Stack(buf, false)
				logger.Sugar().Errorf("method:%s channel:%s %s", c.name, replyer.channel.Name(), fmt.Errorf(fmt.Sprintf("%v: %s", r, buf[:l])))
				replyer.Reply(nil, newError(ErrOther, "method panic"))
			}
		}()
		reflect.ValueOf(c.fn).Call([]reflect.Value{reflect.ValueOf(context), reflect.ValueOf(replyer), reflect.ValueOf(arg)})
	} else {
		logger.Sugar().Errorf("method:%s decode arg error:%s channel:%s", c.name, err.Error(), replyer.channel.Name())
		replyer.Reply(nil, newError(ErrOther, fmt.Sprintf("arg decode error:%v", err)))
	}
}

type Server struct {
	sync.RWMutex
	methods map[string]*methodCaller
	pause   int32
	codec   Codec
}

func NewServer(codec Codec) *Server {
	return &Server{
		methods: map[string]*methodCaller{},
		codec:   codec}
}

func (this *Server) Pause() {
	atomic.StoreInt32(&this.pause, 1)
}

func (this *Server) Resume() {
	atomic.StoreInt32(&this.pause, 0)
}

func (this *Server) Register(name string, method interface{}) error {
	this.Lock()
	defer this.Unlock()
	if name == "" {
		return errors.New("RegisterMethod nams is nil")
	} else if caller, err := makeMethodCaller(name, method); err != nil {
		return err
	} else {
		if _, ok := this.methods[name]; ok {
			return fmt.Errorf("duplicate method:%s", name)
		} else {
			this.methods[name] = caller
			return nil
		}
	}
}

func (this *Server) UnRegister(name string) {
	this.Lock()
	defer this.Unlock()
	delete(this.methods, name)
}

func (this *Server) method(name string) *methodCaller {
	this.RLock()
	defer this.RUnlock()
	return this.methods[name]
}

func (this *Server) OnMessage(context context.Context, channel Channel, req *RequestMsg) {
	replyer := &Replyer{channel: channel, seq: req.Seq, codec: this.codec, oneway: req.Oneway}
	if caller := this.method(req.Method); caller == nil {
		replyer.Reply(nil, newError(ErrInvaildMethod, fmt.Sprintf("method %s not found", req.Method)))
	} else if atomic.LoadInt32(&this.pause) == 1 {
		replyer.Reply(nil, newError(ErrServerPause, "server pause"))
	} else {
		caller.call(context, this.codec, replyer, req)
	}
}
