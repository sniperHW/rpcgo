package rpcgo

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

type Replyer struct {
	channel RPCChannel
	seq     uint64
	replyed int32
	codec   Codec
	oneway  bool
}

func (this *Replyer) Reply(ret interface{}, err *Error) {
	if !this.oneway && atomic.CompareAndSwapInt32(&this.replyed, 0, 1) {
		resp := &RPCResponseMessage{
			Seq: this.seq,
			Err: err}
		if nil == err {
			if b, e := this.codec.Encode(ret); e != nil {
				logger.Sugar().Errorf("send rpc response to (%s) encode ret error:%s\n", this.channel.Name(), e.Error())
			} else {
				resp.Ret = b
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

//接受的method func(*Replyer,*Pointer)
func makeMethodCaller(name string, method interface{}) (*methodCaller, error) {
	if method == nil {
		return nil, errors.New("method is nil")
	}

	fnType := reflect.TypeOf(method)
	if fnType.Kind() != reflect.Func {
		return nil, errors.New("method should have type func(*Replyer,*Pointer)")
	}

	if fnType.NumIn() != 2 {
		return nil, errors.New("method should have type func(*Replyer,*Pointer)")
	}

	if fnType.In(0) != reflect.TypeOf(&Replyer{}) {
		return nil, errors.New("method should have type func(*Replyer,*Pointer)")
	}

	if fnType.In(1).Kind() != reflect.Ptr {
		return nil, errors.New("method should have type func(*Replyer,*Pointer)")
	}

	caller := &methodCaller{
		argType: fnType.In(1).Elem(),
		fn:      method,
		name:    name,
	}

	return caller, nil
}

func (c *methodCaller) call(codec Codec, replyer *Replyer, req *RPCRequestMessage) {
	arg := reflect.New(c.argType).Interface()
	if err := codec.Decode(req.Arg, arg); err == nil {
		defer func() {
			if r := recover(); r != nil {
				logger.Sugar().Errorf("call method:%s channel:%s error:%v", c.name, replyer.channel.Name(), r)
				replyer.Reply(nil, &Error{Code: ErrRuntime, Description: "call method:%s error"})
			}
		}()
		reflect.ValueOf(c.fn).Call([]reflect.Value{reflect.ValueOf(replyer), reflect.ValueOf(arg)})
	} else {
		logger.Sugar().Errorf("method:%s decode arg error:%s channel:%s", c.name, err.Error(), replyer.channel.Name())
		replyer.Reply(nil, &Error{Code: ErrRuntime, Description: fmt.Sprintf("decode arg error:%s", err.Error())})
	}
}

type RPCServer struct {
	sync.RWMutex
	methods map[string]*methodCaller
	pause   int32
	codec   Codec
}

func NewServer(codec Codec) *RPCServer {
	return &RPCServer{
		methods: map[string]*methodCaller{},
		codec:   codec}
}

func (this *RPCServer) Pause() {
	atomic.StoreInt32(&this.pause, 1)
}

func (this *RPCServer) Resume() {
	atomic.StoreInt32(&this.pause, 0)
}

func (this *RPCServer) RegisterMethod(name string, method interface{}) error {
	this.Lock()
	defer this.Unlock()
	if name == "" {
		logger.Sugar().Errorf("RegisterMethod nams is nil")
		return errors.New("RegisterMethod nams is nil")
	} else if caller, err := makeMethodCaller(name, method); err != nil {
		logger.Sugar().Errorf("RegisterMethod nams error:%s", err.Error())
		return err
	} else {
		if _, ok := this.methods[name]; ok {
			logger.Sugar().Errorf("duplicate method:%s", name)
			return errors.New("duplicate method")
		} else {
			this.methods[name] = caller
			return nil
		}
	}
}

func (this *RPCServer) UnRegisterMethod(name string) {
	this.Lock()
	defer this.Unlock()
	delete(this.methods, name)
}

func (this *RPCServer) GetMethod(name string) *methodCaller {
	this.RLock()
	defer this.RUnlock()
	return this.methods[name]
}

func (this *RPCServer) OnRPCMessage(channel RPCChannel, req *RPCRequestMessage) {
	caller := this.GetMethod(req.Method)
	replyer := &Replyer{channel: channel, seq: req.Seq, codec: this.codec, oneway: req.Oneway}
	if caller == nil {
		replyer.Reply(nil, &Error{Code: ErrInvaildMethod, Description: fmt.Sprintf("method %s not found", req.Method)})
	} else {
		if atomic.LoadInt32(&this.pause) == 1 {
			replyer.Reply(nil, &Error{Code: ErrServerPause, Description: "server pause"})
		} else {
			caller.call(this.codec, replyer, req)
		}
	}
}
