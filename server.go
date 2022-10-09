package rpcgo

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

func formatFileLine(format string, v ...interface{}) string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		s := fmt.Sprintf("[%s:%d]", file, line)
		return strings.Join([]string{s, fmt.Sprintf(format, v...)}, "")
	} else {
		return fmt.Sprintf(format, v...)
	}
}

type Replyer struct {
	channel RPCChannel
	seq     uint64
	replyed int32
	codec   Codec
}

func (this *Replyer) Reply(ret interface{}, err *Error) {
	if atomic.CompareAndSwapInt32(&this.replyed, 0, 1) {
		resp := &RPCResponseMessage{
			Seq: this.seq,
			Err: err}
		if nil == err {
			if b, e := this.codec.Encode(ret); e != nil {
				logger.Sugar().Errorf(formatFileLine("send rpc response to (%s) encode ret error:%s\n", this.channel.Name(), e.Error()))
			} else {
				resp.Ret = b
			}
		}
		if e := this.channel.Reply(resp); e != nil {
			logger.Sugar().Errorf(formatFileLine("send rpc response to (%s) error:%s\n", this.channel.Name(), e.Error()))
		}
	}
}

type methodCaller struct {
	argType reflect.Type
	fn      interface{}
}

//接受的method func(*Replyer,*Pointer)
func makeMethodCaller(method interface{}) (*methodCaller, error) {
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
	}

	return caller, nil
}

func (c *methodCaller) call(codec Codec, replyer *Replyer, req *RPCRequestMessage) error {
	arg := reflect.New(c.argType).Interface()
	if err := codec.Decode(req.Arg, arg); err == nil {
		reflect.ValueOf(c.fn).Call([]reflect.Value{reflect.ValueOf(replyer), reflect.ValueOf(arg)})
		return nil
	} else {
		return err
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
	} else if caller, err := makeMethodCaller(method); err != nil {
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
	replyer := &Replyer{channel: channel, seq: req.Seq, codec: this.codec}
	if caller == nil {
		replyer.Reply(nil, &Error{Code: ErrInvaildMethod, Description: fmt.Sprintf("method %s not found", req.Method)})
	} else {
		if atomic.LoadInt32(&this.pause) == 1 {
			replyer.Reply(nil, &Error{Code: ErrServerPause, Description: "server pause"})
		} else if err := caller.call(this.codec, replyer, req); err != nil {
			replyer.Reply(nil, &Error{Code: ErrRuntime, Description: err.Error()})
		}
	}
}
