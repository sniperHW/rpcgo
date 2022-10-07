package rpcgo

import (
	"fmt"
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

type RPCRequest struct {
	channel RPCChannel
	request *RPCRequestMessage
	replyed int32
}

func (this RPCRequest) Context() interface{} {
	return this.request.Context
}

func (this RPCRequest) Argumment() interface{} {
	return this.request.Arg
}

func (this *RPCRequest) reply(response *RPCResponseMessage) {
	if err := this.channel.Reply(response); err != nil {
		logger.Sugar().Errorf(formatFileLine("send rpc response to (%s) error:%s\n", this.channel.Name(), err.Error()))
	}
}

func (this *RPCRequest) Reply(ret interface{}, err *Error) {
	if atomic.CompareAndSwapInt32(&this.replyed, 0, 1) {
		this.reply(&RPCResponseMessage{Seq: this.request.Seq, Ret: ret, Context: this.request.Context, Err: err})
	}
}

type RPCServer struct {
	sync.RWMutex
	methods map[string]func(RPCRequest)
	pause   int32
}

func NewServer() *RPCServer {
	return &RPCServer{methods: map[string]func(RPCRequest){}}
}

func (this *RPCServer) Pause() {
	atomic.StoreInt32(&this.pause, 1)
}

func (this *RPCServer) Resume() {
	atomic.StoreInt32(&this.pause, 0)
}

func (this *RPCServer) RegisterMethod(name string, method func(RPCRequest)) {
	this.Lock()
	defer this.Unlock()
	if name == "" {
		logger.Sugar().Errorf("RegisterMethod nams is nil")
	} else if nil == method {
		logger.Sugar().Errorf("RegisterMethod method is nil")
	} else {
		if _, ok := this.methods[name]; ok {
			logger.Sugar().Errorf("duplicate method:%s", name)
		} else {
			this.methods[name] = method
		}
	}
}

func (this *RPCServer) UnRegisterMethod(name string) {
	this.Lock()
	defer this.Unlock()
	delete(this.methods, name)
}

func (this *RPCServer) GetMethod(name string) func(RPCRequest) {
	this.RLock()
	defer this.RUnlock()
	return this.methods[name]
}

func (this *RPCServer) call(method func(RPCRequest), request *RPCRequest) {
	defer func() {
		if r := recover(); r != nil {
			request.Reply(nil, NewError(ErrRuntime, fmt.Sprintf("%v", r)))
		}
	}()
	method(*request)
}

func (this *RPCServer) OnRPCMessage(channel RPCChannel, req *RPCRequestMessage) {
	method := this.GetMethod(req.Method)
	request := RPCRequest{channel: channel, request: req}
	if method == nil {
		request.Reply(nil, NewError(ErrInvaildMethod, fmt.Sprintf("method %s not found", req.Method)))
	} else {
		if atomic.LoadInt32(&this.pause) == 0 {
			this.call(method, &request)
		} else {
			request.Reply(nil, NewError(ErrServerPause, "server pause"))
		}
	}
}
