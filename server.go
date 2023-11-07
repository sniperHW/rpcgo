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

func (r *Replyer) Error(err error) {
	if !r.oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq: r.seq,
		}

		if _, ok := err.(*Error); ok {
			resp.Err = err.(*Error)
		} else {
			resp.Err = NewError(ErrMethod, err.Error())
		}

		if e := r.channel.Reply(resp); e != nil {
			logger.Errorf("send rpc response to (%s) error:%s\n", r.channel.Name(), e.Error())
		}
	}
}

func (r *Replyer) Reply(ret interface{}) {
	if !r.oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq: r.seq,
		}

		if b, e := r.codec.Encode(ret); e != nil {
			logger.Panicf("send rpc response to (%s) encode ret error:%s\n", r.channel.Name(), e.Error())
		} else {
			resp.Ret = b
		}

		if e := r.channel.Reply(resp); e != nil {
			logger.Errorf("send rpc response to (%s) error:%s\n", r.channel.Name(), e.Error())
		}
	}
}

func (r *Replyer) Channel() Channel {
	return r.channel
}

type methodCaller struct {
	name    string
	argType reflect.Type
	fn      reflect.Value
}

// 接受的method func(context.Context, *Replyer,*Pointer)
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
		fn:      reflect.ValueOf(method),
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
				logger.Errorf("method:%s channel:%s %s", c.name, replyer.channel.Name(), fmt.Errorf(fmt.Sprintf("%v: %s", r, buf[:l])))
				replyer.Error(NewError(ErrOther, "method panic"))
			}
		}()
		c.fn.Call([]reflect.Value{reflect.ValueOf(context), reflect.ValueOf(replyer), reflect.ValueOf(arg)})
	} else {
		logger.Errorf("method:%s decode arg error:%s channel:%s", c.name, err.Error(), replyer.channel.Name())
		replyer.Error(NewError(ErrOther, fmt.Sprintf("arg decode error:%v", err)))
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

func (s *Server) Pause() {
	atomic.StoreInt32(&s.pause, 1)
}

func (s *Server) Resume() {
	atomic.StoreInt32(&s.pause, 0)
}

func (s *Server) Register(name string, method interface{}) error {
	s.Lock()
	defer s.Unlock()
	if name == "" {
		return errors.New("RegisterMethod nams is nil")
	} else if caller, err := makeMethodCaller(name, method); err != nil {
		return err
	} else {
		if _, ok := s.methods[name]; ok {
			return fmt.Errorf("duplicate method:%s", name)
		} else {
			s.methods[name] = caller
			return nil
		}
	}
}

func (s *Server) UnRegister(name string) {
	s.Lock()
	defer s.Unlock()
	delete(s.methods, name)
}

func (s *Server) method(name string) *methodCaller {
	s.RLock()
	defer s.RUnlock()
	return s.methods[name]
}

func (s *Server) OnMessage(context context.Context, channel Channel, req *RequestMsg) {
	replyer := &Replyer{channel: channel, seq: req.Seq, codec: s.codec, oneway: req.Oneway}
	if caller := s.method(req.Method); caller == nil {
		replyer.Error(NewError(ErrInvaildMethod, fmt.Sprintf("method %s not found", req.Method)))
	} else if atomic.LoadInt32(&s.pause) == 1 {
		replyer.Error(NewError(ErrServerPause, "server pause"))
	} else {
		caller.call(context, s.codec, replyer, req)
	}
}
