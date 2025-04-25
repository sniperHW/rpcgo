package rpcgo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type Replyer struct {
	channel        Channel
	replyed        int32
	codec          Codec
	req            *RequestMsg
	outInterceptor []func(*RequestMsg, interface{}, error)
}

func (r *Replyer) AppendOutInterceptor(interceptor func(*RequestMsg, interface{}, error)) {
	r.outInterceptor = append(r.outInterceptor, interceptor)
}

func (r *Replyer) callOutInterceptor(ret interface{}, err error) {
	for _, fn := range r.outInterceptor {
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("%s ", fmt.Errorf(fmt.Sprintf("%v: %s", r, debug.Stack())))
				}
			}()
			fn(r.req, ret, err)
		}()
	}
}

func (r *Replyer) Error(err error) {
	if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		r.callOutInterceptor(nil, err)
		if r.req.Oneway {
			return
		}
		resp := &ResponseMsg{
			Seq: r.req.Seq,
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
	if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		r.callOutInterceptor(ret, nil)
		if r.req.Oneway {
			return
		}
		resp := &ResponseMsg{
			Seq: r.req.Seq,
		}

		if b, e := r.codec.Encode(ret); e != nil {
			logger.Errorf("send rpc response to (%s) encode ret error:%s\n", r.channel.Name(), e.Error())
			return
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

	if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
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

type Server struct {
	sync.RWMutex
	methods       map[string]*methodCaller
	codec         Codec
	stoped        atomic.Bool
	inInterceptor []func(*Replyer, *RequestMsg) bool //入站管道线
}

func NewServer(codec Codec) *Server {
	return &Server{
		methods: map[string]*methodCaller{},
		codec:   codec}
}

func (s *Server) SetInInterceptor(interceptor []func(*Replyer, *RequestMsg) bool) *Server {
	s.inInterceptor = interceptor
	return s
}

func (s *Server) Stop() {
	s.stoped.CompareAndSwap(false, true)
}

func (s *Server) Register(name string, method interface{}) error {
	s.Lock()
	defer s.Unlock()
	if name == "" {
		return errors.New("RegisterMethod nams is nil")
	} else if caller, err := makeMethodCaller(name, method); err != nil {
		return err
	} else if _, ok := s.methods[name]; ok {
		return fmt.Errorf("duplicate method:%s", name)
	} else {
		s.methods[name] = caller
		return nil
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
	replyer := &Replyer{channel: channel, req: req, codec: s.codec}
	if s.stoped.Load() {
		replyer.Error(NewError(ErrServiceUnavaliable, "service unavaliable"))
		return
	}
	caller := s.method(req.Method)
	if caller == nil {
		replyer.Error(NewError(ErrInvaildMethod, fmt.Sprintf("method %s not found", req.Method)))
		return
	}
	arg := reflect.New(caller.argType).Interface()
	if err := s.codec.Decode(req.Arg, arg); err != nil {
		logger.Errorf("method:%s decode arg error:%s channel:%s", req.Method, err.Error(), replyer.channel.Name())
		replyer.Error(NewError(ErrOther, fmt.Sprintf("arg decode error:%v", err)))
		return
	}
	req.arg = arg

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("method:%s channel:%s %s", req.Method, replyer.channel.Name(), fmt.Errorf(fmt.Sprintf("%v: %s", r, debug.Stack())))
			replyer.Error(NewError(ErrOther, "method panic"))
		}
	}()

	for _, v := range s.inInterceptor {
		if !v(replyer, req) {
			return
		}
	}

	caller.fn.Call([]reflect.Value{reflect.ValueOf(context), reflect.ValueOf(replyer), reflect.ValueOf(arg)})
}
