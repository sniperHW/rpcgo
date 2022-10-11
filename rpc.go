package rpcgo

import (
	"context"
	"go.uber.org/zap"
	"time"
)

var logger *zap.Logger

func InitLogger(l *zap.Logger) {
	logger = l
}

/*
 *  注意,传递给RPC模块的所有回调函数可能在底层信道的接收/发送goroutine上执行，
 *  为了避免接收/发送goroutine被阻塞，回调函数中不能调用阻塞函数。
 *  如需调用阻塞函数，请在回调中启动一个goroutine来执行
 */

type Error struct {
	Code int
	Err  string
}

func newError(code int, err string) *Error {
	if code <= 0 || code >= errEnd {
		return nil
	} else {
		return &Error{Code: code, Err: err}
	}
}

func (e *Error) Error() string {
	return e.Err
}

func (e *Error) Is(code int) bool {
	return e.Code == code
}

const (
	ErrOk = iota
	ErrInvaildMethod
	ErrServerPause
	ErrTimeout
	ErrSend
	ErrCancel
	ErrMethod
	errEnd
)

type RequestMsg struct {
	Seq    uint64
	Method string
	Arg    []byte
	Oneway bool
}

type ResponseMsg struct {
	Seq uint64
	Err *Error
	Ret []byte
}

//encode/decode Arg/Ret
type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

type Channel interface {
	SendRequest(*RequestMsg, time.Time) error
	SendRequestWithContext(context.Context, *RequestMsg) error
	Reply(*ResponseMsg) error
	Name() string
	Identity() uint64
}
