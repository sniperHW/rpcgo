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
	Code        int
	Description string
}

const (
	ErrOk                  = 0
	ErrInvaildMethod       = 1
	ErrServerPause         = 2
	ErrBusy                = 3
	ErrRuntime             = 4
	ErrTimeout             = 5
	ErrChannelDisconnected = 6
	ErrSend                = 7
	ErrEncode              = 8
	ErrDecode              = 9
)

type RPCRequestMessage struct {
	Seq    uint64
	Method string
	Arg    []byte
}

type RPCResponseMessage struct {
	Seq uint64
	Err *Error
	Ret []byte
}

//encode/decode Arg/Ret
type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

type RPCChannel interface {
	SendRequest(*RPCRequestMessage, time.Time) error
	SendRequestWithContext(context.Context, *RPCRequestMessage) error
	Reply(*RPCResponseMessage) error
	Name() string
	Identity() uint64
}
