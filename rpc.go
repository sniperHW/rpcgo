package rpcgo

import (
	"context"
	"encoding/binary"
	"errors"
)

var logger Logger

func InitLogger(l Logger) {
	logger = l
}

/*
 *  注意,传递给RPC模块的所有回调函数可能在底层信道的接收/发送goroutine上执行，
 *  为了避免接收/发送goroutine被阻塞，回调函数中不能调用阻塞函数。
 *  如需调用阻塞函数，请在回调中启动一个goroutine来执行
 */

type Error struct {
	code int
	str  string
}

func NewError(code int, err string) *Error {
	if code <= 0 || code >= errEnd {
		return nil
	} else {
		return &Error{code: code, str: err}
	}
}

func (e *Error) Error() string {
	return e.str
}

func (e *Error) Is(code int) bool {
	return e.code == code
}

const (
	ErrOk = iota
	ErrInvaildMethod
	ErrTimeout
	ErrCancel
	ErrMethod
	ErrOther
	ErrServiceUnavaliable
	errEnd
)

type RequestMsg struct {
	Seq     uint64
	Method  string
	Arg     []byte
	Oneway  bool
	arg     interface{}
	replyer *Replyer
}

func (r RequestMsg) GetArg() interface{} {
	return r.arg
}

func (r *RequestMsg) SetReplyHook(fn func(*RequestMsg, error)) {
	r.replyer.hook = fn
}

type ResponseMsg struct {
	Seq uint64
	Err *Error
	Ret []byte
}

const (
	lenSeq       = 8
	lenOneWay    = 1
	lenMethod    = 2
	maxMethodLen = 65535
	reqHdrLen    = lenSeq + lenOneWay + lenMethod // seq + oneway + len(method)
	maxErrStrLen = 65535
	lenErrCode   = 2
	respHdrLen   = lenSeq + lenErrCode //seq + Error.Err.Code
	lenErrStr    = 2
)

func EncodeRequest(req *RequestMsg) []byte {
	method := []byte(req.Method)

	if len(method) > maxMethodLen {
		method = method[:maxMethodLen]
	}

	buff := make([]byte, 11, reqHdrLen+len(method)+len(req.Arg))

	binary.BigEndian.PutUint64(buff, req.Seq)

	if req.Oneway {
		buff[8] = byte(1)
	}

	binary.BigEndian.PutUint16(buff[9:], uint16(len(req.Method)))

	buff = append(buff, method...)

	buff = append(buff, req.Arg...)

	return buff
}

func DecodeRequest(buff []byte) (*RequestMsg, error) {
	var req RequestMsg
	r := 0
	buffLen := len(buff)
	if buffLen-r < lenSeq {
		return nil, errors.New("invaild request packet")
	}
	req.Seq = binary.BigEndian.Uint64(buff[r:])
	r += lenSeq
	if buffLen-r < lenOneWay {
		return nil, errors.New("invaild request packet")
	}
	if buff[r] == byte(1) {
		req.Oneway = true
	}
	r += lenOneWay
	if buffLen-r < lenMethod {
		return nil, errors.New("invaild request packet")
	}
	methodLen := binary.BigEndian.Uint16(buff[r:])
	r += lenMethod
	if buffLen-r < int(methodLen) {
		return nil, errors.New("invaild request packet")
	}
	req.Method = string(buff[r : r+int(methodLen)])
	r += int(methodLen)

	if buffLen-r > 0 {
		req.Arg = make([]byte, 0, buffLen-r)
		req.Arg = append(req.Arg, buff[r:]...)
	}

	return &req, nil
}

func EncodeResponse(resp *ResponseMsg) []byte {

	var buff []byte
	var errByte []byte

	if resp.Err == nil {
		buff = make([]byte, 10, respHdrLen+len(resp.Ret))
	} else {
		errByte = []byte(resp.Err.str)
		if len(errByte) > maxErrStrLen {
			errByte = errByte[:maxErrStrLen]
		}
		buff = make([]byte, 10, respHdrLen+lenErrStr+len(errByte)+len(resp.Ret))
	}

	binary.BigEndian.PutUint64(buff, resp.Seq)

	if resp.Err != nil {
		binary.BigEndian.PutUint16(buff[8:], uint16(resp.Err.code))
		errStrLen := []byte{0, 0}
		binary.BigEndian.PutUint16(errStrLen, uint16(len(errByte)))
		buff = append(buff, errStrLen...)
		buff = append(buff, errByte...)
	}

	buff = append(buff, resp.Ret...)

	return buff
}

func DecodeResponse(buff []byte) (*ResponseMsg, error) {
	var resp ResponseMsg
	r := 0
	buffLen := len(buff)
	if buffLen-r < lenSeq {
		return nil, errors.New("invaild response packet")
	}
	resp.Seq = binary.BigEndian.Uint64(buff[r:])
	r += lenSeq

	if buffLen-r < lenErrCode {
		return nil, errors.New("invaild response packet")
	}

	errCode := binary.BigEndian.Uint16(buff[r:])

	r += lenErrCode

	if errCode != 0 {
		if buffLen-r < lenErrStr {
			return nil, errors.New("invaild response packet")
		}
		errStrLen := binary.BigEndian.Uint16(buff[r:])
		r += lenErrStr
		if buffLen-r < int(errStrLen) {
			return nil, errors.New("invaild response packet")
		}

		resp.Err = NewError(int(errCode), string(buff[r:r+int(errStrLen)]))
		r += int(errStrLen)
	}

	if buffLen-r > 0 {
		resp.Ret = make([]byte, 0, buffLen-r)
		resp.Ret = append(resp.Ret, buff[r:]...)
	}

	return &resp, nil
}

// encode/decode Arg/Ret
type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

type Channel interface {
	SendRequest(context.Context, *RequestMsg) error
	Reply(*ResponseMsg) error
	Name() string
	Identity() uint64
	IsRetryAbleError(error) bool
}
