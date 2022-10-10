package rpcgo

//go test -race -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sniperHW/netgo"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"net"
	"testing"
	"time"
	"unsafe"
)

type JsonCodec struct {
}

func (c *JsonCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (c *JsonCodec) Decode(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}

func init() {
	InitLogger(zap.NewExample())
}

func AppendByte(bs []byte, v byte) []byte {
	return append(bs, v)
}

func AppendBytes(bs []byte, bytes []byte) []byte {
	return append(bs, bytes...)
}

func AppendUint32(bs []byte, u32 uint32) []byte {
	bu := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(bu, u32)
	return AppendBytes(bs, bu)
}

const (
	packet_msg          = byte(1)
	packet_rpc_request  = byte(2)
	packet_rpc_response = byte(3)
)

type testChannel struct {
	socket *netgo.AsynSocket
}

func (c *testChannel) SendRequest(request *RPCRequestMessage, deadline time.Time) error {
	return c.socket.Send(request, deadline)
}

func (c *testChannel) SendRequestWithContext(ctx context.Context, request *RPCRequestMessage) error {
	return c.socket.SendWithContext(ctx, request)
}

func (c *testChannel) Reply(response *RPCResponseMessage) error {
	return c.socket.Send(response)
}

func (c *testChannel) Name() string {
	return fmt.Sprintf("%v <-> %v", c.socket.LocalAddr(), c.socket.RemoteAddr())
}

func (c *testChannel) Identity() uint64 {
	return *(*uint64)(unsafe.Pointer(c.socket))
}

type PacketCodec struct {
	r    int
	w    int
	buff []byte
}

func (codec *PacketCodec) Decode(b []byte) (interface{}, error) {
	switch b[0] {
	case packet_rpc_request:
		request := &RPCRequestMessage{}
		json.Unmarshal(b[1:], request)
		return request, nil
	case packet_rpc_response:
		response := &RPCResponseMessage{}
		json.Unmarshal(b[1:], response)
		return response, nil
	case packet_msg:
		return string(b[1:]), nil
	default:
		return nil, errors.New("invaild packet")
	}
}

func (codec *PacketCodec) Encode(b []byte, o interface{}) []byte {
	logger.Sugar().Debugf("pack %v", o)
	offset := len(b)
	var bytes []byte
	switch o.(type) {
	case *RPCRequestMessage:
		request := o.(*RPCRequestMessage)
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_rpc_request)
		bytes, _ = json.Marshal(request)
	case *RPCResponseMessage:
		response := o.(*RPCResponseMessage)
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_rpc_response)
		bytes, _ = json.Marshal(response)
	case string:
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_msg)
		bytes = []byte(o.(string))
	default:
		return b
	}
	b = AppendBytes(b, bytes)
	binary.BigEndian.PutUint32(b[offset:], uint32(len(b)-offset-4))
	logger.Sugar().Debugf("len %d", len(b))
	return b
}

func (codec *PacketCodec) read(readable netgo.ReadAble, deadline time.Time) (int, error) {
	if err := readable.SetReadDeadline(deadline); err != nil {
		return 0, err
	} else {
		return readable.Read(codec.buff[codec.w:])
	}
}

func (codec *PacketCodec) Recv(readable netgo.ReadAble, deadline time.Time) (pkt []byte, err error) {
	const lenHead int = 4
	for {
		rr := codec.r
		pktLen := 0
		if (codec.w-rr) >= lenHead && uint32(codec.w-rr-lenHead) >= binary.BigEndian.Uint32(codec.buff[rr:]) {
			pktLen = int(binary.BigEndian.Uint32(codec.buff[rr:]))
			logger.Sugar().Debugf("on packet pktLen %d", pktLen)
			rr += lenHead
		}

		if pktLen > 0 {
			if pktLen > (len(codec.buff) - lenHead) {
				err = errors.New("pkt too large")
				return
			}
			if (codec.w - rr) >= pktLen {
				pkt = codec.buff[rr : rr+pktLen]
				rr += pktLen
				codec.r = rr
				if codec.r == codec.w {
					codec.r = 0
					codec.w = 0
				}
				return
			}
		}

		if codec.r > 0 {
			//移动到头部
			copy(codec.buff, codec.buff[codec.r:codec.w])
			codec.w = codec.w - codec.r
			codec.r = 0
		}

		var n int
		n, err = codec.read(readable, deadline)
		logger.Sugar().Debugf("on read %d", n)
		if n > 0 {
			codec.w += n
		}
		if nil != err {
			return
		}
	}
}

func TestRPC(t *testing.T) {
	rpcServer := NewServer(&JsonCodec{})

	rpcServer.RegisterMethod("hello", func(replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg), nil)
	})

	rpcServer.RegisterMethod("timeout", func(replyer *Replyer, arg *string) {
		go func() {
			time.Sleep(time.Second * 5)
			logger.Sugar().Debugf("timeout reply")
			replyer.Reply(fmt.Sprintf("timeout hello world:%s", *arg), nil)
		}()
	})

	listener, serve, _ := netgo.ListenTCP("tcp", "localhost:8110", func(conn *net.TCPConn) {
		logger.Sugar().Debugf("on new client")
		codec := &PacketCodec{buff: make([]byte, 4096)}
		as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn, codec),
			netgo.AsynSocketOption{
				Codec:    codec,
				AutoRecv: true,
			})
		as.SetPacketHandler(func(as *netgo.AsynSocket, packet interface{}) {
			switch packet.(type) {
			case string:
				logger.Sugar().Debugf("on message")
				as.Send(packet)
			case *RPCRequestMessage:
				rpcServer.OnRPCMessage(&testChannel{socket: as}, packet.(*RPCRequestMessage))
			}
		}).Recv()
	})

	go serve()

	dialer := &net.Dialer{}
	conn, _ := dialer.Dial("tcp", "localhost:8110")
	codec := &PacketCodec{buff: make([]byte, 4096)}
	as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), codec),
		netgo.AsynSocketOption{
			Codec:    codec,
			AutoRecv: true,
		})

	msgChan := make(chan struct{})

	rpcChannel := &testChannel{socket: as}
	rpcClient := NewClient(&JsonCodec{})
	as.SetPacketHandler(func(as *netgo.AsynSocket, packet interface{}) {
		switch packet.(type) {
		case string:
			close(msgChan)
		case *RPCResponseMessage:
			rpcClient.OnRPCMessage(packet.(*RPCResponseMessage))
		}
	}).Recv()
	as.Send("msg")
	<-msgChan

	logger.Sugar().Debugf("begin rpc call")

	var resp string
	err := rpcClient.Call(context.TODO(), rpcChannel, "hello", "sniperHW", &resp)
	assert.Nil(t, err)
	assert.Equal(t, resp, "hello world:sniperHW")

	c := make(chan struct{})

	rpcClient.AsynCall(rpcChannel, time.Now().Add(time.Second), "hello", "hw", &resp, func(resp interface{}, err *Error) {
		assert.Equal(t, *resp.(*string), "hello world:hw")
		close(c)
	})

	<-c

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()
	err = rpcClient.Call(ctx, rpcChannel, "timeout", "sniperHW", &resp)
	assert.Equal(t, err.Code, ErrTimeout)

	{
		cancel := rpcClient.AsynCall(rpcChannel, time.Now().Add(time.Second), "timeout", "hw", &resp, func(resp interface{}, err *Error) {
			assert.Equal(t, *resp.(*string), "hello world:hw")
			panic("should not reach here")
		})

		time.Sleep(time.Millisecond * 100)

		assert.Equal(t, true, cancel())

		time.Sleep(time.Second * 5)
	}

	c = make(chan struct{})

	rpcServer.RegisterMethod("syncOneway", func(replyer *Replyer, arg *string) {
		logger.Sugar().Debugf("syncOneway %s", *arg)
		replyer.Reply(*arg, nil)
		close(c)
	})

	err = rpcClient.Call(context.TODO(), rpcChannel, "syncOneway", "sniperHW", nil)
	assert.Nil(t, err)
	<-c

	c = make(chan struct{})

	rpcServer.RegisterMethod("ayncOneway", func(replyer *Replyer, arg *string) {
		logger.Sugar().Debugf("ayncOneway %s", *arg)
		replyer.Reply(*arg, nil)
		close(c)
	})

	rpcClient.AsynCall(rpcChannel, time.Now().Add(time.Second), "ayncOneway", "hw", nil, nil)

	<-c

	c = make(chan struct{})

	rpcClient.AsynCall(rpcChannel, time.Now().Add(time.Second), "timeout", "hw", &resp, func(resp interface{}, err *Error) {
		assert.Equal(t, err.Code, ErrTimeout)
		close(c)
	})

	<-c

	err = rpcClient.Call(context.TODO(), rpcChannel, "invaild method", "sniperHW", &resp)
	assert.Equal(t, err.Code, ErrInvaildMethod)

	rpcServer.RegisterMethod("panic", func(replyer *Replyer, arg *string) {
		replyer = nil
		//should panic here
		replyer.Reply(*arg, nil)
	})

	err = rpcClient.Call(context.TODO(), rpcChannel, "panic", "sniperHW", &resp)
	assert.Equal(t, err.Code, ErrRuntime)

	rpcServer.Pause()

	err = rpcClient.Call(context.TODO(), rpcChannel, "timeout", "sniperHW", &resp)
	assert.Equal(t, err.Code, ErrServerPause)

	as.Close(nil)

	listener.Close()

}
