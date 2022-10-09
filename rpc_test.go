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

func AppendString(bs []byte, s string) []byte {
	return append(bs, s...)
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

type PacketDecoder struct {
}

func (d *PacketDecoder) Decode(b []byte) (interface{}, error) {
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

type PacketPacker struct {
}

func (e *PacketPacker) Pack(b []byte, o interface{}) []byte {
	logger.Sugar().Debugf("pack %v", o)
	offset := len(b)
	switch o.(type) {
	case *RPCRequestMessage:
		request := o.(*RPCRequestMessage)
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_rpc_request)
		jsonByte, _ := json.Marshal(request)
		b = AppendBytes(b, jsonByte)
	case *RPCResponseMessage:
		response := o.(*RPCResponseMessage)
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_rpc_response)
		jsonByte, _ := json.Marshal(response)
		b = AppendBytes(b, jsonByte)
	case string:
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_msg)
		b = AppendString(b, o.(string))
	default:
		return b
	}
	binary.BigEndian.PutUint32(b[offset:], uint32(len(b)-offset-4))
	logger.Sugar().Debugf("len %d", len(b))
	return b
}

type PacketReceiver struct {
	r    int
	w    int
	buff []byte
}

func (r *PacketReceiver) read(readable netgo.ReadAble, deadline time.Time) (n int, err error) {
	if deadline.IsZero() {
		readable.SetReadDeadline(time.Time{})
		n, err = readable.Read(r.buff[r.w:])
	} else {
		readable.SetReadDeadline(deadline)
		n, err = readable.Read(r.buff[r.w:])
	}
	return
}

func (r *PacketReceiver) Recv(readable netgo.ReadAble, deadline time.Time) (pkt []byte, err error) {
	const lenHead int = 4
	for {
		rr := r.r
		pktLen := 0
		if (r.w-rr) >= lenHead && uint32(r.w-rr-lenHead) >= binary.BigEndian.Uint32(r.buff[rr:]) {
			pktLen = int(binary.BigEndian.Uint32(r.buff[rr:]))
			logger.Sugar().Debugf("on packet pktLen %d", pktLen)
			rr += lenHead
		}

		if pktLen > 0 {
			if pktLen > (len(r.buff) - lenHead) {
				err = errors.New("pkt too large")
				return
			}
			if (r.w - rr) >= pktLen {
				pkt = r.buff[rr : rr+pktLen]
				rr += pktLen
				r.r = rr
				if r.r == r.w {
					r.r = 0
					r.w = 0
				}
				return
			}
		}

		if r.r > 0 {
			//移动到头部
			copy(r.buff, r.buff[r.r:r.w])
			r.w = r.w - r.r
			r.r = 0
		}

		var n int
		n, err = r.read(readable, deadline)
		logger.Sugar().Debugf("on read %d", n)
		if n > 0 {
			r.w += n
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

	listener, serve, _ := netgo.ListenTCP("tcp", "localhost:8110", func(conn *net.TCPConn) {
		logger.Sugar().Debugf("on new client")
		as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn, &PacketReceiver{buff: make([]byte, 4096)}),
			netgo.AsynSocketOption{
				Decoder:  &PacketDecoder{},
				Packer:   &PacketPacker{},
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
	as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), &PacketReceiver{buff: make([]byte, 4096)}),
		netgo.AsynSocketOption{
			Decoder:  &PacketDecoder{},
			Packer:   &PacketPacker{},
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

	/*
		resp, err = rpcClient.Call(context.TODO(), rpcChannel, &RPCRequestMessage{
			Method: "world",
			Arg:    "sniperHW"})
		assert.Equal(t, err.Description(), "method world not found")

		rpcServer.Pause()
	*/

	as.Close(nil)

	listener.Close()

}
