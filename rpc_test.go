package rpcgo

//go test -race -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/sniperHW/network"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"net"
	"testing"
	"time"
	"unsafe"
)

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

func AppendUint16(bs []byte, u16 uint16) []byte {
	bu := []byte{0, 0}
	binary.BigEndian.PutUint16(bu, u16)
	return AppendBytes(bs, bu)
}

func AppendUint32(bs []byte, u32 uint32) []byte {
	bu := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(bu, u32)
	return AppendBytes(bs, bu)
}

func AppendUint64(bs []byte, u64 uint64) []byte {
	bu := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(bu, u64)
	return AppendBytes(bs, bu)
}

type BufferReader struct {
	bs     []byte
	offset int
}

func NewReader(b []byte) BufferReader {
	return BufferReader{bs: b}
}

func (this *BufferReader) Reset(b []byte) {
	if len(b) > 0 {
		this.bs = b
	}
	this.offset = 0
}

func (this *BufferReader) GetAll() []byte {
	return this.bs[this.offset:]
}

func (this *BufferReader) GetOffset() int {
	return this.offset
}

func (this *BufferReader) IsOver() bool {
	return this.offset >= len(this.bs)
}

func (this *BufferReader) GetByte() byte {
	if this.offset+1 > len(this.bs) {
		return 0
	} else {
		ret := this.bs[this.offset]
		this.offset += 1
		return ret
	}
}

func (this *BufferReader) GetUint16() uint16 {
	if this.offset+2 > len(this.bs) {
		return 0
	} else {
		ret := binary.BigEndian.Uint16(this.bs[this.offset : this.offset+2])
		this.offset += 2
		return ret
	}
}

func (this *BufferReader) GetUint32() uint32 {
	if this.offset+4 > len(this.bs) {
		return 0
	} else {
		ret := binary.BigEndian.Uint32(this.bs[this.offset : this.offset+4])
		this.offset += 4
		return ret
	}
}

func (this *BufferReader) GetUint64() uint64 {
	if this.offset+8 > len(this.bs) {
		return 0
	} else {
		ret := binary.BigEndian.Uint64(this.bs[this.offset : this.offset+8])
		this.offset += 8
		return ret
	}
}

func (this *BufferReader) GetString(size int) string {
	return string(this.GetBytes(size))
}

func (this *BufferReader) GetBytes(size int) []byte {
	if len(this.bs)-this.offset < size {
		size = len(this.bs) - this.offset
	}
	ret := this.bs[this.offset : this.offset+size]
	this.offset += size
	return ret
}

const (
	packet_msg          = byte(1)
	packet_rpc_request  = byte(2)
	packet_rpc_response = byte(3)
)

type testChannel struct {
	socket *network.AsynSocket
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
	reader BufferReader
}

func (d *PacketDecoder) Decode(b []byte) (interface{}, error) {
	d.reader.Reset(b)
	switch d.reader.GetByte() {
	case packet_rpc_request:
		request := &RPCRequestMessage{}
		request.Seq = d.reader.GetUint64()
		lenMethod := int(d.reader.GetByte())
		request.Method = d.reader.GetString(lenMethod)
		lenArg := int(d.reader.GetByte())
		request.Arg = d.reader.GetString(lenArg)
		return request, nil
	case packet_rpc_response:
		response := &RPCResponseMessage{}
		response.Seq = d.reader.GetUint64()
		errCode := int(d.reader.GetByte())
		if errCode == 0 {
			lenRet := int(d.reader.GetByte())
			response.Ret = d.reader.GetString(lenRet)
		} else {
			lenErr := int(d.reader.GetByte())
			response.Err = NewError(errCode, d.reader.GetString(lenErr))
		}
		return response, nil
	case packet_msg:
		lenMsg := int(d.reader.GetByte())
		return d.reader.GetString(lenMsg), nil
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
		b = AppendUint64(b, request.Seq)
		b = AppendByte(b, byte(len(request.Method)))
		b = AppendString(b, request.Method)
		b = AppendByte(b, byte(len(request.Arg.(string))))
		b = AppendString(b, request.Arg.(string))
	case *RPCResponseMessage:
		response := o.(*RPCResponseMessage)
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_rpc_response)
		b = AppendUint64(b, response.Seq)
		if response.Err == nil {
			b = AppendByte(b, byte(0))
			b = AppendByte(b, byte(len(response.Ret.(string))))
			b = AppendString(b, response.Ret.(string))
		} else {
			b = AppendByte(b, byte(response.Err.Code()))
			b = AppendByte(b, byte(len(response.Err.Description())))
			b = AppendString(b, response.Err.Description())
		}
	case string:
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_msg)
		b = AppendByte(b, byte(len(o.(string))))
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

func (r *PacketReceiver) read(readable network.ReadAble, deadline time.Time) (n int, err error) {
	if deadline.IsZero() {
		readable.SetReadDeadline(time.Time{})
		n, err = readable.Read(r.buff[r.w:])
	} else {
		readable.SetReadDeadline(deadline)
		n, err = readable.Read(r.buff[r.w:])
	}
	return
}

func (r *PacketReceiver) Recv(readable network.ReadAble, deadline time.Time) (pkt []byte, err error) {
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
	rpcServer := NewServer()

	rpcServer.RegisterMethod("hello", func(req RPCRequest) {
		req.Reply(fmt.Sprintf("hello world:%s", req.Argumment().(string)), nil)
	})

	listener, serve, _ := network.ListenTCP("tcp", "localhost:8110", func(conn *net.TCPConn) {
		logger.Sugar().Debugf("on new client")
		as := network.NewAsynSocket(network.NewTcpSocket(conn, &PacketReceiver{buff: make([]byte, 4096)}),
			network.AsynSocketOption{
				Decoder:  &PacketDecoder{},
				Packer:   &PacketPacker{},
				AutoRecv: true,
			})
		as.SetPacketHandler(func(as *network.AsynSocket, packet interface{}) {
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
	as := network.NewAsynSocket(network.NewTcpSocket(conn.(*net.TCPConn), &PacketReceiver{buff: make([]byte, 4096)}),
		network.AsynSocketOption{
			Decoder:  &PacketDecoder{},
			Packer:   &PacketPacker{},
			AutoRecv: true,
		})

	msgChan := make(chan struct{})

	rpcChannel := &testChannel{socket: as}
	rpcClient := NewClient()
	as.SetPacketHandler(func(as *network.AsynSocket, packet interface{}) {
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
	resp, err := rpcClient.Call(context.TODO(), rpcChannel, &RPCRequestMessage{
		Method: "hello",
		Arg:    "sniperHW"})
	assert.Nil(t, err)
	assert.Equal(t, resp.(string), "hello world:sniperHW")

	resp, err = rpcClient.Call(context.TODO(), rpcChannel, &RPCRequestMessage{
		Method: "world",
		Arg:    "sniperHW"})
	assert.Equal(t, err.Description(), "method world not found")

	rpcServer.Pause()

	c := make(chan struct{})

	rpcClient.AsynCall(rpcChannel, &RPCRequestMessage{
		Method: "hello",
		Arg:    "sniperHW",
	}, time.Now().Add(time.Second), func(_ interface{}, err *Error) {
		assert.Equal(t, err.Description(), "server pause")
		close(c)
	})

	<-c

	as.Close(nil)

	listener.Close()

}
