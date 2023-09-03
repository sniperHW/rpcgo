package rpcgo

//go test -race -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
	"unsafe"

	"github.com/sniperHW/netgo"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
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
	InitLogger(zap.NewExample().Sugar())
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

func (c *testChannel) SendRequest(request *RequestMsg, deadline time.Time) error {
	return c.socket.Send(request, deadline)
}

func (c *testChannel) SendRequestWithContext(ctx context.Context, request *RequestMsg) error {
	return c.socket.SendWithContext(ctx, request)
}

func (c *testChannel) Reply(response *ResponseMsg) error {
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
		return DecodeRequest(b[1:])
	case packet_rpc_response:
		return DecodeResponse(b[1:])
	case packet_msg:
		return string(b[1:]), nil
	default:
		return nil, errors.New("invaild packet")
	}
}

func (codec *PacketCodec) Encode(buffs net.Buffers, o interface{}) (net.Buffers, int) {
	logger.Debugf("pack %v", o)
	var headBytes []byte
	var dataBytes []byte
	switch o := o.(type) {
	case *RequestMsg:
		headBytes = AppendUint32(headBytes, 0)
		headBytes = AppendByte(headBytes, packet_rpc_request)
		dataBytes, _ = EncodeRequest(o) //json.Marshal(o)
	case *ResponseMsg:
		headBytes = AppendUint32(headBytes, 0)
		headBytes = AppendByte(headBytes, packet_rpc_response)
		dataBytes, _ = EncodeResponse(o) //json.Marshal(o)
	case string:
		headBytes = AppendUint32(headBytes, 0)
		headBytes = AppendByte(headBytes, packet_msg)
		dataBytes = []byte(o)
	default:
		return buffs, 0
	}

	binary.BigEndian.PutUint32(headBytes, uint32(len(dataBytes)+1))
	return append(buffs, headBytes, dataBytes), len(dataBytes) + 5
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
		if (codec.w - rr) >= lenHead { //&& uint32(codec.w-rr-lenHead) >= binary.BigEndian.Uint32(codec.buff[rr:]) {
			pktLen = int(binary.BigEndian.Uint32(codec.buff[rr:]))
			logger.Debugf("on packet pktLen %d", pktLen)
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
		logger.Debugf("on read %d", n)
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

	rpcServer.Register("hello", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg), nil)
	})

	rpcServer.Register("timeout", func(_ context.Context, replyer *Replyer, arg *string) {
		go func() {
			time.Sleep(time.Second * 5)
			logger.Debugf("timeout reply")
			replyer.Reply(fmt.Sprintf("timeout hello world:%s", *arg), nil)
		}()
	})

	listener, serve, _ := netgo.ListenTCP("tcp", "localhost:8110", func(conn *net.TCPConn) {
		logger.Debugf("on new client")
		codec := &PacketCodec{buff: make([]byte, 4096)}
		as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn, codec),
			netgo.AsynSocketOption{
				Codec:    codec,
				AutoRecv: true,
			})
		as.SetPacketHandler(func(context context.Context, as *netgo.AsynSocket, packet interface{}) error {
			switch packet := packet.(type) {
			case string:
				logger.Debugf("on message")
				as.Send(packet)
			case *RequestMsg:
				rpcServer.OnMessage(context, &testChannel{socket: as}, packet)
			}
			return nil
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
	as.SetPacketHandler(func(context context.Context, as *netgo.AsynSocket, packet interface{}) error {
		switch packet := packet.(type) {
		case string:
			close(msgChan)
		case *ResponseMsg:
			rpcClient.OnMessage(context, packet)
		}
		return nil
	}).Recv()
	as.Send("msg")
	<-msgChan

	logger.Debugf("begin rpc call")

	var resp string
	err := rpcClient.Call(context.TODO(), rpcChannel, "hello", "sniperHW", &resp)
	assert.Nil(t, err)
	assert.Equal(t, resp, "hello world:sniperHW")

	c := make(chan struct{})

	rpcClient.CallWithCallback(rpcChannel, time.Now().Add(time.Second), "hello", "hw", &resp, func(resp interface{}, err error) {
		assert.Equal(t, *resp.(*string), "hello world:hw")
		close(c)
	})

	<-c

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()
	err = rpcClient.Call(ctx, rpcChannel, "timeout", "sniperHW", &resp)
	assert.Equal(t, err.(*Error).Is(ErrTimeout), true)

	{
		cancel, _ := rpcClient.CallWithCallback(rpcChannel, time.Now().Add(time.Second), "timeout", "hw", &resp, func(resp interface{}, err error) {
			assert.Equal(t, *resp.(*string), "hello world:hw")
			panic("should not reach here")
		})

		time.Sleep(time.Millisecond * 100)

		assert.Equal(t, true, cancel())

		time.Sleep(time.Second * 5)
	}

	c = make(chan struct{})

	rpcServer.Register("syncOneway", func(_ context.Context, replyer *Replyer, arg *string) {
		logger.Debugf("syncOneway %s", *arg)
		replyer.Reply(*arg, nil)
		close(c)
	})

	err = rpcClient.Call(context.TODO(), rpcChannel, "syncOneway", "sniperHW", nil)
	assert.Nil(t, err)
	<-c

	c = make(chan struct{})

	rpcServer.Register("ayncOneway", func(_ context.Context, replyer *Replyer, arg *string) {
		logger.Debugf("ayncOneway %s", *arg)
		replyer.Reply(*arg, nil)
		close(c)
	})

	rpcClient.CallWithCallback(rpcChannel, time.Now().Add(time.Second), "ayncOneway", "hw", nil, nil)

	<-c

	c = make(chan struct{})

	rpcClient.CallWithCallback(rpcChannel, time.Now().Add(time.Second), "timeout", "hw", &resp, func(resp interface{}, err error) {
		assert.Equal(t, err.(*Error).Is(ErrTimeout), true)
		close(c)
	})

	<-c

	rpcServer.UnRegister("hello")

	err = rpcClient.Call(context.TODO(), rpcChannel, "hello", "sniperHW", &resp)
	assert.Equal(t, err.(*Error).Is(ErrInvaildMethod), true)

	rpcServer.Register("panic", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer = nil
		//cause panic
		replyer.Reply(*arg, nil)
	})

	err = rpcClient.Call(context.TODO(), rpcChannel, "panic", "sniperHW", &resp)
	assert.Equal(t, err.Error(), "method panic")

	rpcServer.Pause()

	err = rpcClient.Call(context.TODO(), rpcChannel, "timeout", "sniperHW", &resp)
	assert.Equal(t, err.(*Error).Is(ErrServerPause), true)

	as.Close(nil)

	listener.Close()

}

func TestEnDeCode(t *testing.T) {
	{
		req := &RequestMsg{
			Seq:    10012,
			Method: "test",
			Arg:    []byte("hello"),
			Oneway: true,
		}

		b, _ := EncodeRequest(req)

		req = nil

		req, _ = DecodeRequest(b)

		assert.Equal(t, req.Seq, uint64(10012))

		assert.Equal(t, req.Method, "test")

		assert.Equal(t, req.Oneway, true)

		assert.Equal(t, req.Arg, []byte("hello"))
	}

	{
		req := &RequestMsg{
			Seq:    10012,
			Method: "test",
			Arg:    []byte("hello"),
		}

		b, _ := EncodeRequest(req)

		req = nil

		req, _ = DecodeRequest(b)

		assert.Equal(t, req.Seq, uint64(10012))

		assert.Equal(t, req.Method, "test")

		assert.Equal(t, req.Oneway, false)

		assert.Equal(t, req.Arg, []byte("hello"))
	}

	{

		resp := &ResponseMsg{
			Seq: 10012,
			Ret: []byte("hello"),
		}

		b, _ := EncodeResponse(resp)

		resp = nil

		resp, _ = DecodeResponse(b)

		assert.Equal(t, resp.Seq, uint64(10012))

		assert.Nil(t, resp.Err)

		assert.Equal(t, resp.Ret, []byte("hello"))
	}

	{

		resp := &ResponseMsg{
			Seq: 10012,
			Ret: []byte("hello"),
			Err: NewError(ErrOther, "error"),
		}

		b, _ := EncodeResponse(resp)

		resp = nil

		resp, _ = DecodeResponse(b)

		assert.Equal(t, resp.Seq, uint64(10012))

		assert.Equal(t, resp.Err.Error(), "error")

		assert.Equal(t, resp.Ret, []byte("hello"))
	}

}
