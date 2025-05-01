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
	"sync"
	"testing"
	"time"

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

func (c *testChannel) RequestWithContext(ctx context.Context, request *RequestMsg) error {
	return c.socket.SendWithContext(ctx, request)
}

func (c *testChannel) Request(request *RequestMsg) error {
	return c.socket.Send(request, time.Time{})
}

func (c *testChannel) Reply(response *ResponseMsg) error {
	return c.socket.Send(response)
}

func (c *testChannel) Name() string {
	return fmt.Sprintf("%v <-> %v", c.socket.LocalAddr(), c.socket.RemoteAddr())
}

func (c *testChannel) IsRetryAbleError(_ error) bool {
	return false
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
	var headBytes []byte
	var dataBytes []byte
	switch o := o.(type) {
	case *RequestMsg:
		headBytes = AppendUint32(headBytes, 0)
		headBytes = AppendByte(headBytes, packet_rpc_request)
		dataBytes = EncodeRequest(o)
	case *ResponseMsg:
		headBytes = AppendUint32(headBytes, 0)
		headBytes = AppendByte(headBytes, packet_rpc_response)
		dataBytes = EncodeResponse(o)
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
		if (codec.w - rr) >= lenHead {
			pktLen = int(binary.BigEndian.Uint32(codec.buff[rr:]))
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
		if n > 0 {
			codec.w += n
		}
		if nil != err {
			return
		}
	}
}

type server struct {
	rpcServer *Server
	l         net.Listener
	serve     func()
}

func (s *server) start() {
	go s.serve()
}

func (s *server) stop() {
	s.l.Close()
	s.rpcServer.Stop()
}

func newServer(service string) (s *server, err error) {
	s = &server{
		rpcServer: NewServer(&JsonCodec{}),
	}
	s.l, s.serve, err = netgo.ListenTCP("tcp", service, func(conn *net.TCPConn) {
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
				s.rpcServer.OnMessage(context, &testChannel{socket: as}, packet)
			}
			return nil
		}).Recv()
	})

	if err != nil {
		return nil, err
	} else {
		return s, err
	}
}

type client struct {
	rpcClient     *Client
	rpcChannel    *testChannel
	packetHandler func(context context.Context, packet interface{})
}

func newClient(remote string) (*client, error) {
	dialer := &net.Dialer{}
	conn, err := dialer.Dial("tcp", remote)
	if err != nil {
		return nil, err
	}
	codec := &PacketCodec{buff: make([]byte, 4096)}
	as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), codec),
		netgo.AsynSocketOption{
			Codec:    codec,
			AutoRecv: true,
		})

	rpcChannel := &testChannel{socket: as}
	rpcClient := NewClient(&JsonCodec{})

	c := &client{
		rpcClient:  rpcClient,
		rpcChannel: rpcChannel,
	}

	as.SetPacketHandler(func(context context.Context, as *netgo.AsynSocket, packet interface{}) error {
		switch packet := packet.(type) {
		case *ResponseMsg:
			rpcClient.OnMessage(nil, packet)
		default:
			if c.packetHandler != nil {
				c.packetHandler(context, packet)
			}
		}
		return nil
	}).Recv()

	return c, nil
}

func TestCaller(t *testing.T) {
	s, _ := newServer("localhost:8110")
	s.rpcServer.SetInInterceptor(append([]func(replyer *Replyer, req *RequestMsg) bool{}, func(replyer *Replyer, req *RequestMsg) bool {
		beg := time.Now()
		//设置钩子函数,当Replyer发送应答时调用
		replyer.AppendOutInterceptor(func(req *RequestMsg, ret interface{}, err error) {
			if err == nil {
				logger.Debugf("serve %s(\"%v\") resp:%v use:%v", req.Method, *req.GetArg().(*string), ret, time.Now().Sub(beg))
			} else {
				logger.Debugf("serve %s(\"%v\") with error:%v", req.Method, *req.GetArg().(*string), err)
			}
		})
		return true
	}))

	Register[string](s.rpcServer, "hello", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg))
	})

	s.start()

	cli, _ := newClient("localhost:8110")

	//caller := MakeCaller[string, string](cli.rpcClient, "hello", cli.rpcChannel)
	//caller.Oneway(CallerOpt{}, MakeArgument("Oneway"))

	Post(context.Background(), cli.rpcClient, cli.rpcChannel, "hello", "Oneway")

	r, err := Call[string, string](context.Background(), cli.rpcClient, cli.rpcChannel, "hello", "CallWithTimeout") //caller.Call(CallerOpt{Timeout: time.Second}, MakeArgument("CallWithTimeout"))
	if err == nil {
		fmt.Println(*r, err)
	}

	c := make(chan struct{})

	AsyncCall(cli.rpcClient, cli.rpcChannel, "hello", "AsyncCall", time.Now().Add(time.Second), func(r *string, err error) {
		if err == nil {
			fmt.Println(*r, err)
		}
		close(c)
	})

	<-c

	cli.rpcChannel.socket.Close(nil)
	s.stop()
}

func TestAuth(t *testing.T) {
	s, _ := newServer("localhost:8110")

	Register[string](s.rpcServer, "hello", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg))
	})

	s.rpcServer.SetInInterceptor(append([]func(replyer *Replyer, req *RequestMsg) bool{}, func(replyer *Replyer, req *RequestMsg) bool {
		token := string(req.UserData)
		if token == "12345678" {
			return true
		} else {
			replyer.Error(NewError(ErrOther, "auth failed"))
			return false
		}
	}))

	s.start()

	counter := 0

	cli, _ := newClient("localhost:8110")

	cli.rpcClient.SetOutInterceptor([]func(*RequestMsg, interface{}){
		func(req *RequestMsg, arg interface{}) {
			counter++
			if counter > 1 {
				req.UserData = []byte("87654321")
			} else {
				req.UserData = []byte("12345678")
			}
		},
	})

	var resp string
	err := cli.rpcClient.Call(context.TODO(), cli.rpcChannel, "hello", "sniperHW", &resp)
	assert.Nil(t, err)
	assert.Equal(t, resp, "hello world:sniperHW")

	err = cli.rpcClient.Call(context.TODO(), cli.rpcChannel, "hello", "sniperHW", &resp)
	assert.Equal(t, err.Error(), "auth failed")
	s.stop()
}

func TestTrace(t *testing.T) {
	s, _ := newServer("localhost:8110")
	Register[string](s.rpcServer, "hello", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg))
	})

	s.rpcServer.SetInInterceptor(append([]func(replyer *Replyer, req *RequestMsg) bool{}, func(replyer *Replyer, req *RequestMsg) bool {
		traceID := binary.BigEndian.Uint32(req.UserData)
		logger.Debugf("[trace] server recv %s traceID:%d", req.Method, traceID)
		//设置钩子函数,当Replyer发送应答时调用
		replyer.AppendOutInterceptor(func(req *RequestMsg, ret interface{}, err error) {
			logger.Debugf("[trace] server reply %s traceID:%d", req.Method, traceID)
		})
		return true
	}))

	s.start()

	cli, _ := newClient("localhost:8110")

	cli.rpcClient.SetOutInterceptor([]func(*RequestMsg, interface{}){
		func(req *RequestMsg, arg interface{}) {
			//设置traceID
			req.UserData = make([]byte, 4)
			binary.BigEndian.PutUint32(req.UserData, 1000001)
		},
	})

	cli.rpcClient.SetInInterceptor([]func(*RequestMsg, interface{}, error){
		func(req *RequestMsg, ret interface{}, err error) {
			traceID := binary.BigEndian.Uint32(req.UserData)
			logger.Debugf("%s on response traceID:%d", req.Method, traceID)
		},
	})

	logger.Debugf("begin rpc call")

	var resp string
	err := cli.rpcClient.Call(context.TODO(), cli.rpcChannel, "hello", "sniperHW", &resp)
	assert.Nil(t, err)
	assert.Equal(t, resp, "hello world:sniperHW")

	cli.rpcChannel.socket.Close(nil)
	s.stop()
}

func TestRPC(t *testing.T) {
	s, _ := newServer("localhost:8110")

	s.rpcServer.SetInInterceptor(append([]func(replyer *Replyer, req *RequestMsg) bool{}, func(replyer *Replyer, req *RequestMsg) bool {
		beg := time.Now()
		//设置钩子函数,当Replyer发送应答时调用
		replyer.AppendOutInterceptor(func(req *RequestMsg, ret interface{}, err error) {
			if err == nil {
				logger.Debugf("serve %s(\"%v\") resp:%v use:%v", req.Method, *req.GetArg().(*string), ret, time.Now().Sub(beg))
			} else {
				logger.Debugf("serve %s(\"%v\") with error:%v", req.Method, *req.GetArg().(*string), err)
			}
		})
		return true
	}))

	Register[string](s.rpcServer, "hello", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg))
	})

	Register[string](s.rpcServer, "timeout", func(_ context.Context, replyer *Replyer, arg *string) {
		go func() {
			time.Sleep(time.Second * 5)
			logger.Debugf("timeout reply")
			replyer.Reply(fmt.Sprintf("timeout hello world:%s", *arg))
		}()
	})

	s.start()

	cli, _ := newClient("localhost:8110")

	msgChan := make(chan struct{})
	var pendingCall sync.Map

	cli.rpcClient.SetOutInterceptor([]func(*RequestMsg, interface{}){
		func(req *RequestMsg, arg interface{}) {
			if !req.Oneway {
				pendingCall.Store(req.Seq, time.Now())
			}
		},
	})

	cli.rpcClient.SetInInterceptor([]func(*RequestMsg, interface{}, error){
		func(req *RequestMsg, ret interface{}, err error) {
			v, ok := pendingCall.LoadAndDelete(req.Seq)
			if ok {
				logger.Debugf("call %v(seq:%d,arg:%v,ret:%v,err:%v) use:%v", req.Method, req.Seq, req.GetArg(), ret, err, time.Now().Sub(v.(time.Time)))
			}
		},
	})

	cli.packetHandler = func(context context.Context, packet interface{}) {
		close(msgChan)
	}
	cli.rpcChannel.socket.Send("msg")
	<-msgChan

	logger.Debugf("begin rpc call")

	var resp string
	err := cli.rpcClient.Call(context.TODO(), cli.rpcChannel, "hello", "sniperHW", &resp)
	assert.Nil(t, err)
	assert.Equal(t, resp, "hello world:sniperHW")

	err = cli.rpcClient.AsyncCall(cli.rpcChannel, "hello", "sniperHW", &resp, time.Now().Add(time.Second), func(v interface{}, err error) {
		assert.Equal(t, *v.(*string), "hello world:sniperHW")
		assert.Nil(t, err)
	})
	assert.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*4)
	err = cli.rpcClient.Call(ctx, cli.rpcChannel, "timeout", "sniperHW", &resp)
	cancel()
	assert.Equal(t, err.(*Error).Is(ErrTimeout), true)

	time.Sleep(time.Second)

	c := make(chan struct{})

	Register[string](s.rpcServer, "syncOneway", func(_ context.Context, replyer *Replyer, arg *string) {
		logger.Debugf("syncOneway %s", *arg)
		replyer.Reply(*arg)
		close(c)
	})

	err = cli.rpcClient.Call(context.TODO(), cli.rpcChannel, "syncOneway", "sniperHW", nil)
	assert.Nil(t, err)
	<-c

	UnRegister(s.rpcServer, "hello")

	err = cli.rpcClient.Call(context.TODO(), cli.rpcChannel, "hello", "sniperHW", &resp)
	assert.Equal(t, err.(*Error).Is(ErrInvaildMethod), true)

	Register[string](s.rpcServer, "panic", func(_ context.Context, replyer *Replyer, arg *string) {
		//cause panic
		panic("panic")
	})

	err = cli.rpcClient.Call(context.TODO(), cli.rpcChannel, "panic", "sniperHW", &resp)
	assert.Equal(t, err.Error(), "method panic")

	s.rpcServer.Stop()

	err = cli.rpcClient.Call(context.TODO(), cli.rpcChannel, "hello", "sniperHW", &resp)
	assert.Equal(t, err.(*Error).Is(ErrServiceUnavaliable), true)

	cli.rpcChannel.socket.Close(nil)
	s.stop()
}

func TestEnDeCode(t *testing.T) {
	{
		req := &RequestMsg{
			Seq:    10012,
			Method: "test",
			Arg:    []byte("hello"),
			Oneway: true,
		}

		b := EncodeRequest(req)

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

		b := EncodeRequest(req)

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

		b := EncodeResponse(resp)

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

		b := EncodeResponse(resp)

		resp = nil

		resp, _ = DecodeResponse(b)

		assert.Equal(t, resp.Seq, uint64(10012))

		assert.Equal(t, resp.Err.Error(), "error")

		assert.Equal(t, resp.Ret, []byte("hello"))
	}

}

type channelInterestDisconnect struct {
	socket      *netgo.AsynSocket
	pendingCall sync.Map
	counter     int
}

var errBusy error = errors.New("busy")

func (c *channelInterestDisconnect) RequestWithContext(ctx context.Context, request *RequestMsg) error {
	c.counter++
	if c.counter < 3 {
		return errBusy
	}
	return c.socket.SendWithContext(ctx, request)
}

func (c *channelInterestDisconnect) Request(request *RequestMsg) error {
	c.counter++
	if c.counter < 3 {
		return errBusy
	}
	return c.socket.Send(request, time.Time{})
}

func (c *channelInterestDisconnect) Reply(response *ResponseMsg) error {
	return c.socket.Send(response)
}

func (c *channelInterestDisconnect) Name() string {
	return fmt.Sprintf("%v <-> %v", c.socket.LocalAddr(), c.socket.RemoteAddr())
}

func (c *channelInterestDisconnect) IsRetryAbleError(e error) bool {
	return e == errBusy
}

func (c *channelInterestDisconnect) OnDisconnect() {
	c.pendingCall.Range(func(key any, value any) bool {
		c.pendingCall.Delete(key)
		value.(PendingCall).OnDisconnect()
		return true
	})
}

func (c *channelInterestDisconnect) PutPending(seq uint64, ctx PendingCall) {
	c.pendingCall.Store(seq, ctx)
}

func (c *channelInterestDisconnect) LoadAndDeletePending(seq uint64) (interface{}, bool) {
	return c.pendingCall.LoadAndDelete(seq)
}

func TestManagePendingChannel(t *testing.T) {
	s, _ := newServer("localhost:8110")

	s.rpcServer.SetInInterceptor(append([]func(replyer *Replyer, req *RequestMsg) bool{}, func(replyer *Replyer, req *RequestMsg) bool {
		beg := time.Now()
		//设置钩子函数,当Replyer发送应答时调用
		replyer.AppendOutInterceptor(func(req *RequestMsg, ret interface{}, err error) {
			if err == nil {
				logger.Debugf("serve %s(\"%v\") resp:%v use:%v", req.Method, *req.GetArg().(*string), ret, time.Now().Sub(beg))
			} else {
				logger.Debugf("serve %s(\"%v\") with error:%v", req.Method, *req.GetArg().(*string), err)
			}
		})
		return true
	}))
	Register[string](s.rpcServer, "hello", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg))
	})

	Register[string](s.rpcServer, "timeout", func(_ context.Context, replyer *Replyer, arg *string) {
		go func() {
			time.Sleep(time.Second * 5)
			logger.Debugf("timeout reply")
			replyer.Reply(fmt.Sprintf("timeout hello world:%s", *arg))
		}()
	})

	s.start()

	dialer := &net.Dialer{}
	conn, _ := dialer.Dial("tcp", "localhost:8110")
	codec := &PacketCodec{buff: make([]byte, 4096)}
	as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), codec),
		netgo.AsynSocketOption{
			Codec:    codec,
			AutoRecv: true,
		})

	msgChan := make(chan struct{})

	channel := &channelInterestDisconnect{socket: as}
	rpcClient := NewClient(&JsonCodec{})
	as.SetPacketHandler(func(context context.Context, as *netgo.AsynSocket, packet interface{}) error {
		switch packet := packet.(type) {
		case string:
			close(msgChan)
		case *ResponseMsg:
			rpcClient.OnMessage(channel, packet)
		}
		return nil
	}).SetCloseCallback(func(_ *netgo.AsynSocket, _ error) {
		channel.OnDisconnect()
	}).Recv()
	as.Send("msg")
	<-msgChan

	logger.Debugf("begin rpc call")

	var resp string
	err := rpcClient.Call(context.TODO(), channel, "hello", "sniperHW", &resp)
	assert.Nil(t, err)
	assert.Equal(t, resp, "hello world:sniperHW")

	err = rpcClient.AsyncCall(channel, "hello", "sniperHW", &resp, time.Now().Add(time.Second), func(v interface{}, err error) {
		assert.Equal(t, *v.(*string), "hello world:sniperHW")
		assert.Nil(t, err)
	})
	assert.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*4)
	err = rpcClient.Call(ctx, channel, "timeout", "sniperHW", &resp)
	cancel()
	assert.Equal(t, err.(*Error).Is(ErrTimeout), true)

	go func() {
		time.Sleep(time.Second)
		as.Close(nil)
	}()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*4)
	err = rpcClient.Call(ctx, channel, "timeout", "sniperHW", &resp)
	cancel()
	assert.Equal(t, err.(*Error).Is(ErrDisconnet), true)

	s.stop()
}
