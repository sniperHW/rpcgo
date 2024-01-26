package rpcgo

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/sniperHW/netgo"
)

func startServer() (net.Listener, error) {
	rpcServer := NewServer(&JsonCodec{})
	rpcServer.Register("hello", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg))
	})

	listener, serve, err := netgo.ListenTCP("tcp", "localhost:8110", func(conn *net.TCPConn) {
		codec := &PacketCodec{buff: make([]byte, 4096)}
		as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn, codec),
			netgo.AsynSocketOption{
				Codec:    codec,
				AutoRecv: true,
			})
		as.SetPacketHandler(func(context context.Context, as *netgo.AsynSocket, packet interface{}) error {
			switch packet := packet.(type) {
			case string:
				as.Send(packet)
			case *RequestMsg:
				rpcServer.OnMessage(context, &testChannel{socket: as}, packet)
			}
			return nil
		}).Recv()
	})

	if err == nil {
		go serve()
	}

	return listener, err
}

func newClient() (*Client, *testChannel) {
	dialer := &net.Dialer{}
	conn, _ := dialer.Dial("tcp", "localhost:8110")
	codec := &PacketCodec{buff: make([]byte, 4096)}
	as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), codec),
		netgo.AsynSocketOption{
			Codec:    codec,
			AutoRecv: true,
		})

	rpcChannel := &testChannel{socket: as}

	rpcClient := NewClient(&JsonCodec{})
	as.SetPacketHandler(func(context context.Context, as *netgo.AsynSocket, packet interface{}) error {
		switch packet := packet.(type) {
		case string:
		case *ResponseMsg:
			rpcClient.OnMessage(nil, packet)
		}
		return nil
	}).Recv()

	return rpcClient, rpcChannel
}

func Benchmark_Call(b *testing.B) {
	listener, _ := startServer()
	cli, channel := newClient()
	caller := MakeCaller[string, string](cli, "hello", channel)
	b.ResetTimer()
	b.StartTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		_, err := caller.Call(CallerOpt{}, MakeArgument("sniperHW"))
		if err != nil {
			b.Fatal(err.Error())
		}
	}
	listener.Close()
}

func Benchmark_Call_Concurrency(b *testing.B) {
	listener, _ := startServer()
	cli, channel := newClient()
	caller := MakeCaller[string, string](cli, "hello", channel)
	b.ResetTimer()
	b.StartTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := caller.Call(CallerOpt{}, MakeArgument("sniperHW"))
			if err != nil {
				b.Fatal(err.Error())
			}
		}
	})
	listener.Close()
}
