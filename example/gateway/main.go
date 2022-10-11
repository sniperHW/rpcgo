package main

import (
	"context"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
	"go.uber.org/zap"
	"log"
	"net"
	"time"
)

func main() {
	l := zap.NewExample()
	rpcgo.InitLogger(l)

	go startService("localhost:8110")

	go startGateway("localhost:9110", "localhost:8110")

	dialer := &net.Dialer{}

	var conn net.Conn
	var err error

	for {
		if conn, err = dialer.Dial("tcp", "localhost:9110"); err == nil {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	codec := &PacketCodec{buff: make([]byte, 4096)}

	as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), codec),
		netgo.AsynSocketOption{
			Codec:    codec,
			AutoRecv: true,
		})

	channel := &rcpChannel{socket: as}
	rpcClient := rpcgo.NewClient(&JsonCodec{})
	as.SetPacketHandler(func(as *netgo.AsynSocket, resp interface{}) error {
		rpcClient.OnMessage(resp.(*rpcgo.ResponseMsg))
		return nil
	}).Recv()

	for i := 0; i < 100; i++ {
		var resp string
		err := rpcClient.Call(context.TODO(), channel, "hello", "sniperHW", &resp)
		log.Println(resp, err)
	}

}
