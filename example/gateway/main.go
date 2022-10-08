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

	as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), &PacketReceiver{buff: make([]byte, 4096)}),
		netgo.AsynSocketOption{
			Decoder:  &PacketDecoder{},
			Packer:   &PacketPacker{},
			AutoRecv: true,
		})

	channel := &rcpChannel{socket: as}
	rpcClient := rpcgo.NewClient()
	as.SetPacketHandler(func(as *netgo.AsynSocket, resp interface{}) {
		rpcClient.OnRPCMessage(resp.(*rpcgo.RPCResponseMessage))
	}).Recv()

	for i := 0; i < 100; i++ {
		resp, err := rpcClient.Call(context.TODO(), channel, &rpcgo.RPCRequestMessage{
			Method: "hello",
			Arg:    "sniperHW"})
		log.Println(resp, err)
	}

}
