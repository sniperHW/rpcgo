package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
)

func startService(service string) {
	rpcServer := rpcgo.NewServer(&JsonCodec{})

	rpcgo.Register(rpcServer, "hello", func(context context.Context, replyer *rpcgo.Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg))
	})

	_, serve, _ := netgo.ListenTCP("tcp", service, func(conn *net.TCPConn) {
		codec := &PacketCodec{buff: make([]byte, 65536)}
		as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn, codec),
			netgo.AsynSocketOption{
				Codec:    codec,
				AutoRecv: true,
			})
		as.SetPacketHandler(func(context context.Context, as *netgo.AsynSocket, packet interface{}) error {
			rpcServer.OnMessage(context, &rcpChannel{socket: as}, packet.(*rpcgo.RequestMsg))
			return nil
		}).Recv()
	})

	log.Println("rpc service started")

	serve()
}
