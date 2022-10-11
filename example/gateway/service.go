package main

import (
	"fmt"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
	"log"
	"net"
)

func startService(service string) {
	rpcServer := rpcgo.NewServer(&JsonCodec{})

	rpcServer.RegisterMethod("hello", func(replyer *rpcgo.Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg), nil)
	})

	_, serve, _ := netgo.ListenTCP("tcp", service, func(conn *net.TCPConn) {
		codec := &PacketCodec{buff: make([]byte, 4096)}
		as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn, codec),
			netgo.AsynSocketOption{
				Codec:    codec,
				AutoRecv: true,
			})
		as.SetPacketHandler(func(as *netgo.AsynSocket, packet interface{}) error {
			rpcServer.OnRPCMessage(&rcpChannel{socket: as}, packet.(*rpcgo.RPCRequestMessage))
			return nil
		}).Recv()
	})

	log.Println("rpc service started")

	serve()
}
