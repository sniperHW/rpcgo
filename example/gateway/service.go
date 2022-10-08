package main

import (
	"fmt"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
	"log"
	"net"
)

func startService(service string) {
	rpcServer := rpcgo.NewServer()

	rpcServer.RegisterMethod("hello", func(req rpcgo.RPCRequest) {
		req.Reply(fmt.Sprintf("hello world:%s", req.Argumment().(string)), nil)
	})

	_, serve, _ := netgo.ListenTCP("tcp", service, func(conn *net.TCPConn) {
		as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn, &PacketReceiver{buff: make([]byte, 4096)}),
			netgo.AsynSocketOption{
				Decoder:  &PacketDecoder{},
				Packer:   &PacketPacker{},
				AutoRecv: true,
			})
		as.SetPacketHandler(func(as *netgo.AsynSocket, packet interface{}) {
			rpcServer.OnRPCMessage(&rcpChannel{socket: as}, packet.(*rpcgo.RPCRequestMessage))
		}).Recv()
	})

	log.Println("rpc service started")

	serve()
}
