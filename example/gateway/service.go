package main

import (
	//"context"
	//"encoding/binary"
	//"errors"
	"fmt"
	"github.com/sniperHW/network"
	"github.com/sniperHW/rpcgo"
	//"github.com/stretchr/testify/assert"
	//"go.uber.org/zap"
	"net"
	//"testing"
	//"time"
	//"unsafe"
	"log"
)

func startService(service string) {
	rpcServer := rpcgo.NewServer()

	rpcServer.RegisterMethod("hello", func(req rpcgo.RPCRequest) {
		req.Reply(fmt.Sprintf("hello world:%s", req.Argumment().(string)), nil)
	})

	_, serve, _ := network.ListenTCP("tcp", service, func(conn *net.TCPConn) {
		as := network.NewAsynSocket(network.NewTcpSocket(conn, &PacketReceiver{buff: make([]byte, 4096)}),
			network.AsynSocketOption{
				Decoder:  &PacketDecoder{},
				Packer:   &PacketPacker{},
				AutoRecv: true,
			})
		as.SetPacketHandler(func(as *network.AsynSocket, packet interface{}) {
			rpcServer.OnRPCMessage(&rcpChannel{socket: as}, packet.(*rpcgo.RPCRequestMessage))
		}).Recv()
	})

	log.Println("rpc service started")

	serve()
}
