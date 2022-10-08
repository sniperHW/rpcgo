package main

import (
	"github.com/sniperHW/network"
	"log"
	"net"
	"time"
)

func startGateway(gateService string, rpcService string) {
	_, serve, _ := network.ListenTCP("tcp", gateService, func(conn *net.TCPConn) {
		go func() {
			dialer := &net.Dialer{}
			var rpcSocket network.Socket
			for {
				conn, err := dialer.Dial("tcp", rpcService)
				if err != nil {
					time.Sleep(time.Second)
				} else {
					rpcSocket = network.NewTcpSocket(conn.(*net.TCPConn), &PacketReceiver{gate: true, buff: make([]byte, 4096)})
					break
				}
			}
			cli := network.NewTcpSocket(conn, &PacketReceiver{gate: true, buff: make([]byte, 4096)})
			for {
				request, err := cli.Recv()
				if nil != err {
					break
				}
				rpcSocket.Send(request)
				resp, err := rpcSocket.Recv()
				if nil != err {
					break
				}
				cli.Send(resp)
			}

			rpcSocket.Close()
			cli.Close()
		}()
	})
	log.Println("gate started")
	serve()
}
