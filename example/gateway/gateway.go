package main

import (
	"github.com/sniperHW/netgo"
	"log"
	"net"
	"time"
)

func startGateway(gateService string, rpcService string) {
	_, serve, _ := netgo.ListenTCP("tcp", gateService, func(conn *net.TCPConn) {
		go func() {
			dialer := &net.Dialer{}
			var rpcSocket netgo.Socket
			for {
				conn, err := dialer.Dial("tcp", rpcService)
				if err != nil {
					time.Sleep(time.Second)
				} else {
					rpcSocket = netgo.NewTcpSocket(conn.(*net.TCPConn), &PacketReceiver{gate: true, buff: make([]byte, 4096)})
					break
				}
			}
			cli := netgo.NewTcpSocket(conn, &PacketReceiver{gate: true, buff: make([]byte, 4096)})
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
