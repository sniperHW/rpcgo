package main

import (
	"io"
	"log"
	"net"

	"github.com/sniperHW/netgo"
)

/*func startGateway(gateService string, rpcService string) {
	_, serve, _ := netgo.ListenTCP("tcp", gateService, func(conn *net.TCPConn) {
		go func() {
			dialer := &net.Dialer{}
			var rpcSocket netgo.Socket
			for {
				conn, err := dialer.Dial("tcp", rpcService)
				if err != nil {
					time.Sleep(time.Second)
				} else {
					rpcSocket = netgo.NewTcpSocket(conn.(*net.TCPConn), &PacketCodec{gate: true, buff: make([]byte, 65536)})
					break
				}
			}
			cli := netgo.NewTcpSocket(conn, &PacketCodec{gate: true, buff: make([]byte, 65536)})
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
*/

func startGateway(gateService string, rpcService string) {
	_, serve, _ := netgo.ListenTCP("tcp", gateService, func(conn *net.TCPConn) {
		go func() {
			dialer := &net.Dialer{}
			serverConn, err := dialer.Dial("tcp", rpcService)
			if err != nil {
				return
			}
			go io.Copy(conn, serverConn)
			io.Copy(serverConn, conn)
		}()
	})
	log.Println("gate started")
	serve()
}
