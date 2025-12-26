package main

import (
	"io"
	"log"
	"net"
	"sync"
	"time"

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
			// 连接建立时的日志
			remoteAddr := conn.RemoteAddr().String()
			log.Printf("接收到来自 %s 的连接", remoteAddr)

			// 4. 连接上游目标服务器
			// 设置超时时间防止连接卡死
			targetConn, err := net.DialTimeout("tcp", rpcService, 5*time.Second)
			if err != nil {
				log.Printf("无法连接上游服务器 %s: %v", rpcService, err)
				conn.Close()
				return
			}

			// 确保连接最终被关闭
			// 使用 Once 确保只关闭一次，避免重复关闭报错（虽然 net.Conn 允许重复关闭）
			var once sync.Once
			closeConns := func() {
				conn.Close()
				targetConn.Close()
				log.Printf("连接断开: %s", remoteAddr)
			}

			// 5. 核心：使用 io.Copy 进行双向数据转发
			// 因为 io.Copy 是阻塞的，所以需要在一个 Goroutine 中处理一个方向，主程处理另一个方向

			// 方向 1: 上游 -> 客户端
			go func() {
				defer once.Do(closeConns)
				// io.Copy 会一直阻塞，直到 EOF (连接关闭) 或发生错误
				_, err := io.Copy(conn, targetConn)
				if err != nil {
					// 只有非“连接关闭”的错误才需要打印，防止日志噪音
					if err != io.EOF {
						// log.Printf("上游 -> 客户端 复制错误: %v", err)
					}
				}
			}()

			// 方向 2: 客户端 -> 上游
			defer once.Do(closeConns)
			_, err = io.Copy(targetConn, conn)
			if err != nil {
				if err != io.EOF {
					// log.Printf("客户端 -> 上游 复制错误: %v", err)
				}
			}
		}()
	})
	log.Println("gate started")
	serve()
}
