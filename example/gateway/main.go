package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
	"go.uber.org/zap"
)

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	msg := fmt.Sprintf("HeapAlloc = %v MiB  TotalAlloc = %v MiB  Sys = %v MiB Alloc = %v MiB tNumGC = %v  PauseTotal %vms HeapObjects = %v  Goroutine = %v",
		bToMb(m.HeapAlloc), bToMb(m.TotalAlloc), bToMb(m.Sys), bToMb(m.Alloc), m.NumGC, m.PauseTotalNs/100000, m.HeapObjects, runtime.NumGoroutine())
	println(msg)
}

func main() {
	l := zap.NewExample().Sugar()
	rpcgo.InitLogger(l)

	go startService("localhost:8110")

	go startGateway("localhost:9110", "localhost:8110")

	dialer := &net.Dialer{}

	pprof := flag.String("pprof", "localhost:8999", "pprof")

	flag.Parse()

	go func() {
		http.ListenAndServe(*pprof, nil)
	}()

	var conn net.Conn
	var err error
	for {
		if conn, err = dialer.Dial("tcp", "localhost:9110"); err == nil {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	codec := &PacketCodec{buff: make([]byte, 65536)}

	as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), codec),
		netgo.AsynSocketOption{
			Codec:        codec,
			AutoRecv:     true,
			SendChanSize: 256,
		})

	rpcClient := rpcgo.NewClient(&JsonCodec{})
	as.SetPacketHandler(func(context context.Context, as *netgo.AsynSocket, resp interface{}) error {
		rpcClient.OnMessage(nil, resp.(*rpcgo.ResponseMsg))
		return nil
	}).Recv()

	channel := &rcpChannel{socket: as}

	routineCount := 100
	callCount := 1000

	callTimeArray := make([]time.Duration, routineCount*callCount)
	calAverageTime := func() time.Duration {
		total := time.Duration(0)
		for _, time := range callTimeArray {
			total += time
		}
		return total / time.Duration(len(callTimeArray))
	}
	calPercentileTime := func(percentile int) time.Duration {
		sort.Slice(callTimeArray, func(i, j int) bool {
			return callTimeArray[i] < callTimeArray[j]
		})
		return callTimeArray[len(callTimeArray)*percentile/100]
	}

	for {
		var waitGroup sync.WaitGroup
		waitGroup.Add(routineCount)
		beg := time.Now()
		for i := 0; i < routineCount; i++ {
			go func(index int) {
				defer waitGroup.Done()
				req := strings.Repeat("sniperHW", 1)
				for j := 0; j < callCount; j++ {
					var resp string
					beg := time.Now()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					err := rpcClient.Call(ctx, channel, "hello", req, &resp)
					used := time.Since(beg)
					callTimeArray[index*callCount+j] = used
					if err != nil {
						log.Println(err)
					}
				}
			}(i)
		}
		waitGroup.Wait()
		used := time.Since(beg)
		PrintMemUsage()
		avaTime := calAverageTime()
		qps := float64(routineCount*callCount) / used.Seconds()
		log.Println("used:", used, "average:", "qps:", qps, "avaTime:", avaTime, "90th:", calPercentileTime(90), "95th:", calPercentileTime(95), "99th:", calPercentileTime(99))
	}
}
