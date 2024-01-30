package rpcgo

import (
	"context"
	"fmt"
	"testing"
)

func Benchmark_Call(b *testing.B) {
	s, _ := newServer("localhost:8110")
	s.rpcServer.Register("hello", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg))
	})
	s.start()
	cli, _ := newClient("localhost:8110")
	caller := MakeCaller[string, string](cli.rpcClient, "hello", cli.rpcChannel)
	b.ResetTimer()
	b.StartTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		_, err := caller.Call(CallerOpt{}, MakeArgument("sniperHW"))
		if err != nil {
			b.Fatal(err.Error())
		}
	}
	cli.rpcChannel.socket.Close(nil)
	s.stop()
}

func Benchmark_Call_Concurrency(b *testing.B) {
	s, _ := newServer("localhost:8110")
	s.rpcServer.Register("hello", func(_ context.Context, replyer *Replyer, arg *string) {
		replyer.Reply(fmt.Sprintf("hello world:%s", *arg))
	})
	s.start()
	cli, _ := newClient("localhost:8110")
	caller := MakeCaller[string, string](cli.rpcClient, "hello", cli.rpcChannel)
	b.ResetTimer()
	b.StartTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := caller.Call(CallerOpt{}, MakeArgument("sniperHW"))
			if err != nil {
				b.Fatal(err.Error())
			}
		}
	})
	cli.rpcChannel.socket.Close(nil)
	s.stop()
}
