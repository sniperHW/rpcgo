package rpcgo

import (
	"fmt"
	"testing"
)

func TestBytes(t *testing.T) {
	bw := BytesWriter{
		B: make([]byte, 0, 64),
	}

	bw.WriteByte(byte(1))
	bw.WriteUint16(uint16(2))
	bw.WriteUint32(uint32(3))
	bw.WriteUint64(uint64(4))
	bw.WriteBytes([]byte("hello"))
	bw.WriteString("world")
	fmt.Println(len(bw.B))

	br := BytesReader{
		B: bw.B,
	}

	fmt.Println(br.ReadByte())
	fmt.Println(br.ReadUint16())
	fmt.Println(br.ReadUint32())
	fmt.Println(br.ReadUint64())

	b, _ := br.ReadBytes()
	fmt.Println(string(b))

	fmt.Println(br.ReadString())
	fmt.Println(len(br.B))
}

func TestRequestPacket(t *testing.T) {
	req := RequestMsg{
		Seq:      10001,
		Method:   "method1",
		Arg:      []byte("arg"),
		UserData: []byte("userdata"),
		Oneway:   true,
	}
	b := EncodeRequest(&req)

	req2, err := DecodeRequest(b)
	if err != nil {
		panic(err)
	}

	fmt.Println(req2.Seq, req2.Oneway, req2.Method, string(req2.Arg), string(req2.UserData))
}

func TestResponsePacket(t *testing.T) {
	{
		resp := ResponseMsg{
			Seq:      10002,
			UserData: []byte("userdata"),
			Ret:      []byte("ret"),
			Err:      NewError(1, "this is error"),
		}

		b := EncodeResponse(&resp)

		resp2, err := DecodeResponse(b)

		if err != nil {
			panic(err)
		}
		fmt.Println(resp2.Seq, string(resp2.Ret), string(resp2.UserData), resp2.Err.code, resp2.Err.Error())
	}

	{
		resp := ResponseMsg{
			Seq:      10003,
			UserData: []byte("userdata1"),
			Ret:      []byte("ret1"),
		}

		b := EncodeResponse(&resp)

		resp2, err := DecodeResponse(b)

		if err != nil {
			panic(err)
		}
		fmt.Println(resp2.Seq, string(resp2.Ret), string(resp2.UserData), resp2.Err)
	}
}
