package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
)

const (
	packet_rpc_request  = byte(1)
	packet_rpc_response = byte(2)
)

func AppendByte(bs []byte, v byte) []byte {
	return append(bs, v)
}

func AppendBytes(bs []byte, bytes []byte) []byte {
	return append(bs, bytes...)
}

func AppendUint32(bs []byte, u32 uint32) []byte {
	bu := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(bu, u32)
	return AppendBytes(bs, bu)
}

type PacketCodec struct {
	gate bool
	r    int
	w    int
	buff []byte
}

func (codec *PacketCodec) read(readable netgo.ReadAble, deadline time.Time) (int, error) {
	if err := readable.SetReadDeadline(deadline); err != nil {
		return 0, err
	} else {
		return readable.Read(codec.buff[codec.w:])
	}
}

func (codec *PacketCodec) Recv(readable netgo.ReadAble, deadline time.Time) (pkt []byte, err error) {
	const lenHead int = 4
	for {
		rr := codec.r
		pktLen := 0
		if (codec.w - rr) >= lenHead { //&& uint32(codec.w-rr-lenHead) >= binary.BigEndian.Uint32(codec.buff[rr:]) {
			pktLen = int(binary.BigEndian.Uint32(codec.buff[rr:]))
			rr += lenHead
		}

		if pktLen > 0 {
			if pktLen > (len(codec.buff) - lenHead) {
				err = errors.New("pkt too large")
				return
			}
			if (codec.w - rr) >= pktLen {
				if !codec.gate {
					//非gate只返回payload
					pkt = codec.buff[rr : rr+pktLen]
				} else {
					//gate返回len+payload
					pkt = codec.buff[codec.r : codec.r+pktLen+lenHead]
				}
				rr += pktLen
				codec.r = rr
				if codec.r == codec.w {
					codec.r = 0
					codec.w = 0
				}
				return
			}
		}

		if codec.r > 0 {
			//移动到头部
			copy(codec.buff, codec.buff[codec.r:codec.w])
			codec.w = codec.w - codec.r
			codec.r = 0
		}

		var n int
		n, err = codec.read(readable, deadline)
		if n > 0 {
			codec.w += n
		}
		if nil != err {
			return
		}
	}
}

func (codec *PacketCodec) Decode(b []byte) (interface{}, error) {
	switch b[0] {
	case packet_rpc_request:
		request := &rpcgo.RequestMsg{}
		json.Unmarshal(b[1:], request)
		return request, nil
	case packet_rpc_response:
		response := &rpcgo.ResponseMsg{}
		json.Unmarshal(b[1:], response)
		return response, nil
	default:
		return nil, errors.New("invaild packet")
	}
}

func (codec *PacketCodec) Encode(buffs net.Buffers, o interface{}) (net.Buffers, int) {
	var headBytes []byte
	var dataBytes []byte
	switch o := o.(type) {
	case *rpcgo.RequestMsg:
		headBytes = AppendUint32(headBytes, 0)
		headBytes = AppendByte(headBytes, packet_rpc_request)
		dataBytes, _ = json.Marshal(o)
	case *rpcgo.ResponseMsg:
		headBytes = AppendUint32(headBytes, 0)
		headBytes = AppendByte(headBytes, packet_rpc_response)
		dataBytes, _ = json.Marshal(o)
	default:
		return buffs, 0
	}

	binary.BigEndian.PutUint32(headBytes, uint32(len(dataBytes)+1))
	return append(buffs, headBytes, dataBytes), len(dataBytes) + 5
}

type JsonCodec struct {
}

func (c *JsonCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (c *JsonCodec) Decode(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}
