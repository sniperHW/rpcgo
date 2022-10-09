package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
	"time"
)

const (
	packet_rpc_request  = byte(1)
	packet_rpc_response = byte(2)
)

type PacketReceiver struct {
	gate bool
	r    int
	w    int
	buff []byte
}

func (r *PacketReceiver) read(readable netgo.ReadAble, deadline time.Time) (int, error) {
	if err := readable.SetReadDeadline(deadline); err != nil {
		return 0, err
	} else {
		return readable.Read(r.buff[r.w:])
	}
}

func (r *PacketReceiver) Recv(readable netgo.ReadAble, deadline time.Time) (pkt []byte, err error) {
	const lenHead int = 4
	for {
		rr := r.r
		pktLen := 0
		if (r.w-rr) >= lenHead && uint32(r.w-rr-lenHead) >= binary.BigEndian.Uint32(r.buff[rr:]) {
			pktLen = int(binary.BigEndian.Uint32(r.buff[rr:]))
			rr += lenHead
		}

		if pktLen > 0 {
			if pktLen > (len(r.buff) - lenHead) {
				err = errors.New("pkt too large")
				return
			}
			if (r.w - rr) >= pktLen {
				if !r.gate {
					//非gate只返回payload
					pkt = r.buff[rr : rr+pktLen]
				} else {
					//gate返回len+payload
					pkt = r.buff[r.r : r.r+pktLen+lenHead]
				}
				rr += pktLen
				r.r = rr
				if r.r == r.w {
					r.r = 0
					r.w = 0
				}
				return
			}
		}

		if r.r > 0 {
			//移动到头部
			copy(r.buff, r.buff[r.r:r.w])
			r.w = r.w - r.r
			r.r = 0
		}

		var n int
		n, err = r.read(readable, deadline)
		if n > 0 {
			r.w += n
		}
		if nil != err {
			return
		}
	}
}

type PacketDecoder struct {
}

func (d *PacketDecoder) Decode(b []byte) (interface{}, error) {
	switch b[0] {
	case packet_rpc_request:
		request := &rpcgo.RPCRequestMessage{}
		json.Unmarshal(b[1:], request)
		return request, nil
	case packet_rpc_response:
		response := &rpcgo.RPCResponseMessage{}
		json.Unmarshal(b[1:], response)
		return response, nil
	default:
		return nil, errors.New("invaild packet")
	}
}

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

type PacketPacker struct {
}

func (e *PacketPacker) Pack(b []byte, o interface{}) []byte {
	offset := len(b)
	var jsonByte []byte
	switch o.(type) {
	case *rpcgo.RPCRequestMessage:
		request := o.(*rpcgo.RPCRequestMessage)
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_rpc_request)
		jsonByte, _ = json.Marshal(request)
	case *rpcgo.RPCResponseMessage:
		response := o.(*rpcgo.RPCResponseMessage)
		b = AppendUint32(b, 0)
		b = AppendByte(b, packet_rpc_response)
		jsonByte, _ = json.Marshal(response)
	default:
		return b
	}
	b = AppendBytes(b, jsonByte)
	binary.BigEndian.PutUint32(b[offset:], uint32(len(b)-offset-4))
	return b
}

type JsonCodec struct {
}

func (c *JsonCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (c *JsonCodec) Decode(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}
