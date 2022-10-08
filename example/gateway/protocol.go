package main

import (
	"encoding/binary"
	"errors"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/rpcgo"
	"github.com/sniperHW/rpcgo/example/gateway/pb"
	"google.golang.org/protobuf/proto"
	"log"
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

func (r *PacketReceiver) read(readable netgo.ReadAble, deadline time.Time) (n int, err error) {
	if deadline.IsZero() {
		readable.SetReadDeadline(time.Time{})
		n, err = readable.Read(r.buff[r.w:])
	} else {
		readable.SetReadDeadline(deadline)
		n, err = readable.Read(r.buff[r.w:])
	}
	return
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
		var request pb.RPCRequest
		err := proto.Unmarshal(b[1:], &request)
		if err != nil {
			return nil, err
		} else {
			return &rpcgo.RPCRequestMessage{
				Seq:    request.Seq,
				Method: request.Method,
				Arg:    string(request.Arg),
			}, nil
		}
	case packet_rpc_response:
		var response pb.RPCResponse
		err := proto.Unmarshal(b[1:], &response)
		if err != nil {
			return nil, err
		} else {
			resp := &rpcgo.RPCResponseMessage{
				Seq: response.Seq,
			}
			if response.ErrCode != 0 {
				resp.Err = rpcgo.NewError(int(response.ErrCode), response.ErrDesc)
			} else {
				resp.Ret = string(response.Ret)
			}
			return resp, nil
		}
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
	switch o.(type) {
	case *rpcgo.RPCRequestMessage:
		request := pb.RPCRequest{
			Seq:    o.(*rpcgo.RPCRequestMessage).Seq,
			Method: o.(*rpcgo.RPCRequestMessage).Method,
			Arg:    []byte(o.(*rpcgo.RPCRequestMessage).Arg.(string)),
		}
		buff, err := proto.Marshal(&request)
		if err != nil {
			log.Println("Pack err", err)
			return b
		}

		b = AppendUint32(b, uint32(len(buff)+1))
		b = AppendByte(b, packet_rpc_request)
		return AppendBytes(b, buff)
	case *rpcgo.RPCResponseMessage:
		response := pb.RPCResponse{
			Seq: o.(*rpcgo.RPCResponseMessage).Seq,
		}

		if o.(*rpcgo.RPCResponseMessage).Err == nil {
			response.Ret = []byte(o.(*rpcgo.RPCResponseMessage).Ret.(string))
		} else {
			response.ErrCode = int32(o.(*rpcgo.RPCResponseMessage).Err.Code())
			response.ErrDesc = o.(*rpcgo.RPCResponseMessage).Err.Description()
		}

		buff, err := proto.Marshal(&response)
		if err != nil {
			log.Println("Pack err", err)
			return b
		}

		b = AppendUint32(b, uint32(len(buff)+1))
		b = AppendByte(b, packet_rpc_response)
		return AppendBytes(b, buff)
	default:
		log.Println("Pack err:invaild packet")
		return b
	}
}
