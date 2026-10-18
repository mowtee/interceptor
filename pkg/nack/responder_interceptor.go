// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package nack

import (
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/internal/cc"
	"github.com/pion/interceptor/internal/rtpbuffer"
	"github.com/pion/interceptor/internal/validation"
	"github.com/pion/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

// ResponderInterceptorFactory is a interceptor.Factory for a ResponderInterceptor.
type ResponderInterceptorFactory struct {
	opts []ResponderOption
}

// NewInterceptor constructs a new ResponderInterceptor.
func (r *ResponderInterceptorFactory) NewInterceptor(_ string) (interceptor.Interceptor, error) {
	responderInterceptor := &ResponderInterceptor{
		streamsFilter: streamSupportNack,
		size:          1024,
		streams:       map[uint32]*localStream{},
	}

	for _, opt := range r.opts {
		if err := opt(responderInterceptor); err != nil {
			return nil, err
		}
	}

	if responderInterceptor.loggerFactory == nil {
		responderInterceptor.loggerFactory = logging.NewDefaultLoggerFactory()
	}
	if responderInterceptor.log == nil {
		responderInterceptor.log = responderInterceptor.loggerFactory.NewLogger("nack_responder")
	}
	if responderInterceptor.packetFactory == nil {
		responderInterceptor.packetFactory = rtpbuffer.NewPacketFactoryCopy()
	}

	if err := validation.IsPowerOfTwo(responderInterceptor.size); err != nil {
		return nil, err
	}
	if responderInterceptor.onNew != nil {
		responderInterceptor.onNew(responderInterceptor)
	}

	return responderInterceptor, nil
}

// ResponderInterceptor responds to nack feedback messages.
type ResponderInterceptor struct {
	interceptor.NoOp
	streamsFilter func(info *interceptor.StreamInfo) bool
	size          uint16
	log           logging.LeveledLogger
	loggerFactory logging.LoggerFactory
	packetFactory rtpbuffer.PacketFactory

	streams   map[uint32]*localStream
	streamsMu sync.Mutex

	maxResends     int
	resendInterval func() time.Duration
	onNew          func(*ResponderInterceptor)
}

type localStream struct {
	rtpBuffer      *rtpbuffer.RTPBuffer
	rtpBufferMutex sync.RWMutex
	rtpWriter      interceptor.RTPWriter

	// rtxSSRC is the stream's retransmission SSRC (0 without RTX) and
	// rtxSequencer numbers the packets sent on it: one number per
	// resend, assigned when the resend is written (RFC 4588), so a packet
	// resent twice goes out as two RTX packets.
	rtxSSRC      uint32
	rtxPT        uint8
	rtxSequencer rtp.Sequencer
	// resends tracks each buffered packet's resends (guarded by
	// rtpBufferMutex); an entry is dropped when a new packet takes its
	// sequence number.
	resends map[uint16]resendState
}

type resendState struct {
	count int
	last  time.Time
}

// NewResponderInterceptor returns a new ResponderInterceptorFactor.
func NewResponderInterceptor(opts ...ResponderOption) (*ResponderInterceptorFactory, error) {
	return &ResponderInterceptorFactory{opts}, nil
}

// BindRTCPReader lets you modify any incoming RTCP packets. It is called once per sender/receiver, however this might
// change in the future. The returned method will be called once per packet batch.
func (n *ResponderInterceptor) BindRTCPReader(reader interceptor.RTCPReader) interceptor.RTCPReader {
	return interceptor.RTCPReaderFunc(func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
		i, attr, err := reader.Read(b, a)
		if err != nil {
			return 0, nil, err
		}

		if attr == nil {
			attr = make(interceptor.Attributes)
		}
		pkts, err := attr.GetRTCPPackets(b[:i])
		if err != nil {
			return 0, nil, err
		}
		for _, rtcpPacket := range pkts {
			nack, ok := rtcpPacket.(*rtcp.TransportLayerNack)
			if !ok {
				continue
			}

			go n.resendPackets(nack)
		}

		return i, attr, err
	})
}

// BindLocalStream lets you modify any outgoing RTP packets. It is called once for per LocalStream.
// The returned method will be called once per rtp packet.
func (n *ResponderInterceptor) BindLocalStream(
	info *interceptor.StreamInfo, writer interceptor.RTPWriter,
) interceptor.RTPWriter {
	if !n.streamsFilter(info) {
		return writer
	}

	// error is already checked in NewGeneratorInterceptor
	rtpBuffer, _ := rtpbuffer.NewRTPBuffer(n.size)
	stream := &localStream{
		rtpBuffer: rtpBuffer,
		rtpWriter: writer,
		resends:   map[uint16]resendState{},
	}
	if info.SSRCRetransmission != 0 && info.PayloadTypeRetransmission != 0 {
		stream.rtxSSRC = info.SSRCRetransmission
		stream.rtxPT = info.PayloadTypeRetransmission
		stream.rtxSequencer = rtp.NewRandomSequencer()
	}
	n.streamsMu.Lock()
	n.streams[info.SSRC] = stream
	n.streamsMu.Unlock()

	return interceptor.RTPWriterFunc(
		func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
			// If this packet doesn't belong to the main SSRC, do not add it to rtpBuffer
			if header.SSRC != info.SSRC {
				return writer.Write(header, payload, attributes)
			}

			pkt, err := n.packetFactory.NewPacket(header, payload, info.SSRCRetransmission, info.PayloadTypeRetransmission)
			if err != nil {
				return 0, err
			}

			stream.rtpBufferMutex.Lock()
			stream.rtpBuffer.Add(pkt)
			delete(stream.resends, header.SequenceNumber)
			stream.rtpBufferMutex.Unlock()

			return writer.Write(header, payload, attributes)
		},
	)
}

// UnbindLocalStream is called when the Stream is removed. It can be used to clean up any data related to that track.
func (n *ResponderInterceptor) UnbindLocalStream(info *interceptor.StreamInfo) {
	n.streamsMu.Lock()
	stream, ok := n.streams[info.SSRC]
	delete(n.streams, info.SSRC)
	n.streamsMu.Unlock()

	if ok {
		stream.rtpBufferMutex.Lock()
		stream.rtpBuffer.Clear()
		stream.rtpBufferMutex.Unlock()
	}
}

// Close releases all resources held by the ResponderInterceptor.
func (n *ResponderInterceptor) Close() error {
	n.streamsMu.Lock()
	streams := n.streams
	n.streams = map[uint32]*localStream{}
	n.streamsMu.Unlock()

	for _, stream := range streams {
		stream.rtpBufferMutex.Lock()
		stream.rtpBuffer.Clear()
		stream.rtpBufferMutex.Unlock()
	}

	return nil
}

func (n *ResponderInterceptor) resendPackets(nack *rtcp.TransportLayerNack) {
	n.streamsMu.Lock()
	stream, ok := n.streams[nack.MediaSSRC]
	n.streamsMu.Unlock()
	if !ok {
		return
	}

	var interval time.Duration
	if n.resendInterval != nil {
		interval = n.resendInterval()
	}
	for i := range nack.Nacks {
		nack.Nacks[i].Range(func(seq uint16) bool {
			// get the packet under the buffer lock, and decide and number
			// the resend there too (NACKs are handled concurrently)
			stream.rtpBufferMutex.Lock()
			p := stream.rtpBuffer.Get(seq)
			if p == nil {
				stream.rtpBufferMutex.Unlock()

				return true
			}
			now := time.Now()
			st := stream.resends[seq]
			if (n.maxResends > 0 && st.count >= n.maxResends) ||
				(interval > 0 && st.count > 0 && now.Sub(st.last) < interval) {
				stream.rtpBufferMutex.Unlock()
				p.Release()

				return true
			}
			stream.resends[seq] = resendState{count: st.count + 1, last: now}
			header := p.Header()
			if stream.rtxSSRC != 0 && header.SSRC == stream.rtxSSRC {
				clone := header.Clone()
				clone.SequenceNumber = stream.rtxSequencer.NextSequenceNumber()
				header = &clone
			}
			stream.rtpBufferMutex.Unlock()

			// send without holding rtpBufferMutex
			if _, err := stream.rtpWriter.Write(header, p.Payload(), interceptor.Attributes{}); err != nil {
				n.log.Warnf("failed resending nacked packet: %+v", err)
			}
			p.Release()

			return true
		})
	}
}

// probePaddingSize is a probe packet's padding: the most one RTP padding
// count can say.
const probePaddingSize = 255

// Probe sends padding-only RTX packets for the stream whose media SSRC is
// ssrc until at least bytes bytes are on their way, and returns the bytes
// sent (RTP header + padding). They are bandwidth probes — libwebrtc and
// LiveKit probe the same way — that a receiver counts for transport-cc
// and discards (an RTX packet without a payload repairs nothing). Copies
// of media were tried first and overtook their own originals still queued
// in the pacer: the receiver filed the copy, dropped the original as a
// duplicate before transport-cc and reported it lost. The packets carry
// the probe attribute, so the pacer sends them outside its budget. A
// stream without RTX is not probed.
func (n *ResponderInterceptor) Probe(ssrc uint32, bytes int) int {
	n.streamsMu.Lock()
	stream, ok := n.streams[ssrc]
	n.streamsMu.Unlock()
	if !ok || stream.rtxSSRC == 0 || bytes <= 0 {
		return 0
	}
	stream.rtpBufferMutex.Lock()
	var timestamp uint32
	highest, started := stream.rtpBuffer.Highest()
	if started {
		if p := stream.rtpBuffer.Get(highest); p != nil {
			timestamp = p.Header().Timestamp
			p.Release()
		}
	}
	stream.rtpBufferMutex.Unlock()
	if !started {
		return 0
	}
	sent := 0
	for sent < bytes {
		stream.rtpBufferMutex.Lock()
		seq := stream.rtxSequencer.NextSequenceNumber()
		stream.rtpBufferMutex.Unlock()
		header := &rtp.Header{
			Version: 2, Padding: true, PaddingSize: probePaddingSize,
			PayloadType: stream.rtxPT, SequenceNumber: seq, Timestamp: timestamp, SSRC: stream.rtxSSRC,
		}
		attrs := interceptor.Attributes{}
		attrs.Set(cc.ProbePacketAttributesKey, true)
		n, err := stream.rtpWriter.Write(header, nil, attrs)
		if err != nil {
			break
		}
		sent += max(n, header.MarshalSize()+probePaddingSize)
	}

	return sent
}
