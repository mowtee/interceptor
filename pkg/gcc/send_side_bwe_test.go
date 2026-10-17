// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/twcc"
	"github.com/pion/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"time"
)

// mockTWCCResponder is a RTPWriter that writes
// TWCC feedback to a embedded SendSideBWE instantly.
type mockTWCCResponder struct {
	bwe     *SendSideBWE
	rtpChan chan []byte
}

func (m *mockTWCCResponder) Read(out []byte, _ interceptor.Attributes) (int, interceptor.Attributes, error) {
	pkt := <-m.rtpChan
	copy(out, pkt)

	return len(pkt), nil, nil
}

func (m *mockTWCCResponder) Write(pkts []rtcp.Packet, attributes interceptor.Attributes) (int, error) {
	return 0, m.bwe.WriteRTCP(pkts, attributes)
}

// mockGCCWriteStream receives RTP packets that have been paced by
// the congestion controller.
type mockGCCWriteStream struct {
	twccResponder *mockTWCCResponder
	t             *testing.T
}

func (m *mockGCCWriteStream) Write(header *rtp.Header, payload []byte, _ interceptor.Attributes) (int, error) {
	m.t.Helper()
	pkt, err := (&rtp.Packet{Header: *header, Payload: payload}).Marshal()
	assert.NoError(m.t, err)

	m.twccResponder.rtpChan <- pkt

	return 0, err
}

func TestSendSideBWE(t *testing.T) {
	buffer := make([]byte, 1500)
	rtpPayload := make([]byte, 1460)
	streamInfo := &interceptor.StreamInfo{
		SSRC:                1,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{{URI: transportCCURI, ID: 1}},
	}

	bwe, err := NewSendSideBWE(WithLoggerFactory(logging.NewDefaultLoggerFactory()))
	require.NoError(t, err)
	require.NotNil(t, bwe)

	gccMock := &mockGCCWriteStream{
		twccResponder: &mockTWCCResponder{
			bwe,
			make(chan []byte, 500),
		},
		t: t,
	}

	twccSender, err := (&twcc.SenderInterceptorFactory{}).NewInterceptor("")
	require.NoError(t, err)
	require.NotNil(t, twccSender)

	twccInboundRTP := twccSender.BindRemoteStream(streamInfo, gccMock.twccResponder)
	twccSender.BindRTCPWriter(gccMock.twccResponder)

	require.Equal(t, latestBitrate, bwe.GetTargetBitrate())
	require.NotEqual(t, 0, len(bwe.GetStats()))

	rtpWriter := bwe.AddStream(streamInfo, gccMock)
	require.NotNil(t, rtpWriter)

	twccWriter := twcc.HeaderExtensionInterceptor{}
	rtpWriter = twccWriter.BindLocalStream(streamInfo, rtpWriter)

	for i := 0; i <= 100; i++ {
		_, err = rtpWriter.Write(&rtp.Header{SSRC: 1, Extensions: []rtp.Extension{}}, rtpPayload, nil)
		assert.NoError(t, err)
		_, _, err = twccInboundRTP.Read(buffer, nil)
		assert.NoError(t, err)
	}

	// Sending a stream with zero loss and no RTT should increase estimate
	require.Less(t, latestBitrate, bwe.GetTargetBitrate())
}

func TestSendSideBWE_ErrorOnWriteRTCPAtClosedState(t *testing.T) {
	bwe, err := NewSendSideBWE()
	require.NoError(t, err)
	require.NotNil(t, bwe)

	pkts := []rtcp.Packet{&rtcp.TransportLayerCC{}}
	require.NoError(t, bwe.WriteRTCP(pkts, nil))
	require.Equal(t, bwe.isClosed(), false)
	require.NoError(t, bwe.Close())
	require.ErrorIs(t, bwe.WriteRTCP(pkts, nil), ErrSendSideBWEClosed)
	require.Equal(t, bwe.isClosed(), true)
}

func BenchmarkSendSideBWE_WriteRTCP(b *testing.B) {
	numSequencesPerTwccReport := []int{10, 100, 500, 1000}

	for _, count := range numSequencesPerTwccReport {
		b.Run(fmt.Sprintf("num_sequences=%d", count), func(b *testing.B) {
			bwe, err := NewSendSideBWE(SendSideBWEPacer(NewNoOpPacer()))
			require.NoError(b, err)
			require.NotNil(b, bwe)

			recorder := twcc.NewRecorder(5000)
			seq := uint16(0)
			arrivalTime := int64(0)

			for i := 0; i < b.N; i++ {
				// nolint:gosec
				seqs := rand.Intn(count/2) + count // [count, count * 1.5)
				for range seqs {
					seq++

					if rand.Intn(5) != 0 { //nolint:gosec
						arrivalTime += int64(rtcp.TypeTCCDeltaScaleFactor * (rand.Intn(128) + 1)) //nolint:gosec
						recorder.Record(5000, seq, arrivalTime)
					}
				}

				rtcpPackets := recorder.BuildFeedbackPacket()
				require.Equal(b, 1, len(rtcpPackets))

				require.NoError(b, bwe.WriteRTCP(rtcpPackets, nil))
			}

			require.NoError(b, bwe.Close())
		})
	}
}

// A packet under the stream's RTX SSRC — a NACK responder's resend written
// upstream of the estimator — is paced like the media, not dropped as an
// unknown SSRC.
func TestSendSideBWE_PacesTheRTXSSRC(t *testing.T) {
	bwe, err := NewSendSideBWE()
	require.NoError(t, err)
	defer func() { require.NoError(t, bwe.Close()) }()
	streamInfo := &interceptor.StreamInfo{
		SSRC:                1,
		SSRCRetransmission:  2,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{{URI: transportCCURI, ID: 1}},
	}
	seen := make(chan uint32, 4)
	rtpWriter := bwe.AddStream(streamInfo, interceptor.RTPWriterFunc(
		func(header *rtp.Header, payload []byte, _ interceptor.Attributes) (int, error) {
			seen <- header.SSRC

			return len(payload), nil
		},
	))
	twccWriter := twcc.HeaderExtensionInterceptor{}
	rtpWriter = twccWriter.BindLocalStream(streamInfo, rtpWriter)
	for _, ssrc := range []uint32{1, 2} {
		_, err = rtpWriter.Write(&rtp.Header{SSRC: ssrc, Extensions: []rtp.Extension{}}, make([]byte, 100), nil)
		require.NoError(t, err)
	}
	got := map[uint32]bool{}
	deadline := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case ssrc := <-seen:
			got[ssrc] = true
		case <-deadline:
			t.Fatalf("the pacer forwarded only %v", got)
		}
	}
}
