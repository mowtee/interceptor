// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/logging"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Audio and retransmissions jump a queue of video: at a pacing rate that
// lets one packet through per tick or two, video queued first still
// leaves after the audio and the RTX packet written later.
func TestLeakyBucketPacerOrdersByPriority(t *testing.T) {
	pacer := newLeakyBucketPacer(160_000, logging.NewDefaultLoggerFactory()) // 1.5× → 150 B per 5 ms tick
	defer func() { require.NoError(t, pacer.Close()) }()
	var mu sync.Mutex
	var order []uint32
	writer := interceptor.RTPWriterFunc(func(header *rtp.Header, payload []byte, _ interceptor.Attributes) (int, error) {
		mu.Lock()
		order = append(order, header.SSRC)
		mu.Unlock()

		return len(payload), nil
	})
	const video, rtx, audio = 1, 2, 3
	pacer.AddStream(video, writer)
	pacer.AddStreamWithPriority(rtx, writer, PacerPriorityRetransmission)
	pacer.AddStreamWithPriority(audio, writer, PacerPriorityAudio)
	for i := 0; i < 6; i++ {
		_, err := pacer.Write(&rtp.Header{SSRC: video, SequenceNumber: uint16(i)}, make([]byte, 1000), nil) //nolint:gosec
		require.NoError(t, err)
	}
	_, err := pacer.Write(&rtp.Header{SSRC: rtx}, make([]byte, 1000), nil)
	require.NoError(t, err)
	_, err = pacer.Write(&rtp.Header{SSRC: audio}, make([]byte, 100), nil)
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n >= 8 {
			break
		}
		require.Less(t, time.Now(), deadline, "the pacer did not drain: %v", order)
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	// Whatever left on the first tick, audio and the resend go before the
	// video still waiting.
	audioAt, rtxAt, lastVideo := -1, -1, -1
	for i, ssrc := range order {
		switch ssrc {
		case audio:
			audioAt = i
		case rtx:
			rtxAt = i
		case video:
			lastVideo = i
		}
	}
	assert.Less(t, audioAt, rtxAt, "audio before the resend: %v", order)
	assert.Less(t, rtxAt, lastVideo, "the resend before the queued video: %v", order)
	assert.LessOrEqual(t, audioAt, 2, "audio within the first packets: %v", order)
}

// A stream registered without a priority is video; an unknown SSRC is
// queued as video and reported as before.
func TestLeakyBucketPacerDefaultsToVideo(t *testing.T) {
	pacer := newLeakyBucketPacer(1_000_000, logging.NewDefaultLoggerFactory())
	defer func() { require.NoError(t, pacer.Close()) }()
	pacer.AddStream(7, interceptor.RTPWriterFunc(func(_ *rtp.Header, payload []byte, _ interceptor.Attributes) (int, error) {
		return len(payload), nil
	}))
	assert.Equal(t, PacerPriorityVideo, pacer.ssrcToPriority[7])
	pacer.AddStreamWithPriority(8, nil, PacerPriority(99))
	assert.Equal(t, PacerPriorityVideo, pacer.ssrcToPriority[8], "an out-of-range priority is video")
}
