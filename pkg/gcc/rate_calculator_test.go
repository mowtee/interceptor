// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"testing"
	"time"

	"github.com/pion/interceptor/internal/cc"
	"github.com/stretchr/testify/assert"
)

func TestRateCalculator(t *testing.T) {
	t0 := time.Now()
	cases := []struct {
		name     string
		acks     []cc.Acknowledgment
		expected []int
	}{
		{
			name:     "emptyCreatesNoRate",
			acks:     []cc.Acknowledgment{},
			expected: []int{},
		},
		{
			name: "ignoresZeroArrivalTimes",
			acks: []cc.Acknowledgment{{
				SequenceNumber: 0,
				Size:           0,
				Departure:      time.Time{},
				Arrival:        time.Time{},
			}},
			expected: []int{},
		},
		{
			name: "singleAckCreatesRate",
			acks: []cc.Acknowledgment{{
				SequenceNumber: 0,
				Size:           1000,
				Departure:      time.Time{},
				Arrival:        t0,
			}},
			expected: []int{8000},
		},
		{
			name: "twoAcksCalculateCorrectRates",
			acks: []cc.Acknowledgment{{
				SequenceNumber: 0,
				Size:           125,
				Departure:      time.Time{},
				Arrival:        t0,
			}, {
				SequenceNumber: 0,
				Size:           125,
				Departure:      time.Time{},
				Arrival:        t0.Add(100 * time.Millisecond),
			}},
			// Two acks 100 ms apart: 250 B over the half-window floor
			// (250 ms), not over their 100 ms span.
			expected: []int{1000, 8_000},
		},
		{
			name: "steadyACKsCalculateCorrectRates",
			acks: getACKStream(10, 1200, 100*time.Millisecond),
			// Bytes per window once the history spans one; before that
			// over the span, floored at half a window (no start-up
			// inflation: 2,400 B over 100 ms used to read as 192 kbit/s).
			expected: []int{
				9_600,
				76_800,
				115_200,
				128_000,
				120_000,
				115_200,
				115_200,
				115_200,
				115_200,
				115_200,
			},
		},
	}

	// A burst after a gap — a bottleneck queue draining after a capacity
	// drop: five 1,200 B acks within 4 ms, one second after the last.
	// Per span that read as 12 Mbit/s; per window it is 6,000 B over
	// 250 ms = 192 kbit/s (the stale ack a second back leaves the window).
	burst := []cc.Acknowledgment{{SequenceNumber: 0, Size: 1200, Arrival: t0}}
	for i := 1; i <= 5; i++ {
		burst = append(burst, cc.Acknowledgment{SequenceNumber: uint16(i), Size: 1200, Arrival: t0.Add(time.Second + time.Duration(i)*time.Millisecond)})
	}
	cases = append(cases, struct {
		name     string
		acks     []cc.Acknowledgment
		expected []int
	}{
		name:     "aBurstAfterAGapReadsPerWindowNotPerSpan",
		acks:     burst,
		expected: []int{9_600, 38_400, 76_800, 115_200, 153_600, 192_000},
	})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc := newRateCalculator(500 * time.Millisecond)
			in := make(chan []cc.Acknowledgment)
			out := make(chan int)
			onRateUpdate := func(rate int) {
				out <- rate
			}
			go func() {
				defer close(out)
				rc.run(in, onRateUpdate)
			}()
			go func() {
				in <- tc.acks
				close(in)
			}()

			received := []int{}
			for r := range out {
				received = append(received, r)
			}
			assert.Equal(t, tc.expected, received)
		})
	}
}

func getACKStream(length int, size int, interval time.Duration) []cc.Acknowledgment {
	res := []cc.Acknowledgment{}
	t0 := time.Now()
	for range length {
		res = append(res, cc.Acknowledgment{
			Size:    size,
			Arrival: t0,
		})
		t0 = t0.Add(interval)
	}

	return res
}
