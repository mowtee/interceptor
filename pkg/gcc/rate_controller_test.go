// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateControllerRun(t *testing.T) {
	cases := []struct {
		name           string
		initialBitrate int
		usage          []usage
		expected       []DelayStats
	}{
		{
			name:           "empty",
			initialBitrate: 100_000,
			usage:          []usage{},
			expected:       []DelayStats{},
		},
		{
			name:           "increasesMultiplicativelyBy8000",
			initialBitrate: 100_000,
			usage:          []usage{usageNormal, usageNormal},
			expected: []DelayStats{{
				Usage:           usageNormal,
				State:           stateIncrease,
				TargetBitrate:   108_000,
				ReceivedBitrate: 100_000,
				Estimate:        0,
				Threshold:       0,
			}},
		},
	}

	t0 := time.Time{}
	mockNoFn := func() time.Time {
		t0 = t0.Add(100 * time.Millisecond)

		return t0
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := make(chan DelayStats)
			dc := newRateController(mockNoFn, 100_000, 1_000, 50_000_000, func(ds DelayStats) {
				out <- ds
			})
			in := make(chan DelayStats)
			dc.onReceivedRate(100_000)
			dc.updateRTT(300 * time.Millisecond)
			go func() {
				defer close(out)
				for _, state := range tc.usage {
					dc.onDelayStats(DelayStats{
						Measurement:   0,
						Estimate:      0,
						Threshold:     0,
						Usage:         state,
						State:         0,
						TargetBitrate: 0,
					})
				}
				close(in)
			}()
			received := []DelayStats{}
			for ds := range out {
				received = append(received, ds)
			}
			if len(tc.expected) > 0 {
				assert.Equal(t, tc.expected[0], received[0])
			}
		})
	}
}

// libwebrtc's increase limit: an increase never takes the target above
// 1.5 × the received rate + 10 kbps, and a target already above that limit
// holds there instead of growing — the old rule stopped limiting once the
// target had passed the limit.
func TestRateControllerIncreaseIsLimitedByTheReceivedRate(t *testing.T) {
	c := &rateController{target: 2_000_000, latestReceivedRate: 700_000, minBitrate: 100_000, maxBitrate: 50_000_000}
	limit := int(1.5*700_000) + 10_000
	// Above the limit: hold, do not grow, do not drop.
	assert.Equal(t, 2_000_000, c.limitIncrease(2_160_000))
	// Below the limit: grow up to it.
	c.target = 900_000
	assert.Equal(t, 972_000, c.limitIncrease(972_000))
	assert.Equal(t, limit, c.limitIncrease(1_200_000))
	// Never lower the target on an increase step.
	c.target = 1_100_000
	assert.Equal(t, 1_100_000, c.limitIncrease(1_050_000))
	assert.Equal(t, 1_100_000, c.limitIncrease(1_188_000), "already above the limit: held")
}

func TestRateControllerSetEstimateReplacesTheTarget(t *testing.T) {
	c := &rateController{
		now: time.Now, target: 400_000, latestReceivedRate: 400_000, minBitrate: 100_000, maxBitrate: 2_000_000,
		lastUpdate: time.Now(), latestDecreaseRate: &exponentialMovingAverage{},
	}
	c.setEstimate(1_100_000)
	assert.Equal(t, 1_100_000, c.target)
	// The stale received rate's limit (610 kbps) is below the set
	// estimate: increases hold the target, they never lower it.
	assert.Equal(t, 1_100_000, c.limitIncrease(1_188_000))
	assert.Equal(t, 1_100_000, c.increase(time.Now().Add(time.Second)))
	// Clamped to the configured bounds.
	c.setEstimate(5_000_000)
	assert.Equal(t, 2_000_000, c.target)
	c.setEstimate(10)
	assert.Equal(t, 100_000, c.target)
}
