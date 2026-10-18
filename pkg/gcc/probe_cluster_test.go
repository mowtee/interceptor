// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// A cluster sent at 1 Mbit/s over 200 ms and delivered without a queue:
// the delivered rate is the sent rate, no growth, no loss.
func TestProbeClusterDeliveredInFull(t *testing.T) {
	c := newProbeCluster(1)
	t0 := time.Unix(1000, 0)
	for i := range 20 { // 20 × 1250 B over 200 ms = 1 Mbit/s
		c.sent(uint16(i), t0.Add(time.Duration(i)*10*time.Millisecond), 1250) //nolint:gosec
	}
	c.end(t0.Add(200 * time.Millisecond))
	assert.False(t, c.ready(t0.Add(210*time.Millisecond), time.Second), "unreported packets: not ready")
	for i := range 20 {
		c.acked(uint16(i), t0.Add(30*time.Millisecond+time.Duration(i)*10*time.Millisecond)) //nolint:gosec
	}
	assert.True(t, c.ready(t0.Add(260*time.Millisecond), time.Second), "all reported: ready")
	r := c.result()
	assert.Equal(t, 20, r.Received)
	assert.Zero(t, r.Lost)
	assert.Equal(t, time.Duration(0), r.QueueGrowth())
	assert.InDelta(t, 1_055_000, r.DeliveredBitrate, 10_000, "19 intervals of 10 ms carry 20 packets")
}

// The same cluster through a 500 kbit/s bottleneck: arrivals spread to
// twice the send span, the delivered rate is the bottleneck's, and the
// queue growth shows it.
func TestProbeClusterThroughABottleneck(t *testing.T) {
	c := newProbeCluster(2)
	t0 := time.Unix(1000, 0)
	for i := range 20 {
		c.sent(uint16(i), t0.Add(time.Duration(i)*10*time.Millisecond), 1250)                //nolint:gosec
		c.acked(uint16(i), t0.Add(30*time.Millisecond+time.Duration(i)*20*time.Millisecond)) //nolint:gosec
	}
	c.end(t0.Add(200 * time.Millisecond))
	r := c.result()
	assert.Equal(t, 190*time.Millisecond, r.QueueGrowth())
	assert.InDelta(t, 526_000, r.DeliveredBitrate, 10_000)
}

// Lost packets count against the cluster; feedback that never comes
// stops counting after the settle time.
func TestProbeClusterLossAndSettle(t *testing.T) {
	c := newProbeCluster(3)
	t0 := time.Unix(1000, 0)
	for i := range 10 {
		c.sent(uint16(i), t0.Add(time.Duration(i)*10*time.Millisecond), 1000) //nolint:gosec
	}
	c.end(t0.Add(100 * time.Millisecond))
	for i := range 8 {
		arrival := t0.Add(20*time.Millisecond + time.Duration(i)*10*time.Millisecond) //nolint:gosec
		if i == 3 {
			arrival = time.Time{} // reported lost
		}
		c.acked(uint16(i), arrival) //nolint:gosec
	}
	assert.False(t, c.ready(t0.Add(200*time.Millisecond), 250*time.Millisecond))
	assert.True(t, c.ready(t0.Add(350*time.Millisecond), 250*time.Millisecond))
	r := c.result()
	assert.Equal(t, 10, r.Sent)
	assert.Equal(t, 3, r.Lost, "one reported lost, two never reported")
	assert.InDelta(t, 0.3, r.LossRatio(), 1e-9)
	// Packets sent after the end do not join.
	c.sent(99, t0.Add(400*time.Millisecond), 1000)
	assert.Equal(t, 10, c.result().Sent)
}

func TestProbeSettle(t *testing.T) {
	assert.Equal(t, 250*time.Millisecond, probeSettle(0))
	assert.Equal(t, 500*time.Millisecond, probeSettle(100*time.Millisecond))
	assert.Equal(t, 5*time.Second, probeSettle(2*time.Second))
}
