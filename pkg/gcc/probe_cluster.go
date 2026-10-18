// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"time"
)

// ProbeResult is what the wire did with a probe cluster: every packet that
// left the pacer between StartProbe and EndProbe, media and probe copies
// alike (libwebrtc's ProbeBitrateEstimator and LiveKit's probe groups
// judge a probe the same way).
type ProbeResult struct {
	ID                       int
	Sent, Received, Lost     int // packets
	SentBytes, ReceivedBytes int
	// SendSpan and ReceiveSpan run from the first to the last RECEIVED
	// packet, by departure and by arrival.
	SendSpan, ReceiveSpan time.Duration
	// DeliveredBitrate is ReceivedBytes over the longer of the two spans,
	// in bits per second: what the path carried, not what was offered.
	DeliveredBitrate int
}

// QueueGrowth is how much longer the received packets took to arrive than
// to leave: positive when the probe built a queue.
func (r ProbeResult) QueueGrowth() time.Duration { return r.ReceiveSpan - r.SendSpan }

// LossRatio is the share of the cluster's packets reported lost.
func (r ProbeResult) LossRatio() float64 {
	if r.Sent == 0 {
		return 0
	}

	return float64(r.Lost) / float64(r.Sent)
}

type probePacket struct {
	departure time.Time
	arrival   time.Time
	size      int
	reported  bool
}

// probeCluster accumulates one probe's packets and their feedback.
type probeCluster struct {
	id      int
	ended   bool
	endedAt time.Time
	packets map[uint16]*probePacket
}

func newProbeCluster(id int) *probeCluster {
	return &probeCluster{id: id, packets: map[uint16]*probePacket{}}
}

func (c *probeCluster) sent(seq uint16, at time.Time, size int) {
	if c.ended {
		return
	}
	c.packets[seq] = &probePacket{departure: at, size: size}
}

func (c *probeCluster) acked(seq uint16, arrival time.Time) {
	p, ok := c.packets[seq]
	if !ok {
		return
	}
	p.reported = true
	if !arrival.IsZero() {
		p.arrival = arrival
	}
}

func (c *probeCluster) end(at time.Time) {
	if !c.ended {
		c.ended = true
		c.endedAt = at
	}
}

// ready is true once the cluster ended and every packet was reported, or
// settle has passed since it ended (late feedback is lost feedback).
func (c *probeCluster) ready(now time.Time, settle time.Duration) bool {
	if !c.ended {
		return false
	}
	if now.Sub(c.endedAt) >= settle {
		return true
	}
	for _, p := range c.packets {
		if !p.reported {
			return false
		}
	}

	return true
}

func (c *probeCluster) result() ProbeResult {
	r := ProbeResult{ID: c.id}
	var firstDep, lastDep, firstArr, lastArr time.Time
	for _, p := range c.packets {
		r.Sent++
		r.SentBytes += p.size
		if p.arrival.IsZero() {
			r.Lost++

			continue
		}
		r.Received++
		r.ReceivedBytes += p.size
		if firstDep.IsZero() || p.departure.Before(firstDep) {
			firstDep = p.departure
		}
		if p.departure.After(lastDep) {
			lastDep = p.departure
		}
		if firstArr.IsZero() || p.arrival.Before(firstArr) {
			firstArr = p.arrival
		}
		if p.arrival.After(lastArr) {
			lastArr = p.arrival
		}
	}
	if r.Received < 2 {
		return r
	}
	r.SendSpan = lastDep.Sub(firstDep)
	r.ReceiveSpan = lastArr.Sub(firstArr)
	span := max(r.SendSpan, r.ReceiveSpan, time.Millisecond)
	r.DeliveredBitrate = int(float64(r.ReceivedBytes*8) / span.Seconds())

	return r
}

// probeSettle is how long a probe's feedback may take after it ended:
// 5 × RTT within [250 ms, 5 s] (LiveKit's probe settle time).
func probeSettle(rtt time.Duration) time.Duration {
	return min(max(5*rtt, 250*time.Millisecond), 5*time.Second)
}
