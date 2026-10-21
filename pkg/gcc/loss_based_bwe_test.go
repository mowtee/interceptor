// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pion/interceptor/internal/cc"

	"github.com/pion/logging"
	"github.com/stretchr/testify/assert"
)

func TestLossBasedBWESetBitrateReplacesTheEstimate(t *testing.T) {
	e := newLossBasedBWE(400_000, logging.NewDefaultLoggerFactory())
	e.lossLimited = true // a set bitrate binds only while loss limits
	e.setBitrate(1_100_000)
	assert.Equal(t, 1_100_000, e.getEstimate(2_000_000).TargetBitrate)
	// Still bounded by what the delay controller wants.
	assert.Equal(t, 900_000, e.getEstimate(900_000).TargetBitrate)
	// Clamped to the controller's bounds, and the next increase waits
	// the usual interval from the set.
	e.setBitrate(10)
	assert.Equal(t, 100_000, e.getEstimate(2_000_000).TargetBitrate)
	assert.WithinDuration(t, time.Now(), e.lastIncrease, time.Second)
	e.setBitrate(1_000_000_000)
	assert.Equal(t, 100_000_000, e.getEstimate(1_000_000_000).TargetBitrate)
}

// lossFeed drives a loss controller with transport feedback: pps packets
// a second, a report every feedbackEvery, each packet lost with
// probability loss (seeded).
type lossFeed struct {
	e      *lossBasedBandwidthEstimator
	clock  time.Time
	seq    uint16
	rng    *rand.Rand
	minBps int
	cuts   int // updates after which the target was lower than before
	wanted int // the delay controller's target the feed asks against
}

func newLossFeed(initial int, seed int64) *lossFeed {
	f := &lossFeed{
		e:     newLossBasedBWE(initial, logging.NewDefaultLoggerFactory()),
		clock: time.Unix(1000, 0),
		rng:   rand.New(rand.NewSource(seed)), //nolint:gosec // test randomness
	}
	f.e.now = func() time.Time { return f.clock }
	f.minBps = initial
	f.wanted = initial

	return f
}

func (f *lossFeed) run(d time.Duration, pps int, feedbackEvery time.Duration, loss float64) {
	perReport := float64(pps) * feedbackEvery.Seconds()
	carry := 0.0
	for end := f.clock.Add(d); f.clock.Before(end); {
		f.clock = f.clock.Add(feedbackEvery)
		carry += perReport
		n := int(carry)
		carry -= float64(n)
		acks := make([]cc.Acknowledgment, n)
		for i := range acks {
			acks[i] = cc.Acknowledgment{SequenceNumber: f.seq, Departure: f.clock.Add(-50 * time.Millisecond)}
			if f.rng.Float64() >= loss {
				acks[i].Arrival = f.clock.Add(-10 * time.Millisecond)
			}
			f.seq++
		}
		f.update(acks)
	}
}

func (f *lossFeed) update(acks []cc.Acknowledgment) {
	before := f.e.getEstimate(f.wanted).TargetBitrate
	f.e.updateLossEstimate(acks)
	after := f.e.getEstimate(f.wanted).TargetBitrate
	if after < before {
		f.cuts++
	}
	f.minBps = min(f.minBps, after)
}

// runBurst sends `reports` reports of 7 packets, `lost` of each lost.
func (f *lossFeed) runBurst(reports, lost int) {
	for range reports {
		f.clock = f.clock.Add(45 * time.Millisecond)
		acks := make([]cc.Acknowledgment, 7)
		for i := range acks {
			acks[i] = cc.Acknowledgment{SequenceNumber: f.seq, Departure: f.clock.Add(-50 * time.Millisecond)}
			if i >= lost {
				acks[i].Arrival = f.clock.Add(-10 * time.Millisecond)
			}
			f.seq++
		}
		f.update(acks)
	}
}

// 1 % random loss on a link carrying ~150 packets/s, reported every ~45 ms
// (6–7 packets a report): a single loss reads ~15 % for its report. The
// loss controller must not cut on that — libwebrtc's 10 % decrease
// threshold is about the link, not one report — so over a minute the
// target never drops below where it started. (pion's per-report ratio
// on a 200 ms average cut it 30–50 % within seconds; home lab R11.)
func TestLossBasedBWEDoesNotCutOnRandomLossInSmallReports(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		f := newLossFeed(1_000_000, seed)
		f.run(60*time.Second, 150, 45*time.Millisecond, 0.01)
		assert.Zero(t, f.cuts, "seed %d: the target was cut on 1 %% random loss", seed)
	}
}

// A short burst — six packets lost across two consecutive reports, as a
// Wi-Fi hiccup or a resend copied three times makes them — every five
// seconds on an otherwise clean link (0.8 % loss overall). Over the
// window libwebrtc's loss estimator judges (~1 s) that is 6 %; pion's
// per-report average read it as 15 % and cut (2–4 cuts per subscriber
// in 50 s when the home lab's R11h feedback is replayed through it).
func TestLossBasedBWEDoesNotCutOnAShortBurst(t *testing.T) {
	f := newLossFeed(1_000_000, 3)
	for range 12 {
		f.run(5*time.Second, 150, 45*time.Millisecond, 0.0)
		f.runBurst(2, 3)
	}
	assert.Zero(t, f.cuts, "a 90 ms burst on a clean link cut the target")
}

// Sustained loss above the decrease threshold still cuts, and promptly.
func TestLossBasedBWECutsOnSustainedLoss(t *testing.T) {
	f := newLossFeed(1_000_000, 7)
	f.run(2*time.Second, 150, 45*time.Millisecond, 0.0)
	start := f.e.getEstimate(f.wanted).TargetBitrate
	f.run(1500*time.Millisecond, 150, 45*time.Millisecond, 0.20)
	assert.Less(t, f.e.getEstimate(f.wanted).TargetBitrate, start*9/10, "20 %% loss for 1.5 s must cut")
}

// The same losses cut once: after a decrease, only packets sent after it
// count toward the next.
func TestLossBasedBWECutsOncePerWindow(t *testing.T) {
	f := newLossFeed(1_000_000, 9)
	// The window needs ≥ 100 packets and ≥ 500 ms: at 150 packets/s the
	// first decision comes after ~0.7 s of loss.
	f.run(800*time.Millisecond, 150, 45*time.Millisecond, 0.20)
	first := f.e.getEstimate(f.wanted).TargetBitrate
	assert.Less(t, first, 1_000_000)
	// One report later, the same window's losses must not cut again.
	f.run(45*time.Millisecond, 150, 45*time.Millisecond, 0.0)
	assert.Equal(t, first, f.e.getEstimate(f.wanted).TargetBitrate)
}

// A packet a report marked lost and a later report marked received is
// not a loss.
func TestLossBasedBWELateReportedPacketIsNotLost(t *testing.T) {
	e := newLossBasedBWE(1_000_000, logging.NewDefaultLoggerFactory())
	clock := time.Unix(1000, 0)
	e.now = func() time.Time { return clock }
	var acks []cc.Acknowledgment
	for i := range 120 {
		a := cc.Acknowledgment{SequenceNumber: uint16(i), Departure: clock}
		if i%5 != 0 {
			a.Arrival = clock
		}
		acks = append(acks, a)
	}
	clock = clock.Add(600 * time.Millisecond)
	e.updateLossEstimate(acks) // 20 % "lost"
	for i := range acks {
		acks[i].Arrival = clock // a later report: all received
	}
	clock = clock.Add(50 * time.Millisecond)
	e.updateLossEstimate(acks)
	assert.InDelta(t, 0.0, e.getEstimate(1<<30).AverageLoss, 1e-9)
}

// The loss controller constrains the target only while the link is loss
// limited (libwebrtc's LossBasedBweV2): without a loss cut it follows the
// delay controller up as well as down. It used to latch the delay target
// whenever it dipped (min) and then climb back at 8 %/s, and only while
// the window read under 2 % — on a 1 %-loss link a third of the delay
// target for seconds with no loss cut at all (home lab R18: delay 1.39
// Mbit/s, loss 0.47).
func TestLossBasedBWEFollowsTheDelayTargetUnlessLossLimited(t *testing.T) {
	f := newLossFeed(1_000_000, 4)
	f.run(2*time.Second, 150, 45*time.Millisecond, 0.0)
	if got := f.e.getEstimate(430_000).TargetBitrate; got != 430_000 {
		t.Fatalf("a dip of the delay target applies: %d", got)
	}
	f.run(45*time.Millisecond, 150, 45*time.Millisecond, 0.0)
	if got := f.e.getEstimate(1_390_000).TargetBitrate; got != 1_390_000 {
		t.Fatalf("not loss limited: the delay target's rise applies at once, got %d", got)
	}
	// Sustained loss cuts and the controller is loss limited: it holds
	// below the delay target and climbs at its own pace.
	f.run(1500*time.Millisecond, 150, 45*time.Millisecond, 0.20)
	cut := f.e.getEstimate(2_000_000).TargetBitrate
	if cut >= 1_390_000 {
		t.Fatalf("20 %% loss must cut: %d", cut)
	}
	f.run(500*time.Millisecond, 150, 45*time.Millisecond, 0.0)
	if got := f.e.getEstimate(2_000_000).TargetBitrate; got > cut*11/10 {
		t.Fatalf("loss limited: no jump back to the delay target (%d after the cut to %d)", got, cut)
	}
	// After a clean stretch the limit lifts.
	f.run(4*time.Second, 150, 45*time.Millisecond, 0.0)
	if got := f.e.getEstimate(2_000_000).TargetBitrate; got != 2_000_000 {
		t.Fatalf("clean for 4 s: no longer loss limited, got %d", got)
	}
}

// Between the thresholds (2–10 % loss, no cut) the controller holds: it
// follows the delay target down, not up.
func TestLossBasedBWEHoldsInTheLossBand(t *testing.T) {
	f := newLossFeed(1_000_000, 6)
	f.run(2*time.Second, 150, 45*time.Millisecond, 0.0)
	f.wanted = 600_000
	f.run(45*time.Millisecond, 150, 45*time.Millisecond, 0.0)
	if got := f.e.getEstimate(600_000).TargetBitrate; got != 600_000 {
		t.Fatalf("down with the delay target: %d", got)
	}
	f.run(3*time.Second, 150, 45*time.Millisecond, 0.05)
	if got := f.e.getEstimate(1_400_000).TargetBitrate; got > 700_000 {
		t.Fatalf("5 %% loss: no rise with the delay target, got %d", got)
	}
}
