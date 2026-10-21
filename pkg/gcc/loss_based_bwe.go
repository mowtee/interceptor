// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"math"
	"sync"
	"time"

	"github.com/pion/interceptor/internal/cc"
	"github.com/pion/logging"
)

const (
	// constants from
	// https://datatracker.ietf.org/doc/html/draft-ietf-rmcat-gcc-02#section-6

	increaseLossThreshold = 0.02
	// libwebrtc (SendSideBandwidthEstimation::UpdateEstimate, kBweIncreaseInterval):
	// with no loss the estimate grows 8 % at most once per second. At 5 %
	// per 200 ms it grew 28 %/s — ×2.9 in four seconds on nothing delivered
	// (sotto, 800 kbit/s step: 514 kbit/s → 1.5 Mbit/s), far ahead of the
	// delay controller's climb, and an SFU allocating on the minimum of the
	// two upswitched into the cut on the loss controller's number.
	increaseTimeThreshold = time.Second
	increaseFactor        = 1.08

	decreaseLossThreshold = 0.1
	decreaseTimeThreshold = 200 * time.Millisecond

	// The loss ratio is judged over the newest packets until both
	// lossWindowPackets and lossWindowSpan (by send time) are covered —
	// never one report. A transport-cc report carries a handful of
	// packets at video rates (6–8 at 150 packets/s every ~45 ms), so one
	// lost packet reads ~15 % for its report, and pion's 200 ms average of
	// per-report ratios cut the target on a 90 ms burst on an otherwise
	// clean link (home lab R11h, replayed: 2–4 cuts per subscriber in
	// 50 s; LiveKit decides over ≥ 3 groups of ≥ 30 packets / 500 ms).
	lossWindowPackets = 100
	lossWindowSpan    = 500 * time.Millisecond
	// lossWindowMax bounds the history kept; below lossWindowMin packets
	// in it (a low-rate stream) no loss ratio is computed and only a
	// window without any loss may increase.
	lossWindowMax = 2 * time.Second
	lossWindowMin = 20

	// lossLimitClear is how long after the last loss cut, with the window
	// under increaseLossThreshold, the controller stops constraining.
	lossLimitClear = 2 * time.Second
)

// lossEntry is one packet the feedback reported on.
type lossEntry struct {
	seq       int64 // transport-wide sequence number, unwrapped
	departure time.Time
	received  bool
}

// LossStats contains internal statistics of the loss based controller.
type LossStats struct {
	TargetBitrate int
	AverageLoss   float64
}

type lossBasedBandwidthEstimator struct {
	lock           sync.Mutex
	maxBitrate     int
	minBitrate     int
	bitrate        int
	averageLoss    float64
	lastLossUpdate time.Time
	lastIncrease   time.Time
	lastDecrease   time.Time
	log            logging.LeveledLogger
	now            func() time.Time

	// window holds the reported packets in send order; index maps an
	// unwrapped sequence number to its entry, so a packet first reported
	// lost and later reported received is un-lost. countFrom excludes
	// packets sent before the last decrease: the same losses cut once.
	window    []lossEntry
	index     map[int64]int
	highest   int64
	hasSeq    bool
	countFrom time.Time

	// lossLimited: a loss cut happened and the window has not been clean
	// for lossLimitClear since. Only then does this controller constrain
	// the target (libwebrtc's LossBasedBweV2 is "delay based" until loss
	// limits it); otherwise it follows the delay controller.
	lossLimited bool
	// follow: not loss limited and the window reads under
	// increaseLossThreshold — the controller takes the delay target up.
	// Between the thresholds it holds (and follows down), as before: on
	// a 5 %-loss link following the delay controller up built a 1.1–1.3 s
	// queue (home lab R19, `poor`).
	follow bool
}

func newLossBasedBWE(initialBitrate int, loggerFactory logging.LoggerFactory) *lossBasedBandwidthEstimator {
	return &lossBasedBandwidthEstimator{
		lock:           sync.Mutex{},
		maxBitrate:     100_000_000, // 100 mbit
		minBitrate:     100_000,     // 100 kbit
		bitrate:        initialBitrate,
		averageLoss:    0,
		lastLossUpdate: time.Time{},
		lastIncrease:   time.Time{},
		lastDecrease:   time.Time{},
		log:            loggerFactory.NewLogger("gcc_loss_controller"),
		now:            time.Now,
		index:          map[int64]int{},
	}
}

func (e *lossBasedBandwidthEstimator) getEstimate(wantedRate int) LossStats {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.bitrate <= 0 || e.follow {
		// Not loss limited: follow the delay controller up as well as
		// down. Latching every dip of it (min) and climbing back at 8 %/s
		// — and only while the window read under 2 %, a quarter of the
		// time on a 1 %-loss link — held a third of the delay target for
		// seconds with no loss cut at all (home lab R18).
		e.bitrate = clampInt(wantedRate, e.minBitrate, e.maxBitrate)
	}
	e.bitrate = min(wantedRate, e.bitrate)

	return LossStats{
		TargetBitrate: e.bitrate,
		AverageLoss:   e.averageLoss,
	}
}

// setBitrate applies a probe result (libwebrtc's SendSideBandwidthEstimation::
// SetSendBitrate): the loss controller's bitrate jumps to a rate the link
// carried without loss; its next increase waits the usual interval.
func (e *lossBasedBandwidthEstimator) setBitrate(rate int) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.bitrate = clampInt(rate, e.minBitrate, e.maxBitrate)
	e.lastIncrease = e.now()
}

func (e *lossBasedBandwidthEstimator) updateLossEstimate(results []cc.Acknowledgment) {
	if len(results) == 0 {
		return
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	for _, p := range results {
		seq := e.unwrap(p.SequenceNumber)
		if idx, ok := e.index[seq]; ok {
			if !p.Arrival.IsZero() {
				e.window[idx].received = true // reported late: not a loss
			}

			continue
		}
		e.index[seq] = len(e.window)
		e.window = append(e.window, lossEntry{seq: seq, departure: p.Departure, received: !p.Arrival.IsZero()})
	}
	e.prune()

	lossRatio, valid, anyLost := e.windowLoss()
	e.lastLossUpdate = e.now()
	defer func() {
		e.follow = !e.lossLimited && (valid && lossRatio < increaseLossThreshold || !valid && !anyLost)
	}()
	if valid {
		e.averageLoss = lossRatio
	}

	if e.lossLimited && valid && lossRatio < increaseLossThreshold && e.now().Sub(e.lastDecrease) >= lossLimitClear {
		e.lossLimited = false
	}

	switch {
	case (valid && lossRatio < increaseLossThreshold || !valid && !anyLost) &&
		e.now().Sub(e.lastIncrease) > increaseTimeThreshold:
		e.log.Infof("loss controller increasing; loss: %v (window valid %v)", lossRatio, valid)
		e.lastIncrease = e.now()
		e.bitrate = clampInt(int(increaseFactor*float64(e.bitrate)), e.minBitrate, e.maxBitrate)
	case valid && lossRatio > decreaseLossThreshold && e.now().Sub(e.lastDecrease) > decreaseTimeThreshold:
		e.log.Infof("loss controller decreasing; loss: %v", lossRatio)
		e.lastDecrease = e.now()
		e.lossLimited = true
		e.bitrate = clampInt(int(float64(e.bitrate)*(1-0.5*lossRatio)), e.minBitrate, e.maxBitrate)
		// The losses that cut are spent: the next decrease needs new ones.
		e.countFrom = e.window[len(e.window)-1].departure
	}
}

// unwrap extends a 16-bit transport-wide sequence number to the value
// nearest the highest seen.
func (e *lossBasedBandwidthEstimator) unwrap(seq16 uint16) int64 {
	if !e.hasSeq {
		e.hasSeq = true
		e.highest = int64(seq16)

		return e.highest
	}
	cand := e.highest&^0xFFFF | int64(seq16)
	switch {
	case cand-e.highest > 0x8000:
		cand -= 0x10000
	case e.highest-cand > 0x8000:
		cand += 0x10000
	}
	e.highest = max(e.highest, cand)

	return cand
}

// prune drops entries sent more than lossWindowMax before the newest.
func (e *lossBasedBandwidthEstimator) prune() {
	if len(e.window) == 0 {
		return
	}
	newest := e.window[len(e.window)-1].departure
	drop := 0
	for drop < len(e.window) && newest.Sub(e.window[drop].departure) > lossWindowMax {
		delete(e.index, e.window[drop].seq)
		drop++
	}
	if drop == 0 {
		return
	}
	e.window = append(e.window[:0], e.window[drop:]...)
	for i, en := range e.window {
		e.index[en.seq] = i
	}
}

// windowLoss is the loss ratio over the newest packets (sent after the
// last decrease) until lossWindowPackets and lossWindowSpan are both
// covered. valid is false without enough packets; anyLost says whether
// what there is holds a loss.
func (e *lossBasedBandwidthEstimator) windowLoss() (ratio float64, valid, anyLost bool) {
	if len(e.window) == 0 {
		return 0, false, false
	}
	newest := e.window[len(e.window)-1].departure
	total, lost := 0, 0
	covered := false
	for i := len(e.window) - 1; i >= 0; i-- {
		en := e.window[i]
		if !e.countFrom.IsZero() && !en.departure.After(e.countFrom) {
			break
		}
		total++
		if !en.received {
			lost++
		}
		if total >= lossWindowPackets && newest.Sub(en.departure) >= lossWindowSpan {
			covered = true

			break
		}
	}
	if total == 0 {
		return 0, false, false
	}
	ratio = float64(lost) / float64(total)
	// A low-rate stream (all of lossWindowMax holds fewer packets than
	// the window wants) is judged over what there is, if enough.
	if !covered && total >= lossWindowMin && e.window[0].departure.Before(newest.Add(-lossWindowMax/2)) {
		covered = true
	}

	return ratio, covered, lost > 0
}

func (e *lossBasedBandwidthEstimator) average(delta time.Duration, prev, sample float64) float64 {
	return sample + math.Exp(-float64(delta.Milliseconds())/200.0)*(prev-sample)
}
