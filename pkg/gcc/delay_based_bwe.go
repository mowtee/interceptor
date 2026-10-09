// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"fmt"
	"sync"
	"time"

	"github.com/pion/interceptor/internal/cc"
	"github.com/pion/logging"
)

// DelayStats contains some internal statistics of the delay based congestion
// controller.
type DelayStats struct {
	Measurement      time.Duration
	Estimate         time.Duration
	Threshold        time.Duration
	LastReceiveDelta time.Duration

	Usage         usage
	State         state
	TargetBitrate int
}

type now func() time.Time

type delayController struct {
	dbg         *dbgState
	ackPipe     chan<- []cc.Acknowledgment
	ackRatePipe chan<- []cc.Acknowledgment

	*arrivalGroupAccumulator
	*rateController

	onUpdateCallback func(DelayStats)

	wg sync.WaitGroup

	log logging.LeveledLogger
}

type delayControllerConfig struct {
	nowFn          now
	initialBitrate int
	minBitrate     int
	maxBitrate     int
}

func newDelayController(delayConfig delayControllerConfig, loggerFactory logging.LoggerFactory) *delayController {
	ackPipe := make(chan []cc.Acknowledgment)
	ackRatePipe := make(chan []cc.Acknowledgment)

	delayController := &delayController{
		ackPipe:                 ackPipe,
		ackRatePipe:             ackRatePipe,
		arrivalGroupAccumulator: nil,
		rateController:          nil,
		onUpdateCallback:        nil,
		wg:                      sync.WaitGroup{},
		log:                     loggerFactory.NewLogger("gcc_delay_controller"),
	}

	rateController := newRateController(
		delayConfig.nowFn, delayConfig.initialBitrate, delayConfig.minBitrate, delayConfig.maxBitrate,
		func(ds DelayStats) {
			delayController.log.Infof("delaystats: %v", ds)
			if delayController.onUpdateCallback != nil {
				delayController.onUpdateCallback(ds)
			}
		},
	)
	delayController.rateController = rateController
	dbgS := &dbgState{id: fmt.Sprintf("%p", delayController)}
	delayController.dbg = dbgS
	rateController.dbg = dbgS
	overuseDetector := newOveruseDetector(newAdaptiveThreshold(), 10*time.Millisecond, rateController.onDelayStats)
	slopeEstimator := newSlopeEstimator(newKalman(), overuseDetector.onDelayStats)
	arrivalGroupAccumulator := newArrivalGroupAccumulator()

	rc := newRateCalculator(500 * time.Millisecond)

	delayController.wg.Add(2)
	go func() {
		defer delayController.wg.Done()
		arrivalGroupAccumulator.run(ackPipe, func(ag arrivalGroup) {
			dbgS.group(ag)
			slopeEstimator.onArrivalGroup(ag)
		})
	}()
	go func() {
		defer delayController.wg.Done()
		rc.run(ackRatePipe, func(rate int) {
			if dbgS.active() {
				dbg(dbgS, "gcc rate", "rate", rate)
			}
			rateController.onReceivedRate(rate)
		})
	}()

	return delayController
}

func (d *delayController) onUpdate(f func(DelayStats)) {
	d.onUpdateCallback = f
}

func (d *delayController) updateDelayEstimate(acks []cc.Acknowledgment) {
	if len(acks) > 0 {
		d.dbg.mark()
	}
	d.ackPipe <- acks
	d.ackRatePipe <- acks
}

func (d *delayController) Close() error {
	defer d.wg.Wait()

	close(d.ackPipe)
	close(d.ackRatePipe)

	return nil
}
