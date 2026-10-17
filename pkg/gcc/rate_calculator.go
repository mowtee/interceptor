// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"time"

	"github.com/pion/interceptor/internal/cc"
)

type rateCalculator struct {
	window time.Duration
}

func newRateCalculator(window time.Duration) *rateCalculator {
	return &rateCalculator{
		window: window,
	}
}

func (c *rateCalculator) run(in <-chan []cc.Acknowledgment, onRateUpdate func(int)) {
	var history []cc.Acknowledgment
	init := false
	sum := 0
	for acks := range in {
		for _, next := range acks {
			if next.Arrival.IsZero() {
				// Ignore packet if it didn't arrive
				continue
			}
			history = append(history, next)
			sum += next.Size

			if !init {
				init = true
				// Don't know any timeframe here, only arrival of last packet,
				// which is by definition in the window that ends with the last
				// arrival time
				onRateUpdate(next.Size * 8)

				continue
			}

			del := 0
			for _, ack := range history {
				deadline := next.Arrival.Add(-c.window)
				if !ack.Arrival.Before(deadline) {
					break
				}
				del++
				sum -= ack.Size
			}
			history = history[del:]
			if len(history) == 0 {
				onRateUpdate(0)

				continue
			}
			// Bytes per window, as libwebrtc's acknowledged-bitrate
			// estimator measures it — not per the span the kept acks
			// happen to cover: after a gap, a burst of acks (a bottleneck
			// queue draining after a capacity drop) spans a few
			// milliseconds and read as megabits per second (sotto,
			// 800 kbit/s step: "received" 1.35 Mbit/s on an 800 kbit/s
			// link, and the increase limit at 1.5× of that). Until the
			// history spans a window the span is used, floored at half a
			// window so a start-up burst cannot inflate it either.
			dt := next.Arrival.Sub(history[0].Arrival)
			if dt < c.window {
				dt = max(dt, c.window/2)
			}
			bits := 8 * sum
			rate := int(float64(bits) / dt.Seconds())
			onRateUpdate(rate)
		}
	}
}
