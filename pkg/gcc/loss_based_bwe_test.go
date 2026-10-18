// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"testing"
	"time"

	"github.com/pion/logging"
	"github.com/stretchr/testify/assert"
)

func TestLossBasedBWESetBitrateReplacesTheEstimate(t *testing.T) {
	e := newLossBasedBWE(400_000, logging.NewDefaultLoggerFactory())
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
