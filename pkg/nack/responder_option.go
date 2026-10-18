// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package nack

import (
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/internal/rtpbuffer"
	"github.com/pion/logging"
	"time"
)

// ResponderOption can be used to configure ResponderInterceptor.
type ResponderOption func(s *ResponderInterceptor) error

// ResponderSize sets the size of the interceptor.
// Size must be one of: 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768.
func ResponderSize(size uint16) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.size = size

		return nil
	}
}

// ResponderLog sets a logger for the interceptor.
func ResponderLog(log logging.LeveledLogger) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.log = log

		return nil
	}
}

// WithResponderLoggerFactory sets a logger factory for the interceptor.
func WithResponderLoggerFactory(loggerFactory logging.LoggerFactory) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.loggerFactory = loggerFactory

		return nil
	}
}

// DisableCopy bypasses copy of underlying packets. It should be used when
// you are not re-using underlying buffers of packets that have been written.
func DisableCopy() ResponderOption {
	return func(s *ResponderInterceptor) error {
		s.packetFactory = &rtpbuffer.PacketFactoryNoOp{}

		return nil
	}
}

// ResponderStreamsFilter sets filter for local streams.
func ResponderStreamsFilter(filter func(info *interceptor.StreamInfo) bool) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.streamsFilter = filter

		return nil
	}
}

// ResponderMaxResends caps how often one packet is resent; 0 (the
// default) means no cap. Repeated NACKs for a packet whose resend is
// still in flight would otherwise put copy after copy on a link that is
// already losing packets.
func ResponderMaxResends(n int) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.maxResends = n

		return nil
	}
}

// ResponderResendInterval sets the minimum time between two resends of
// the same packet, read at every NACK (a round-trip-time estimate makes
// it adapt); nil (the default) means no minimum.
func ResponderResendInterval(interval func() time.Duration) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.resendInterval = interval

		return nil
	}
}
