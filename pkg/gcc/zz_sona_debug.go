// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

// Sona debug instrumentation (branch sona-debug-gcc, never upstream):
// JSON lines on stderr, in sotto's log format, to find what collapses a
// fresh subscriber's estimate at the start of video.

package gcc

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// dbgWindow: per-estimator logging window after its first feedback and
// again after its first video-sized packet (audio feedback can start
// seconds before video does).
const dbgWindow = 3 * time.Second

type dbgState struct {
	id    string
	start atomic.Int64 // unix nanos of the first feedback; 0 = none yet
	video atomic.Int64 // unix nanos of the first video-sized packet
	arr0  atomic.Int64 // first arrival (receiver clock), base for arrivals
}

// dbgVideoBytes: a packet at least this large is video, not audio.
const dbgVideoBytes = 400

func (s *dbgState) mark() {
	s.start.CompareAndSwap(0, time.Now().UnixNano())
}

func (s *dbgState) active() bool {
	st := s.start.Load()

	if st != 0 && time.Since(time.Unix(0, st)) < dbgWindow {
		return true
	}
	v := s.video.Load()

	return v != 0 && time.Since(time.Unix(0, v)) < dbgWindow
}

// relArr is milliseconds of a receiver-clock arrival since the first one.
func (s *dbgState) relArr(t time.Time) string {
	if t.IsZero() {
		return "null"
	}
	s.arr0.CompareAndSwap(0, t.UnixNano())

	return fmt.Sprintf("%.3f", float64(t.UnixNano()-s.arr0.Load())/1e6)
}

// rel is milliseconds of t since the first feedback.
func (s *dbgState) rel(t time.Time) string {
	st := s.start.Load()
	if st == 0 || t.IsZero() {
		return "null"
	}

	return fmt.Sprintf("%.3f", float64(t.UnixNano()-st)/1e6)
}

func dbg(s *dbgState, msg string, kv ...any) {
	var b strings.Builder
	fmt.Fprintf(&b, `{"time":%q,"level":"DEBUG","msg":%q,"gcc":%q`, time.Now().Format(time.RFC3339Nano), msg, s.id)
	for i := 0; i+1 < len(kv); i += 2 {
		switch v := kv[i+1].(type) {
		case string:
			if v == "null" || (len(v) > 0 && (v[0] == '-' || (v[0] >= '0' && v[0] <= '9'))) {
				fmt.Fprintf(&b, `,%q:%s`, kv[i], v)
			} else {
				fmt.Fprintf(&b, `,%q:%q`, kv[i], v)
			}
		case time.Duration:
			fmt.Fprintf(&b, `,%q:%.3f`, kv[i], float64(v.Microseconds())/1000)
		default:
			fmt.Fprintf(&b, `,%q:%v`, kv[i], v)
		}
	}
	b.WriteString("}\n")
	_, _ = os.Stderr.WriteString(b.String())
}

func (s *dbgState) group(ag arrivalGroup) {
	for _, p := range ag.packets {
		if p.Size >= dbgVideoBytes && s.video.CompareAndSwap(0, time.Now().UnixNano()) {
			dbg(s, "gcc first video", "seq", p.SequenceNumber, "bytes", p.Size, "dep", s.rel(p.Departure))
		}
	}
	if !s.active() || len(ag.packets) == 0 {
		return
	}
	first, last := ag.packets[0], ag.packets[len(ag.packets)-1]
	bytes := 0
	for _, p := range ag.packets {
		bytes += p.Size
	}
	dbg(s, "gcc group",
		"n", len(ag.packets), "bytes", bytes,
		"depFirst", s.rel(first.Departure), "depLast", s.rel(last.Departure),
		"arrFirst", s.relArr(first.Arrival), "arrLast", s.relArr(last.Arrival),
		"groupDep", s.rel(ag.departure), "groupArr", s.relArr(ag.arrival))
}
