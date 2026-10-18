// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/internal/cc"
	"github.com/pion/logging"
	"github.com/pion/rtp"
)

var errLeakyBucketPacerPoolCastFailed = errors.New("failed to access leaky bucket pacer pool, cast failed")

type item struct {
	header     *rtp.Header
	payload    *[]byte
	size       int
	attributes interceptor.Attributes
}

// PacerPriority orders a pacer's queues: lower goes first. libwebrtc's
// pacer sends audio before retransmissions before video — a resend
// queued behind a keyframe burst at the pacing rate (a 40 kB keyframe
// drains in ~180 ms at 1.5 × 1.2 Mbit/s) arrives after the receiver's
// repair window and the frame is lost anyway.
type PacerPriority int

const (
	PacerPriorityAudio          PacerPriority = iota // first
	PacerPriorityRetransmission                      // RTX before new media
	PacerPriorityVideo                               // default
	pacerPriorities
	// pacerProbeLane is the queue of probe packets (the probe attribute):
	// sent every tick, outside the budget.
	pacerProbeLane = pacerPriorities
)

// PriorityPacer is a Pacer that orders its queue by stream priority.
type PriorityPacer interface {
	Pacer
	AddStreamWithPriority(ssrc uint32, writer interceptor.RTPWriter, priority PacerPriority)
}

// LeakyBucketPacer implements a leaky bucket pacing algorithm.
type LeakyBucketPacer struct {
	log logging.LeveledLogger

	f                 float64
	targetBitrate     int
	targetBitrateLock sync.Mutex

	pacingInterval time.Duration

	qLock  sync.RWMutex
	queues [pacerPriorities + 1]*list.List // drained in priority order; the last is the probe lane
	done   chan struct{}

	ssrcToWriter   map[uint32]interceptor.RTPWriter
	ssrcToPriority map[uint32]PacerPriority
	writerLock     sync.RWMutex

	pool *sync.Pool
}

// NewLeakyBucketPacer initializes a new LeakyBucketPacer.
func NewLeakyBucketPacer(initialBitrate int) *LeakyBucketPacer {
	return newLeakyBucketPacer(initialBitrate, logging.NewDefaultLoggerFactory())
}

func newLeakyBucketPacer(initialBitrate int, loggerFactory logging.LoggerFactory) *LeakyBucketPacer {
	pacer := &LeakyBucketPacer{
		log:            loggerFactory.NewLogger("pacer"),
		f:              1.5,
		targetBitrate:  initialBitrate,
		pacingInterval: 5 * time.Millisecond,
		qLock:          sync.RWMutex{},
		done:           make(chan struct{}),
		ssrcToWriter:   map[uint32]interceptor.RTPWriter{},
		ssrcToPriority: map[uint32]PacerPriority{},
		pool:           &sync.Pool{},
	}
	for i := range pacer.queues {
		pacer.queues[i] = list.New()
	}
	pacer.pool = &sync.Pool{
		New: func() any {
			b := make([]byte, 1460)

			return &b
		},
	}

	go pacer.Run()

	return pacer
}

// AddStream adds a new stream and its corresponding writer to the pacer
// at video priority.
func (p *LeakyBucketPacer) AddStream(ssrc uint32, writer interceptor.RTPWriter) {
	p.AddStreamWithPriority(ssrc, writer, PacerPriorityVideo)
}

// AddStreamWithPriority adds a stream whose packets are queued at the
// given priority.
func (p *LeakyBucketPacer) AddStreamWithPriority(ssrc uint32, writer interceptor.RTPWriter, priority PacerPriority) {
	if priority < 0 || priority >= pacerPriorities {
		priority = PacerPriorityVideo
	}
	p.writerLock.Lock()
	defer p.writerLock.Unlock()
	p.ssrcToWriter[ssrc] = writer
	p.ssrcToPriority[ssrc] = priority
}

// SetTargetBitrate updates the target bitrate at which the pacer is allowed to
// send packets. The pacer may exceed this limit by p.f.
func (p *LeakyBucketPacer) SetTargetBitrate(rate int) {
	p.targetBitrateLock.Lock()
	defer p.targetBitrateLock.Unlock()
	p.targetBitrate = int(p.f * float64(rate))
}

func (p *LeakyBucketPacer) getTargetBitrate() int {
	p.targetBitrateLock.Lock()
	defer p.targetBitrateLock.Unlock()

	return p.targetBitrate
}

// Write sends a packet with header and payload the a previously registered
// stream.
func (p *LeakyBucketPacer) Write(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
	buf, ok := p.pool.Get().(*[]byte)
	if !ok {
		return 0, errLeakyBucketPacerPoolCastFailed
	}

	copy(*buf, payload)
	hdr := header.Clone()

	p.writerLock.RLock()
	priority, known := p.ssrcToPriority[header.SSRC]
	p.writerLock.RUnlock()
	if !known {
		priority = PacerPriorityVideo
	}
	if probe, _ := attributes.Get(cc.ProbePacketAttributesKey).(bool); probe {
		priority = pacerProbeLane
	}
	p.qLock.Lock()
	p.queues[priority].PushBack(&item{
		header:     &hdr,
		payload:    buf,
		size:       len(payload),
		attributes: attributes,
	})
	p.qLock.Unlock()

	return header.MarshalSize() + len(payload), nil
}

// Run starts the LeakyBucketPacer.
func (p *LeakyBucketPacer) Run() {
	ticker := time.NewTicker(p.pacingInterval)
	defer ticker.Stop()

	lastSent := time.Now()
	for {
		select {
		case <-p.done:
			return
		case now := <-ticker.C:
			p.drainProbes()
			budget := int(float64(now.Sub(lastSent).Milliseconds()) * float64(p.getTargetBitrate()) / 8000.0)
			p.qLock.Lock()
			for budget > 0 {
				queue := p.frontQueue()
				if queue == nil {
					break
				}
				p.log.Infof("budget=%v, len(queue)=%v, targetBitrate=%v", budget, p.queued(), p.getTargetBitrate())
				next, ok := queue.Remove(queue.Front()).(*item)
				p.qLock.Unlock()
				if !ok {
					p.log.Warnf("failed to access leaky bucket pacer queue, cast failed")

					continue
				}

				p.writerLock.RLock()
				writer, ok := p.ssrcToWriter[next.header.SSRC]
				p.writerLock.RUnlock()
				if !ok {
					p.log.Warnf("no writer found for ssrc: %v", next.header.SSRC)
					p.pool.Put(next.payload)
					p.qLock.Lock()

					continue
				}

				n, err := writer.Write(next.header, (*next.payload)[:next.size], next.attributes)
				if err != nil {
					p.log.Errorf("failed to write packet: %v", err)
				}
				lastSent = now
				budget -= n

				p.pool.Put(next.payload)
				p.qLock.Lock()
			}
			p.qLock.Unlock()
		}
	}
}

// drainProbes sends every queued probe packet, outside the budget.
func (p *LeakyBucketPacer) drainProbes() {
	for {
		p.qLock.Lock()
		lane := p.queues[pacerProbeLane]
		if lane.Len() == 0 {
			p.qLock.Unlock()

			return
		}
		next, ok := lane.Remove(lane.Front()).(*item)
		p.qLock.Unlock()
		if !ok {
			continue
		}
		p.writerLock.RLock()
		writer, ok := p.ssrcToWriter[next.header.SSRC]
		p.writerLock.RUnlock()
		if ok {
			if _, err := writer.Write(next.header, (*next.payload)[:next.size], next.attributes); err != nil {
				p.log.Errorf("failed to write probe packet: %v", err)
			}
		}
		p.pool.Put(next.payload)
	}
}

// frontQueue is the highest-priority queue with a packet waiting, or nil.
// Caller holds qLock.
func (p *LeakyBucketPacer) frontQueue() *list.List {
	for _, q := range p.queues[:pacerPriorities] {
		if q.Len() != 0 {
			return q
		}
	}

	return nil
}

// queued is the number of packets waiting across all priorities. Caller
// holds qLock.
func (p *LeakyBucketPacer) queued() int {
	n := 0
	for _, q := range p.queues {
		n += q.Len()
	}

	return n
}

// Close closes the LeakyBucketPacer.
func (p *LeakyBucketPacer) Close() error {
	close(p.done)

	return nil
}
