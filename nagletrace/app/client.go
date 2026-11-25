package app

import (
	"time"

	"nagletrace/clock"
	"nagletrace/transport"
)

// WritePattern is how the client pushes one request's bytes.
type WritePattern string

const (
	// WriteSplit mirrors the buggy framework: header and body in two
	// separate Write calls, close together in time.
	WriteSplit WritePattern = "split-write"
	// WriteMerged is the writev/sendmmsg style: one syscall, both parts.
	WriteMerged WritePattern = "writev"
	// WriteCorked accumulates under TCP_CORK and flushes explicitly.
	WriteCorked WritePattern = "corked"
)

// Interaction controls whether requests overlap on the wire.
type Interaction string

const (
	// StopAndWait is classic request/response: one outstanding request.
	StopAndWait Interaction = "request-response"
	// Pipeline fires every request without waiting for responses.
	Pipeline Interaction = "pipeline"
)

// ClientConfig describes the client workload.
type ClientConfig struct {
	Pattern  WritePattern
	Mode     Interaction
	Requests int
	BodySize int
	// MixedSplitFraction > 0 makes a deterministic fraction of requests
	// use WriteSplit even when Pattern is WriteMerged — this reproduces
	// the "90% fast, 10% stuck at 40ms" production shape.
	MixedSplitFraction float64
}

// Sample is one completed request/response round trip.
type Sample struct {
	RequestID int
	Start     time.Duration
	End       time.Duration
	Latency   time.Duration
	Split     bool // this request used the split write
}

// Latency collects per-request latencies (single-goroutine, no locks).
type Latency struct {
	Samples []Sample
}

// Stats summarizes the latency distribution.
type Stats struct {
	Count        int           `json:"count"`
	Min          time.Duration `json:"min"`
	P50          time.Duration `json:"p50"`
	P90          time.Duration `json:"p90"`
	P99          time.Duration `json:"p99"`
	Max          time.Duration `json:"max"`
	Mean         time.Duration `json:"mean"`
	TailCount    int           `json:"tailCount"`
	TailFraction float64       `json:"tailFraction"`
}

// ClientApp drives requests over one connection.
type ClientApp struct {
	clk  *clock.Clock
	cfg  ClientConfig
	lat  *Latency
	conn *transport.Conn
	rd   Reader

	start      map[int]time.Duration
	split      map[int]bool
	nextID     int
	completed  int
	onComplete func()
}

func NewClient(clk *clock.Clock, cfg ClientConfig, lat *Latency, onComplete func()) *ClientApp {
	return &ClientApp{clk: clk, cfg: cfg, lat: lat, start: map[int]time.Duration{},
		split: map[int]bool{}, onComplete: onComplete}
}

func (a *ClientApp) OnConnected(c *transport.Conn) {
	a.conn = c
	if a.cfg.Mode == Pipeline {
		for i := 0; i < a.cfg.Requests; i++ {
			a.fire(i)
		}
		return
	}
	a.fire(0)
}

func (a *ClientApp) usesSplit(id int) bool {
	if a.cfg.Pattern == WriteSplit {
		return true
	}
	if a.cfg.MixedSplitFraction > 0 {
		// deterministic, RNG-free: every k-th request is split. Skip
		// request 0 so the very first request of a fresh connection
		// (which is warm in real services) follows the fast path.
		k := int(1.0 / a.cfg.MixedSplitFraction)
		return k > 0 && id > 0 && id%k == 0
	}
	return false
}

func (a *ClientApp) fire(id int) {
	header, body := encodeRequest(id, a.cfg.BodySize)
	split := a.usesSplit(id)
	a.start[id] = a.clk.Now()
	a.split[id] = split

	switch {
	case a.cfg.Pattern == WriteCorked:
		a.conn.Cork()
		_, _ = a.conn.Write(header)
		_, _ = a.conn.Write(body)
		_ = a.conn.Uncork()
	case split:
		_, _ = a.conn.Write(header)
		_, _ = a.conn.Write(body)
	default:
		_, _ = a.conn.WriteMulti(header, body)
	}
	a.nextID = id + 1
}

func (a *ClientApp) OnRead(c *transport.Conn, b []byte) {
	a.rd.Push(b)
	for {
		id, _, ok := a.rd.NextResponse()
		if !ok {
			return
		}
		end := a.clk.Now()
		start := a.start[id]
		a.lat.Samples = append(a.lat.Samples, Sample{
			RequestID: id, Start: start, End: end,
			Latency: end - start, Split: a.split[id],
		})
		a.completed++
		if a.onComplete != nil {
			a.onComplete()
		}
		if a.cfg.Mode == StopAndWait && a.completed < a.cfg.Requests {
			a.fire(a.nextID)
		}
	}
}

func (a *ClientApp) OnClose(*transport.Conn) {}

// Done reports whether every requested response has arrived.
func (a *ClientApp) Done() bool { return a.completed >= a.cfg.Requests }
