package transport

import (
	"strconv"
	"time"
)

// sendRaw puts a segment on the (possibly lossy) link and records it
// in the packet trace — the same record a real sniffer would produce.
func (c *Conn) sendRaw(seg *Segment, note string) {
	c.tracePacket(seg, "send", note)
	c.net.Send(seg.Src, seg.Dst, seg)
}

func (c *Conn) tracePacket(seg *Segment, dir, note string) {
	c.trace.add(&TraceEvent{
		Time:       c.clk.Now(),
		Kind:       KindPacket,
		Name:       "segment",
		Endpoint:   c.Name,
		Direction:  dir,
		Seq:        seg.Seq,
		Ack:        seg.Ack,
		Flags:      seg.Flags,
		PayloadLen: len(seg.Payload),
		Window:     seg.Window,
		Detail:     note,
	})
}

func (c *Conn) traceState(name, detail string) {
	c.trace.add(&TraceEvent{
		Time: c.clk.Now(), Kind: KindState, Name: name,
		Endpoint: c.Name, Detail: detail,
	})
}

func (c *Conn) traceTimer(name, detail string) {
	c.trace.add(&TraceEvent{
		Time: c.clk.Now(), Kind: KindTimer, Name: name,
		Endpoint: c.Name, Detail: detail,
	})
}

func (c *Conn) traceHold(name, detail string) {
	c.trace.add(&TraceEvent{
		Time: c.clk.Now(), Kind: KindState, Name: name,
		Endpoint: c.Name, Detail: detail,
	})
}

func (c *Conn) traceWrite(n int, merged bool) {
	name := "app-write"
	if merged {
		name = "app-writev"
	}
	c.trace.add(&TraceEvent{
		Time: c.clk.Now(), Kind: KindState, Name: name,
		Endpoint: c.Name, Detail: strconv.Itoa(n) + " bytes",
	})
}

func itoa(u uint32) string { return strconv.FormatUint(uint64(u), 10) }

func nsToDur(ns int64) time.Duration { return time.Duration(ns) }

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
