package transport

import (
	"strconv"
	"time"
)

// handleSegment is the netem delivery callback.
func (c *Conn) handleSegment(pkt any) {
	seg, ok := pkt.(*Segment)
	if !ok || seg.Dst != c.Local {
		return
	}
	c.tracePacket(seg, "recv", "")

	switch {
	case seg.Flags.Has(RST):
		c.teardown("reset by peer")
		return
	case seg.Flags.Has(SYN) && c.state == stateSynRcvd:
		// Retransmitted SYN while half-open: resend the SYNACK. A SYNACK
		// arriving on the client (state syn-sent/established) falls
		// through to normal ACK processing.
		c.handleSyn(seg)
		return
	}

	c.processAck(seg)

	if len(seg.Payload) > 0 {
		c.processData(seg)
	}
	if seg.Flags.Has(FIN) {
		c.processFin(seg)
	}
}
func (c *Conn) handleSyn(seg *Segment) {
	// Retransmitted SYN while half-open: resend the SYNACK.
	if c.state == stateSynRcvd {
		c.emitAck(seg, "syn-ack-retransmit")
	}
}

// processAck updates the send side: cumulative ack, RTT sample,
// retransmit accounting and the peer's advertised window.
func (c *Conn) processAck(seg *Segment) {
	if !seg.Flags.Has(ACK) {
		return
	}
	c.peerWindow = seg.Window

	if seg.TSecr > 0 && c.rttSeen == false {
		c.rttSeen = true
	}
	if seg.TSecr > 0 {
		rtt := c.clk.Now() - nsToDur(seg.TSecr)
		if rtt > 0 {
			c.updateRTO(rtt)
		}
	}
	if c.state == stateInit || c.state == stateSynSent {
		// client side: SYNACK path
		if c.state == stateSynSent {
			c.state = stateEstab
			c.sndUna = seg.Ack
			c.sndNxt = seg.Ack
			c.rcvNxt = seg.Seq + 1
			c.clk.Cancel(c.rtoTimer)
			c.rtoTimer = nil
			c.traceState("established", c.Name+" connection established")
			// final ACK of the handshake, immediate
			c.sendRaw(&Segment{
				Src: c.Local, Dst: c.Peer, Seq: c.sndNxt,
				Ack: c.rcvNxt, Flags: ACK, Window: c.rcvWindow(),
				TSval: c.clk.Now().Nanoseconds(),
			}, "handshake-ack")
			if c.app != nil {
				c.app.OnConnected(c)
			}
		}
		return
	}
	if c.state == stateSynRcvd {
		// server side: the final ACK of the three-way handshake.
		c.state = stateEstab
		c.sndUna = seg.Ack
		c.sndNxt = seg.Ack
		c.peerWindow = seg.Window
		c.clk.Cancel(c.synackTimer)
		c.synackTimer = nil
		c.clk.Cancel(c.rtoTimer)
		c.rtoTimer = nil
		c.traceState("established", c.Name+" connection established")
		if c.app != nil {
			c.app.OnConnected(c)
		}
		c.deliverQueued()
		return
	}
	if seg.Ack <= c.sndUna {
		// duplicate or stale ACK
		if seg.Ack == c.sndUna && len(seg.Payload) == 0 {
			c.dupAcks++
			c.traceState("dup-ack", "duplicate ack #"+strconv.Itoa(c.dupAcks))
			if c.dupAcks == 3 {
				c.fastRetransmit()
			}
		}
		return
	}

	if seg.Ack > c.sndNxt {
		return // unknown future ack, ignore
	}

	// cumulative acknowledgement: drop fully-acked chunks
	acked := seg.Ack - c.sndUna
	c.dupAcks = 0
	consumed := uint32(0)
	for len(c.unacked) > 0 && consumed < acked {
		head := c.unacked[0]
		if consumed+uint32(len(head.data)) <= acked {
			consumed += uint32(len(head.data))
			c.unacked = c.unacked[1:]
		} else {
			head.data = head.data[acked-consumed:]
			consumed = acked
		}
	}
	c.sndUna = seg.Ack

	if len(c.unacked) == 0 {
		c.clk.Cancel(c.rtoTimer)
		c.rtoTimer = nil
		c.traceTimer("rto-cancel", "all data acknowledged")
	} else {
		c.armRTO(c.rto, false)
	}
	// ACK may have opened the window or cleared Nagle's unacked condition.
	c.pump()
}

// updateRTO follows RFC 6298's SRTT/RTTVAR estimator.
func (c *Conn) updateRTO(d time.Duration) {
	if c.srtt == 0 {
		c.srtt = d
		c.rttvar = d / 2
	} else {
		alpha := absDur(d - c.srtt)
		c.rttvar = (3*c.rttvar + alpha) / 4
		c.srtt = (7*c.srtt + d) / 8
	}
	rto := c.srtt + 4*c.rttvar
	if rto < c.opts.MinRTO {
		rto = c.opts.MinRTO
	}
	if rto > c.opts.MaxRTO {
		rto = c.opts.MaxRTO
	}
	c.rto = rto
}

func (c *Conn) processData(seg *Segment) {
	if seg.Seq < c.rcvNxt {
		// Retransmitted/duplicate byte range. Still (re)ack.
		c.traceState("dup-segment", "retransmitted bytes seq="+itoa(seg.Seq))
		c.scheduleAck(seg, false)
		return
	}
	if seg.Seq > c.rcvNxt {
		// gap: out of order — always ACK immediately and repeatedly
		c.ooo[seg.Seq] = append([]byte(nil), seg.Payload...)
		c.traceState("out-of-order",
			"gap detected: expected seq="+itoa(c.rcvNxt)+" got "+itoa(seg.Seq))
		c.scheduleAck(seg, true)
		return
	}

	// in-order delivery
	c.pktsSinceAck++
	c.acceptOrdered(seg.Payload)
	c.reassemble()
	c.scheduleAck(seg, false)
}

func (c *Conn) acceptOrdered(data []byte) {
	c.rcvNxt += uint32(len(data))
	if c.appPaused {
		c.appQueue = append(c.appQueue, append([]byte(nil), data...))
		c.recvHeld += len(data)
	} else {
		c.app.OnRead(c, append([]byte(nil), data...))
	}
}

func (c *Conn) reassemble() {
	for {
		data, ok := c.ooo[c.rcvNxt]
		if !ok {
			return
		}
		delete(c.ooo, c.rcvNxt)
		c.acceptOrdered(data)
	}
}

func (c *Conn) processFin(seg *Segment) {
	if seg.Seq+uint32(len(seg.Payload)) == c.rcvNxt && !c.peerFinSeen {
		c.peerFinSeen = true
		c.rcvNxt++
		c.traceState("peer-fin", "FIN received")
		c.scheduleAck(seg, true)
		if c.state != stateFinWait {
			c.teardownMaybe()
		}
	}
}

// PauseRead / ResumeRead model a slow or stalled application: while
// paused, data is held locally so the advertised window shrinks to zero.
func (c *Conn) PauseRead() {
	c.appPaused = true
	c.traceState("app-paused", "application stopped reading; window will fill")
}

func (c *Conn) ResumeRead() {
	c.appPaused = false
	c.traceState("app-resumed", "application drains receive buffer")
	c.deliverQueued()
	// Window reopens: tell the peer, immediately if no ACK is pending.
	if c.pendingAck {
		c.flushDelayedAck("window reopened by ResumeRead")
	} else {
		c.emitPureAck("window update after ResumeRead")
	}
	c.pump()
}

func (c *Conn) deliverQueued() {
	if c.appPaused || len(c.appQueue) == 0 {
		return
	}
	q := c.appQueue
	c.appQueue = nil
	c.recvHeld = 0
	for _, data := range q {
		c.app.OnRead(c, data)
	}
}

// rcvWindow is what this endpoint can still buffer.
func (c *Conn) rcvWindow() uint32 {
	w := c.opts.ReceiveBuffer - c.recvHeld
	for _, d := range c.ooo {
		w -= len(d)
	}
	if w < 0 {
		return 0
	}
	return uint32(w)
}

// scheduleAck implements the receiver's acknowledgement policy.
//
//	immediate=true forces an instant ACK (out-of-order data, FIN,
//	window reopening). Otherwise the delayed-ACK timer, the second-packet
//	rule, quick-ACK mode and piggybacking decide.
func (c *Conn) scheduleAck(seg *Segment, immediate bool) {
	// Any data we ourselves send takes the ACK along for free.
	if immediate {
		c.emitAck(seg, "immediate: "+c.whyImmediate(seg))
		return
	}
	if c.quickLeft > 0 {
		c.quickLeft--
		c.emitAck(seg, "quick-ack mode (TCP_QUICKACK)")
		return
	}
	// Sustained quick mode: while the option stays on, every in-order
	// segment is acknowledged immediately. Real Linux TCP_QUICKACK is a
	// one-shot hint (the kernel silently reverts to delayed ACK after a
	// few packets); QuickAck=true with QuickAckCount==0 models an
	// application that keeps re-arming it, or a host with delayed ACK
	// disabled.
	if c.opts.QuickAck && c.opts.QuickAckCount == 0 {
		c.emitAck(seg, "quick-ack mode (sustained TCP_QUICKACK)")
		return
	}
	if !c.opts.DelayAck {
		c.emitAck(seg, "immediate: DelayAck disabled")
		return
	}
	// RFC 1122: ACK at least every second full-sized (here: every second
	// data) segment.
	if c.opts.AckAfterPackets > 0 && c.pktsSinceAck >= c.opts.AckAfterPackets {
		c.emitAck(seg, "immediate: AckAfterPackets reached")
		return
	}
	// Spontaneous kernel quick-ACK re-arm: deterministic per-connection
	// RNG so the same scenario always produces the same tail pattern.
	if c.opts.QuickAckRearmRate > 0 &&
		c.ackRand.Float64() < c.opts.QuickAckRearmRate {
		c.emitAck(seg, "immediate: kernel quick-ACK re-arm (idle path heuristic)")
		return
	}
	if !c.pendingAck {
		c.pendingAck = true
		c.delackTimer = c.clk.After(c.opts.DelayAckTimeout, func(time.Duration) {
			c.delackTimer = nil
			if c.pendingAck {
				c.pendingAck = false
				c.pktsSinceAck = 0
				c.emitPureAck("delayed-ACK timer fired after " + c.opts.DelayAckTimeout.String())
			}
		})
		c.traceTimer("delack-arm",
			"pure ACK held for up to "+c.opts.DelayAckTimeout.String())
	}
}

func (c *Conn) whyImmediate(seg *Segment) string {
	if seg.Flags.Has(FIN) {
		return "FIN must be acknowledged at once"
	}
	if seg.Seq > c.rcvNxt || len(c.ooo) > 0 {
		return "out-of-order segment: duplicate ACK for fast retransmit"
	}
	return "receiver policy"
}

func (c *Conn) emitAck(seg *Segment, why string) {
	c.clk.Cancel(c.delackTimer)
	c.delackTimer = nil
	c.pendingAck = false
	c.pktsSinceAck = 0
	out := &Segment{
		Src: c.Local, Dst: c.Peer, Seq: c.sndNxt,
		Ack: c.rcvNxt, Flags: ACK, Window: c.rcvWindow(),
		TSval: c.clk.Now().Nanoseconds(),
		TSecr: seg.TSval,
	}
	c.sendRaw(out, "ack: "+why)
}

func (c *Conn) emitPureAck(why string) {
	out := &Segment{
		Src: c.Local, Dst: c.Peer, Seq: c.sndNxt,
		Ack: c.rcvNxt, Flags: ACK, Window: c.rcvWindow(),
		TSval: c.clk.Now().Nanoseconds(),
	}
	c.sendRaw(out, "ack: "+why)
}

func (c *Conn) flushDelayedAck(why string) {
	c.clk.Cancel(c.delackTimer)
	c.delackTimer = nil
	if c.pendingAck {
		c.pendingAck = false
		c.pktsSinceAck = 0
		c.emitPureAck(why)
	}
}

// consumePendingAck is called when outgoing data piggybacks the ACK.
func (c *Conn) consumePendingAck(why string) {
	c.clk.Cancel(c.delackTimer)
	c.delackTimer = nil
	c.pendingAck = false
	c.pktsSinceAck = 0
	c.traceState("piggyback", "held ACK carried by outgoing data ("+why+")")
}

func (c *Conn) teardown(why string) {
	c.traceState("closed", why)
	c.state = stateClosed
	c.clk.Cancel(c.rtoTimer)
	c.clk.Cancel(c.delackTimer)
	c.clk.Cancel(c.corkTimer)
	if c.app != nil {
		c.app.OnClose(c)
	}
}

func (c *Conn) teardownMaybe() {
	if c.peerFinSeen && c.state == stateFinWait {
		c.teardown("both sides closed")
	}
}
