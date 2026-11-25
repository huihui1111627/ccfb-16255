package transport

import "time"

// pendingBytes is everything the sender is on the hook for.
func (c *Conn) pendingBytes() int {
	n := 0
	for _, ch := range c.unsent {
		n += len(ch.data)
	}
	for _, ch := range c.unacked {
		n += len(ch.data)
	}
	n += len(c.corkBuf)
	return n
}

// Write hands one application message to the transport. Two successive
// Writes stay two distinct chunks; whether they end up in one segment
// depends on Nagle / window state at pump time.
func (c *Conn) Write(b []byte) (int, error) {
	if c.state != stateEstab {
		return 0, ErrNotConnected
	}
	if c.pendingBytes()+len(b) > c.opts.SendBuffer {
		return 0, ErrWouldBlock
	}
	if c.corked {
		c.corkBuf = append(c.corkBuf, b...)
		c.armCorkTimer()
		return len(b), nil
	}
	cp := append([]byte(nil), b...)
	c.unsent = append(c.unsent, &chunk{data: cp})
	c.traceWrite(len(b), false)
	c.pump()
	return len(b), nil
}

// WriteMulti is the writev / sendmmsg equivalent: the application hands
// several pieces at once and the transport treats them as one merged
// write, so a header + body handed over together never hits Nagle as a
// second small packet.
func (c *Conn) WriteMulti(parts ...[]byte) (int, error) {
	if c.state != stateEstab {
		return 0, ErrNotConnected
	}
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	if c.pendingBytes()+total > c.opts.SendBuffer {
		return 0, ErrWouldBlock
	}
	if c.corked {
		for _, p := range parts {
			c.corkBuf = append(c.corkBuf, p...)
		}
		c.armCorkTimer()
		return total, nil
	}
	merged := make([]byte, 0, total)
	for _, p := range parts {
		merged = append(merged, p...)
	}
	c.unsent = append(c.unsent, &chunk{data: merged})
	c.traceWrite(total, true)
	c.pump()
	return total, nil
}

// Cork enables TCP_CORK: subsequent writes accumulate until Flush.
func (c *Conn) Cork() {
	c.corked = true
	c.traceState("cork", "on")
}

// Uncork flushes accumulated data and disables corking.
func (c *Conn) Uncork() error {
	if !c.corked {
		return nil
	}
	c.clk.Cancel(c.corkTimer)
	c.corkTimer = nil
	c.corked = false
	if len(c.corkBuf) > 0 {
		c.unsent = append(c.unsent, &chunk{data: c.corkBuf})
		c.corkBuf = nil
	}
	c.traceState("cork", "flushed")
	c.pump()
	return nil
}

// Flush is an explicit flush that works under both cork and Nagle.
func (c *Conn) Flush() error { return c.Uncork() }

func (c *Conn) armCorkTimer() {
	if c.corkTimer != nil || c.opts.CorkTimeout <= 0 {
		return
	}
	c.corkTimer = c.clk.After(c.opts.CorkTimeout, func(time.Duration) {
		c.corkTimer = nil
		c.traceTimer("cork-timeout", "auto-flush after CorkTimeout")
		_ = c.Uncork()
	})
}

// pump is the single place that moves unsent bytes onto the wire.
// It enforces, in order: cork, flow/congestion window, then Nagle.
func (c *Conn) pump() {
	if c.state != stateEstab && c.state != stateFinWait {
		return
	}
	heldLogged := false
	for len(c.unsent) > 0 {
		if c.corked {
			if !heldLogged {
				c.traceHold("cork-hold", "data held: TCP_CORK set")
			}
			return
		}
		flight := int(c.sndNxt - c.sndUna)
		rwndAvail := int(c.peerWindow) - flight
		cwndAvail := c.opts.InitialCwnd*c.opts.MSS - flight
		win := rwndAvail
		if cwndAvail < win {
			win = cwndAvail
		}
		if win <= 0 {
			if rwndAvail <= 0 {
				c.traceHold("window-hold", "sender blocked: peer receive window full")
			} else {
				c.traceHold("cwnd-hold", "sender blocked: congestion window full")
			}
			return
		}

		head := c.unsent[0]
		full := len(head.data) >= c.opts.MSS
		// Nagle: a small segment must wait while any byte is unacked,
		// unless TCP_NODELAY is set or the segment fills the MSS.
		if !c.opts.NoDelay && flight > 0 && !full {
			c.traceHold("nagle-hold",
				"small packet held by Nagle: unacked bytes in flight, NoDelay=false")
			return
		}

		n := c.opts.MSS
		if n > len(head.data) {
			n = len(head.data)
		}
		if n > win {
			n = win
		}
		payload := head.data[:n]
		seg := c.dataSegment(payload)
		c.transmit(seg, head)

		head.data = head.data[n:]
		if len(head.data) == 0 {
			c.unsent = c.unsent[1:]
		}
		heldLogged = false
	}
}

func (c *Conn) dataSegment(payload []byte) *Segment {
	seg := &Segment{
		Src:     c.Local,
		Dst:     c.Peer,
		Seq:     c.sndNxt,
		Flags:   PSH,
		Window:  c.rcvWindow(),
		Payload: append([]byte(nil), payload...),
		TSval:   c.clk.Now().Nanoseconds(),
	}
	// Piggyback: any ACK the receiver was holding rides out for free.
	if c.pendingAck {
		seg.Flags |= ACK
		seg.Ack = c.rcvNxt
		c.consumePendingAck("piggyback on data segment")
	}
	return seg
}

// transmit emits a data segment and bookkeeps the unacked queue + RTO.
func (c *Conn) transmit(seg *Segment, ch *chunk) {
	now := c.clk.Now()
	if ch != nil {
		ch.txCount++
		ch.lastTx = now
		if ch.txCount == 1 {
			ch.firstTx = now
		}
		c.unacked = append(c.unacked, &chunk{data: seg.Payload, txCount: ch.txCount, firstTx: ch.firstTx, lastTx: now})
	}
	c.sndNxt += uint32(len(seg.Payload))
	c.sendRaw(seg, "data")
	if c.rtoTimer == nil && len(c.unacked) > 0 {
		c.armRTO(c.rto, false)
	}
}

// armRTO (re)starts the retransmission timer. backoff=true is used after
// a timer expiry or for the SYN, where the timeout doubles.
func (c *Conn) armRTO(d time.Duration, backoff bool) {
	c.clk.Cancel(c.rtoTimer)
	if backoff && c.rtoTimer != nil {
		d *= 2
	}
	if d > c.opts.MaxRTO {
		d = c.opts.MaxRTO
	}
	if d < c.opts.MinRTO {
		d = c.opts.MinRTO
	}
	c.rto = d
	c.traceTimer("rto-arm", "retransmit timer armed for "+d.String())
	c.rtoTimer = c.clk.After(d, func(time.Duration) {
		c.rtoTimer = nil
		c.onRTO()
	})
}

func (c *Conn) onRTO() {
	if len(c.unacked) == 0 {
		return
	}
	c.dupAcks = 0
	old := c.rto
	c.rto = old * 2
	if c.rto > c.opts.MaxRTO {
		c.rto = c.opts.MaxRTO
	}
	head := c.unacked[0]
	n := c.opts.MSS
	if n > len(head.data) {
		n = len(head.data)
	}
	seg := c.dataSegment(head.data[:n])
	seg.Seq = c.sndUna
	head.txCount++
	head.lastTx = c.clk.Now()
	c.traceTimer("rto-fire", "timeout after "+old.String()+", retransmitting seq="+itoa(seg.Seq))
	c.sendRaw(seg, "retransmit")
	c.armRTO(c.rto, false)
}

// fastRetransmit fires on the third duplicate ACK.
func (c *Conn) fastRetransmit() {
	if len(c.unacked) == 0 {
		return
	}
	head := c.unacked[0]
	n := c.opts.MSS
	if n > len(head.data) {
		n = len(head.data)
	}
	seg := c.dataSegment(head.data[:n])
	seg.Seq = c.sndUna
	head.txCount++
	head.lastTx = c.clk.Now()
	c.traceTimer("fast-retransmit", "3 dup ACKs, retransmitting seq="+itoa(seg.Seq))
	c.sendRaw(seg, "retransmit")
}

// Close drains the send queue then emits a FIN.
func (c *Conn) Close() error {
	if c.state == stateClosed {
		return ErrClosed
	}
	c.pump()
	if len(c.unsent) > 0 {
		// Caller can close again after data has drained; keep it simple
		// by scheduling a retry.
		c.clk.After(1, func(time.Duration) { _ = c.Close() })
		return nil
	}
	c.state = stateFinWait
	seg := &Segment{Src: c.Local, Dst: c.Peer, Seq: c.sndNxt, Flags: FIN, Window: c.rcvWindow(), TSval: c.clk.Now().Nanoseconds()}
	if c.pendingAck {
		seg.Flags |= ACK
		seg.Ack = c.rcvNxt
		c.consumePendingAck("piggyback on FIN")
	}
	c.sndNxt++
	c.sendRaw(seg, "fin")
	if c.rtoTimer == nil {
		c.armRTO(c.rto, false)
	}
	return nil
}
