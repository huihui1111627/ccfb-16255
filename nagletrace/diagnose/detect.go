package diagnose

import (
	"sort"
	"time"

	"nagletrace/transport"
)

// minStall is the smallest gap worth reporting as a mechanism-induced
// wait rather than normal propagation (the simulated OWD is ~0.5ms).
const minStall = 5 * time.Millisecond

// owdTolerance bounds how tightly the next data must follow the ACK
// that unblocked it; one one-way delay plus a little scheduling slack.
const owdTolerance = 3 * time.Millisecond

// findStalls reconstructs, purely from packet order and timing, every
// interval in which a sender went idle with unacked small bytes and was
// released by an incoming ACK. It then splits them into:
//
//   - nagle-delayed-ack: the sender already had a second small segment
//     buffered when it went idle (two app writes, one wire segment), so
//     Nagle and the delayed-ACK timer are mutually waiting;
//   - delayed-ack-only: nothing else was buffered — a stop-and-wait
//     boundary — so only the receiver's timer is in play;
//   - ack-piggybacked: the releasing ACK rides reverse data.
func findNagleDelayedAck(tr *transport.Trace, pkts []pkt, mss int) []WaitEvent {
	var waits []WaitEvent
	for _, dir := range []struct{ sender, receiver string }{
		{"client", "server"}, {"server", "client"},
	} {
		var dataPkts []pkt
		for _, p := range pkts {
			if p.src == dir.sender && p.isData {
				dataPkts = append(dataPkts, p)
			}
		}
		var fromReceiver []pkt
		for _, p := range pkts {
			if p.src == dir.receiver {
				fromReceiver = append(fromReceiver, p)
			}
		}

		for i := 0; i+1 < len(dataPkts); i++ {
			cur, next := dataPkts[i], dataPkts[i+1]
			gap := next.ev.Time - cur.ev.Time
			if gap < minStall {
				continue
			}
			curEnd := cur.seq + uint32(cur.ev.PayloadLen)
			// Find the ACK from the receiver that covers cur and arrives
			// within the idle gap (before next is sent).
			var ackPkt *pkt
			for j := range fromReceiver {
				r := fromReceiver[j]
				if r.ev.Time >= next.ev.Time {
					break
				}
				if r.ack >= curEnd && r.ev.Time > cur.ev.Time {
					ackPkt = &fromReceiver[j]
				}
			}
			if ackPkt == nil {
				continue // released by something else, handled elsewhere
			}
			// Nagle only applies to sub-MSS (small) segments; a full-MSS
			// segment is always allowed to fly.
			if cur.ev.PayloadLen >= mss {
				continue
			}
			// The unblock must actually be the ACK: the next segment has
			// to follow it within roughly one OWD. If the ACK arrived
			// long before the next send, the application was the one
			// waiting, not the transport.
			follow := next.ev.Time - ackPkt.ev.Time
			if follow > owdTolerance {
				continue
			}
			// The ACK must itself have been delayed: it shows up near the
			// end of the gap, not within one OWD of the data it acks.
			ackDelay := ackPkt.ev.Time - cur.ev.Time
			if ackDelay < minStall {
				continue
			}

			// Nagle evidence: was a small packet actually held on this
			// sender during the idle window? The packet-level signal is a
			// small, contiguous next segment; the transport's own
			// nagle-hold events confirm the attribution. A stall with no
			// hold event is a pure delayed-ACK wait at a stop-and-wait
			// boundary.
			nagleHeld := hadNagleHold(tr, dir.sender, cur.ev.Time, ackPkt.ev.Time)
			nagleSuspect := nagleHeld || (next.ev.PayloadLen < mss &&
				next.seq == curEnd)

			blocked := WaitEvent{
				Who:        dir.sender,
				Start:      cur.ev.Time,
				End:        ackPkt.ev.Time,
				Duration:   ackPkt.ev.Time - cur.ev.Time,
				BlockedSeq: next.seq,
				Evidence: []string{
					"small data segment seq=" + u32(cur.seq) + " (" + itoa(cur.ev.PayloadLen) + "B) sent at " + cur.ev.Time.String(),
					"no further segment from " + dir.sender + " for " + gap.String(),
					"peer ACK seq=" + u32(ackPkt.ack) + " arrives at " + ackPkt.ev.Time.String() + " (" + ackDelay.String() + " after the data)",
					"next data segment follows " + follow.String() + " later, starting at seq=" + u32(next.seq),
				},
			}

			if ackPkt.isData {
				blocked.Type = WaitPiggyback
				blocked.WaitingFor = "ACK for the held small packet; it was carried by the peer's own data segment"
				blocked.UnblockPkt = dir.receiver + " data+ACK seq=" + u32(ackPkt.seq)
				blocked.CausedBy = dir.receiver + " application happened to send data during the window"
				blocked.Suggestion = "piggybacking hides the stall only while the peer talks first; do not rely on it — apply the same fixes as for the delayed-ACK stall."
			} else if nagleHeld {
				blocked.Type = WaitNagleDelayedACK
				blocked.WaitingFor = "ACK of the in-flight small packet (Nagle forbids a second small packet while any byte is unacked)"
				blocked.UnblockPkt = dir.receiver + " pure ACK seq=" + u32(ackPkt.ack)
				blocked.CausedBy = dir.sender + " Nagle (TCP_NODELAY off) waiting on " + dir.receiver + " delayed-ACK timer (TCP_QUICKACK off)"
				blocked.Suggestion = "either set TCP_NODELAY on " + dir.sender +
					", merge the two writes into one writev/sendmmsg, or enable TCP_QUICKACK / reduce the delayed-ACK timer on " + dir.receiver
				blocked.Evidence = append(blocked.Evidence,
					"transport recorded a Nagle hold on "+dir.sender+" during the idle window — a second small packet was buffered and blocked")
			} else if nagleSuspect {
				blocked.Type = WaitNagleDelayedACK
				blocked.WaitingFor = "ACK of the in-flight small packet (Nagle forbids a second small packet while any byte is unacked)"
				blocked.UnblockPkt = dir.receiver + " pure ACK seq=" + u32(ackPkt.ack)
				blocked.CausedBy = dir.sender + " Nagle (TCP_NODELAY off) waiting on " + dir.receiver + " delayed-ACK timer (TCP_QUICKACK off)"
				blocked.Suggestion = "either set TCP_NODELAY on " + dir.sender +
					", merge the two writes into one writev/sendmmsg, or enable TCP_QUICKACK / reduce the delayed-ACK timer on " + dir.receiver
				blocked.Evidence = append(blocked.Evidence,
					"the queued next segment is small and contiguous: a second small packet was already buffered when the sender went idle — Nagle held it")
			} else {
				blocked.Type = WaitDelayedACK
				blocked.WaitingFor = "the receiver's delayed-ACK timer; the sender had no second packet queued (stop-and-wait boundary)"
				blocked.UnblockPkt = dir.receiver + " pure ACK seq=" + u32(ackPkt.ack)
				blocked.CausedBy = dir.receiver + " delayed-ACK timer alone (the sender's Nagle was not blocking anything)"
				blocked.Suggestion = "enable TCP_QUICKACK or lower the delayed-ACK timer on " + dir.receiver +
					"; pipelining requests also hides it because the every-second-packet rule then ACKs immediately"
			}
			waits = append(waits, blocked)
		}
	}
	return waits
}

// findRetransmitWaits flags every retransmission seen on the wire and
// tells RTO (single duplicate-less timeout) from fast retransmit
// (receiver kept emitting duplicate ACKs after a gap).
func findRetransmitWaits(pkts []pkt) []WaitEvent {
	var waits []WaitEvent
	// rebuild first-transmission and all-transmission times per sender+seq
	first := map[string]map[uint32]time.Duration{}
	allTx := map[string]map[uint32][]time.Duration{}
	for _, p := range pkts {
		if !p.isData {
			continue
		}
		if first[p.src] == nil {
			first[p.src] = map[uint32]time.Duration{}
			allTx[p.src] = map[uint32][]time.Duration{}
		}
		if _, ok := first[p.src][p.seq]; !ok {
			first[p.src][p.seq] = p.ev.Time
		}
		allTx[p.src][p.seq] = append(allTx[p.src][p.seq], p.ev.Time)
	}
	// One verdict per lost segment: from first transmission to the first
	// successful re-send that gets acknowledged (approximated by the last
	// observed transmission of that seq).
	reported := map[string]map[uint32]bool{}
	for _, p := range pkts {
		if !p.isData {
			continue
		}
		if reported[p.src] == nil {
			reported[p.src] = map[uint32]bool{}
		}
		t0 := first[p.src][p.seq]
		txList := allTx[p.src][p.seq]
		lastTx := txList[len(txList)-1]
		// report at the last transmission for this seq, once
		if p.ev.Time != lastTx || reported[p.src][p.seq] {
			continue
		}
		reported[p.src][p.seq] = true
		if lastTx-t0 < minStall {
			continue // link-level duplicate, not a loss recovery
		}
		dupCount := countDupAcksBetween(pkts, p.src, t0, lastTx)
		w := WaitEvent{
			Who:        p.src,
			Start:      t0,
			End:        lastTx,
			Duration:   lastTx - t0,
			BlockedSeq: p.seq,
			Evidence: []string{
				"seq=" + u32(p.seq) + " first sent at " + t0.String(),
				"retransmitted " + itoa(len(txList)-1) + " time(s), last at " + lastTx.String(),
			},
		}
		if dupCount >= 3 {
			w.Type = WaitFastRetransmit
			w.WaitingFor = "three duplicate ACKs after a sequence gap"
			w.CausedBy = "packet loss on the path; fast retransmit repaired it without waiting for the RTO"
			w.Suggestion = "fast retransmit worked as designed; investigate the loss rate on the path"
			w.Evidence = append(w.Evidence, itoa(dupCount)+" duplicate ACKs observed before recovery")
		} else {
			w.Type = WaitRTO
			w.WaitingFor = "the retransmission (RTO) timer — no ACK ever acknowledged the segment"
			w.CausedBy = "packet loss on the path; the retransmit timer had to expire (delayed ACKs can push the effective wait toward ~200ms+)"
			w.Suggestion = "reduce path loss; note that with delayed ACK on the receiver, a lost single packet can also delay the ACK that would otherwise trigger fast retransmit"
			w.Evidence = append(w.Evidence, "only "+itoa(dupCount)+" duplicate ACKs before recovery, so the RTO timer fired")
		}
		waits = append(waits, w)
	}
	return waits
}

// countDupAcksBetween counts pure ACKs from the peer of sender with the
// same ack number, within (t0, t1).
func countDupAcksBetween(pkts []pkt, sender string, t0, t1 time.Duration) int {
	peer := other(sender)
	ackSeen := map[uint32]int{}
	for _, p := range pkts {
		if p.src != peer || !p.isPureAck || p.ev.Time <= t0 || p.ev.Time >= t1 {
			continue
		}
		ackSeen[p.ack]++
	}
	best := 0
	for _, n := range ackSeen {
		if n > best {
			best = n
		}
	}
	return best
}

// findWindowWaits detects a sender going idle right after advertising a
// near-zero window from the other side, then resuming after a window
// update.
func findWindowWaits(pkts []pkt) []WaitEvent {
	var waits []WaitEvent
	for _, p := range pkts {
		if p.isPureAck && p.ev.Window == 0 {
			// find the next data from the peer of p.src
			peer := other(p.src)
			for _, q := range pkts {
				if q.src == peer && q.isData && q.ev.Time > p.ev.Time {
					gap := q.ev.Time - p.ev.Time
					if gap >= minStall {
						waits = append(waits, WaitEvent{
							Type:       WaitWindow,
							Who:        peer,
							Start:      p.ev.Time,
							End:        q.ev.Time,
							Duration:   gap,
							WaitingFor: "the receiver's receive buffer to drain (zero-window)",
							UnblockPkt: peer + " data after window update",
							CausedBy:   p.src + " application reading too slowly / receive buffer too small",
							Suggestion: "speed up the reading application or enlarge the receive buffer; check for application-level backpressure",
							Evidence: []string{
								p.src + " advertised window=0 at " + p.ev.Time.String(),
								peer + " paused for " + gap.String(),
							},
						})
					}
					break
				}
			}
		}
	}
	return waits
}

// findCorkWaits uses the cork state events (ground truth) to label
// intervals where bytes were held by TCP_CORK.
func findCorkWaits(tr *transport.Trace, pkts []pkt) []WaitEvent {
	var waits []WaitEvent
	type span struct {
		start, end time.Duration
		who        string
	}
	var spans []span
	open := map[string]time.Duration{}
	for _, ev := range tr.Events {
		if ev.Name != "cork" {
			continue
		}
		if ev.Detail == "on" {
			open[ev.Endpoint] = ev.Time
		} else if ev.Detail == "flushed" {
			if t0, ok := open[ev.Endpoint]; ok {
				spans = append(spans, span{t0, ev.Time, ev.Endpoint})
				delete(open, ev.Endpoint)
			}
		}
	}
	for _, s := range spans {
		if s.end-s.start < minStall {
			continue
		}
		waits = append(waits, WaitEvent{
			Type:       WaitCork,
			Who:        s.who,
			Start:      s.start,
			End:        s.end,
			Duration:   s.end - s.start,
			WaitingFor: "explicit TCP_CORK flush (or the cork auto-timeout)",
			CausedBy:   s.who + " application enabled TCP_CORK and delayed the flush",
			Suggestion: "uncork promptly after the body is buffered; remember the kernel auto-flush is ~200ms, which becomes a latency floor if forgotten",
			Evidence: []string{
				"TCP_CORK engaged at " + s.start.String(),
				"flush at " + s.end.String() + " (" + (s.end - s.start).String() + " later)",
			},
		})
	}
	return waits
}

// hadNagleHold checks the transport's internal state events for a
// Nagle hold by endpoint within [start, end]. This is the attribution
// cross-check; the existence and timing of the stall itself come from
// packets only.
func hadNagleHold(tr *transport.Trace, endpoint string, start, end time.Duration) bool {
	for _, ev := range tr.Events {
		if ev.Kind == transport.KindState && ev.Name == "nagle-hold" &&
			ev.Endpoint == endpoint && ev.Time >= start && ev.Time <= end {
			return true
		}
	}
	return false
}

func other(endpoint string) string {
	if endpoint == "client" {
		return "server"
	}
	return "client"
}

func sortWaits(ws []WaitEvent) {
	sort.SliceStable(ws, func(i, j int) bool {
		if ws[i].Start != ws[j].Start {
			return ws[i].Start < ws[j].Start
		}
		return ws[i].End < ws[j].End
	})
}

func u32(v uint32) string { return itoa(int(v)) }

func itoa(v int) string {
	return formatInt(int64(v))
}
