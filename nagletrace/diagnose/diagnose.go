// Package diagnose reads a packet timeline the way a sniffer would and
// decides, from packets alone, what the two endpoints were waiting for,
// which packet unblocked them, how long the wait lasted and which
// endpoint's setting caused it. Internal state/timer events are used
// only to cross-check the packet-level verdict, never as its basis.
package diagnose

import (
	"time"

	"nagletrace/transport"
)

// WaitType classifies a detected stall.
type WaitType string

const (
	// WaitNagleDelayedACK is the headline pattern: the sender holds a
	// second small packet (Nagle, unacked bytes in flight) while the
	// receiver holds the ACK on its delayed-ACK timer — two mechanisms
	// waiting on each other.
	WaitNagleDelayedACK WaitType = "nagle-delayed-ack"
	// WaitDelayedACK: only the receiver's delayed-ACK timer is involved;
	// the sender had no second packet ready (stop-and-wait request
	// boundary) and simply could not close its message until the ACK
	// window advanced.
	WaitDelayedACK WaitType = "delayed-ack-only"
	// WaitDelayedACKPiggyback: receiver was going to delay but its own
	// outgoing data carries the ACK first.
	WaitPiggyback WaitType = "ack-piggybacked"
	// WaitRTO: sender timed out and retransmitted.
	WaitRTO WaitType = "rto-retransmit"
	// WaitFastRetransmit: three duplicate ACKs triggered a retransmit.
	WaitFastRetransmit WaitType = "fast-retransmit"
	// WaitCork: data sat under TCP_CORK until the flush/timeout.
	WaitCork WaitType = "cork-hold"
	// WaitWindow: sender blocked by a zero/small receive window.
	WaitWindow WaitType = "window-full"
	// WaitApp: no transport cause — the application simply wrote later.
	WaitApp WaitType = "application-delay"
)

// WaitEvent is one diagnosed stall interval.
type WaitEvent struct {
	Type       WaitType      `json:"type"`
	Who        string        `json:"who"`        // endpoint that was blocked
	WaitingFor string        `json:"waitingFor"` // human description
	Start      time.Duration `json:"start"`      // when the wait began
	End        time.Duration `json:"end"`        // when it ended
	Duration   time.Duration `json:"duration"`
	BlockedSeq uint32        `json:"blockedSeq,omitempty"`
	UnblockPkt string        `json:"unblockPacket,omitempty"` // packet that ended the wait
	Evidence   []string      `json:"evidence"`
	CausedBy   string        `json:"causedBy"` // which side + setting
	Suggestion string        `json:"suggestion"`
}

// Severity ranks waits for the summary.
type Severity string

const (
	SevCritical Severity = "critical"
	SevWarn     Severity = "warning"
	SevInfo     Severity = "info"
)

// Diagnosis is the full verdict for one run.
type Diagnosis struct {
	PrimaryType WaitType      `json:"primaryType"`
	Severity    Severity      `json:"severity"`
	Summary     string        `json:"summary"`
	RootCause   string        `json:"rootCause"`
	Waits       []WaitEvent   `json:"waits"`
	TotalStall  time.Duration `json:"totalStall"`
	MaxStall    time.Duration `json:"maxStall"`
	Retransmits int           `json:"retransmits"`
	Suggestions []string      `json:"suggestions"`
	// CrossCheck reports whether internal state events corroborate the
	// packet-level diagnosis (empty when everything matched).
	CrossCheck []string `json:"crossCheck,omitempty"`
}

// Hint carries transport configuration the sniffer-only analysis cannot
// recover from packets (the negotiated MSS). The stall *existence and
// timing* still come exclusively from the packet trace; this only tells
// the classifier whether a segment counts as full-sized.
type Hint struct {
	MSS int
}

// pkt is a flattened, direction-annotated packet event.
type pkt struct {
	ev        *transport.TraceEvent
	src       string
	dst       string
	isData    bool
	isPureAck bool
	seq       uint32
	ack       uint32
}

func flatten(tr *transport.Trace) []pkt {
	evs := tr.Packets()
	out := make([]pkt, 0, len(evs))
	for _, ev := range evs {
		src, dst := ev.Endpoint, ""
		if ev.Direction == "send" {
			if ev.Endpoint == "client" {
				dst = "server"
			} else {
				dst = "client"
			}
		} else {
			if ev.Endpoint == "client" {
				src = "server"
				dst = "client"
			} else {
				src = "client"
				dst = "server"
			}
		}
		out = append(out, pkt{
			ev: ev, src: src, dst: dst,
			isData:    ev.PayloadLen > 0,
			isPureAck: ev.PayloadLen == 0 && ev.Flags.Has(transport.ACK) && !ev.Flags.Has(transport.SYN) && !ev.Flags.Has(transport.FIN),
			seq:       ev.Seq,
			ack:       ev.Ack,
		})
	}
	return out
}

// Analyze produces the verdict for a trace using the default MSS hint.
func Analyze(tr *transport.Trace) *Diagnosis {
	return AnalyzeWithHint(tr, Hint{MSS: 1460})
}

// AnalyzeWithHint produces the verdict with an explicit MSS hint.
func AnalyzeWithHint(tr *transport.Trace, hint Hint) *Diagnosis {
	pkts := flatten(tr)
	d := &Diagnosis{Severity: SevInfo, Waits: []WaitEvent{}, Suggestions: []string{}}

	waits := findNagleDelayedAck(tr, pkts, hint.MSS)
	waits = append(waits, findRetransmitWaits(pkts)...)
	waits = append(waits, findWindowWaits(pkts)...)
	waits = append(waits, findCorkWaits(tr, pkts)...)

	// order by start time
	sortWaits(waits)
	d.Waits = waits
	for _, w := range waits {
		d.TotalStall += w.Duration
		if w.Duration > d.MaxStall {
			d.MaxStall = w.Duration
		}
		if w.Type == WaitRTO || w.Type == WaitFastRetransmit {
			d.Retransmits++
		}
	}
	summarize(d, tr)
	crossCheck(d, tr)
	return d
}
