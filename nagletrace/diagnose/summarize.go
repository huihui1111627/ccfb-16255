package diagnose

import (
	"strconv"
	"strings"
	"time"

	"nagletrace/transport"
)

// summarize turns the raw wait list into a single verdict.
func summarize(d *Diagnosis, tr *transport.Trace) {
	if d.Waits == nil {
		d.Waits = []WaitEvent{}
	}
	if d.Suggestions == nil {
		d.Suggestions = []string{}
	}
	if len(d.Waits) == 0 {
		d.PrimaryType = WaitApp
		d.Severity = SevInfo
		d.Summary = "no transport-level mutual wait detected; request latency is application/propagation time"
		d.RootCause = "none"
		return
	}

	// The primary pattern is the type responsible for the largest total
	// stall time.
	totals := map[WaitType]time.Duration{}
	counts := map[WaitType]int{}
	for _, w := range d.Waits {
		totals[w.Type] += w.Duration
		counts[w.Type]++
	}
	var primary WaitType
	var best time.Duration
	for t, v := range totals {
		if v > best {
			best, primary = v, t
		}
	}
	d.PrimaryType = primary

	switch primary {
	case WaitNagleDelayedACK:
		d.Severity = SevCritical
		d.Summary = "Nagle/delayed-ACK mutual wait detected " +
			"on " + itoa(counts[WaitNagleDelayedACK]) + " request(s): " +
			"a small second packet is held by the sender until the receiver's delayed-ACK timer fires"
		d.RootCause = firstWait(d, WaitNagleDelayedACK).CausedBy
		d.Suggestions = []string{
			"sender side: set TCP_NODELAY (disables Nagle for this connection)",
			"application side: merge header+body into one writev/sendmmsg call, or use TCP_CORK with an immediate flush",
			"receiver side: enable TCP_QUICKACK, or lower the delayed-ACK timer (40ms on Linux by default)",
		}
	case WaitDelayedACK:
		d.Severity = SevWarn
		d.Summary = "delayed-ACK-only wait on " + itoa(counts[WaitDelayedACK]) +
			" request boundary(ies): no Nagle-held packet, but the receiver's delayed-ACK timer gates each stop-and-wait round trip"
		d.RootCause = firstWait(d, WaitDelayedACK).CausedBy
		d.Suggestions = []string{
			"receiver side: enable TCP_QUICKACK or lower the delayed-ACK timer",
			"application side: pipeline requests so the every-second-packet rule ACKs immediately",
		}
	case WaitRTO:
		d.Severity = SevCritical
		d.Summary = itoa(counts[WaitRTO]) + " retransmission timeout(s): segments were lost and the sender waited for the RTO"
		d.RootCause = "packet loss combined with an RTO-scale timer; delayed ACKs can prevent fast retransmit for single-packet windows"
		d.Suggestions = []string{
			"fix the underlying loss (this run had loss injected)",
			"with single outstanding small packets, delayed ACK removes the duplicate-ACK signal: TCP_NODELAY/quick ACK also restores faster loss recovery",
		}
	case WaitFastRetransmit:
		d.Severity = SevWarn
		d.Summary = itoa(counts[WaitFastRetransmit]) + " loss event(s) repaired by fast retransmit after duplicate ACKs"
		d.RootCause = "injected packet loss"
		d.Suggestions = []string{"fast retransmit is working; reduce path loss"}
	case WaitCork:
		d.Severity = SevWarn
		d.Summary = "TCP_CORK held data for " + d.MaxStall.String() + " before flush"
		d.RootCause = firstWait(d, WaitCork).CausedBy
		d.Suggestions = []string{
			"call uncork as soon as the complete message is buffered",
			"never rely on the ~200ms kernel auto-flush for interactive traffic",
		}
	case WaitWindow:
		d.Severity = SevWarn
		d.Summary = "zero-window stall: the receiver's application is not draining its buffer"
		d.RootCause = firstWait(d, WaitWindow).CausedBy
		d.Suggestions = []string{"speed up the reading application", "increase the receive buffer"}
	case WaitPiggyback:
		d.Severity = SevInfo
		d.Summary = "delayed ACKs were consistently hidden by piggybacking on reverse traffic"
		d.RootCause = "latent Nagle/delayed-ACK interaction masked by bidirectional traffic"
		d.Suggestions = []string{"the stall reappears whenever the peer has no data to send; apply the Nagle fixes anyway"}
	default:
		d.Severity = SevInfo
		d.Summary = "stalls attributed to application timing"
	}

	// Retransmissions alongside the headline stall change the wording.
	if (primary == WaitNagleDelayedACK || primary == WaitDelayedACK) && d.Retransmits > 0 {
		d.Summary += "; additionally, " + itoa(d.Retransmits) + " retransmission(s) overlap the delayed-ACK pattern"
	}
}

func firstWait(d *Diagnosis, t WaitType) WaitEvent {
	for _, w := range d.Waits {
		if w.Type == t {
			return w
		}
	}
	return WaitEvent{}
}

// crossCheck compares the packet-level verdict against the internal
// Nagle/delack timer annotations. It exists to prove the sniffer-only
// diagnosis agrees with ground truth; mismatches are reported, not hidden.
func crossCheck(d *Diagnosis, tr *transport.Trace) {
	nagleHolds := 0
	delackArms := 0
	delackFires := 0
	for _, ev := range tr.Events {
		switch ev.Name {
		case "nagle-hold":
			nagleHolds++
		case "delack-arm":
			delackArms++
		}
		if ev.Kind == transport.KindPacket &&
			strings.HasPrefix(ev.Detail, "ack: delayed-ACK timer fired") {
			delackFires++
		}
	}
	diagnosed := 0
	for _, w := range d.Waits {
		if w.Type == WaitNagleDelayedACK {
			diagnosed++
		}
	}
	// Every diagnosed stall should correspond to at least one nagle-hold,
	// and roughly one delack arm/fire per stall.
	if diagnosed > 0 && nagleHolds == 0 {
		d.CrossCheck = append(d.CrossCheck,
			"packet analysis found "+itoa(diagnosed)+" Nagle stalls but the trace contains no nagle-hold state events")
	}
	if diagnosed > 0 && delackArms == 0 {
		d.CrossCheck = append(d.CrossCheck,
			"packet analysis found delayed-ACK stalls but the trace contains no delack-arm events")
	}
	if len(d.CrossCheck) == 0 && diagnosed > 0 {
		d.CrossCheck = append(d.CrossCheck,
			"ground truth corroborates the verdict: "+itoa(nagleHolds)+
				" Nagle hold(s), "+itoa(delackArms)+" delayed-ACK timer(s) armed, "+
				itoa(delackFires)+" delayed-ACK fire(s), "+
				itoa(diagnosed)+" stall(s) diagnosed from packets alone")
	}
}

func formatInt(v int64) string {
	return strconv.FormatInt(v, 10)
}

// Markdown renders the verdict for terminal output.
func (d *Diagnosis) Markdown() string {
	var b strings.Builder
	b.WriteString("### Diagnosis: " + string(d.PrimaryType) + " (" + string(d.Severity) + ")\n\n")
	b.WriteString(d.Summary + "\n\n")
	b.WriteString("Root cause: " + d.RootCause + "\n\n")
	if len(d.Waits) > 0 {
		b.WriteString("Wait intervals:\n\n")
		for i, w := range d.Waits {
			b.WriteString(itostr(i+1) + ". [" + string(w.Type) + "] " + w.Who +
				" waited " + w.Duration.String() + " (" + w.Start.String() + " → " + w.End.String() + ")\n")
			b.WriteString("   - waiting for: " + w.WaitingFor + "\n")
			b.WriteString("   - unblocked by: " + w.UnblockPkt + "\n")
			b.WriteString("   - caused by: " + w.CausedBy + "\n")
		}
		b.WriteString("\n")
	}
	if len(d.Suggestions) > 0 {
		b.WriteString("Suggestions:\n")
		for _, s := range d.Suggestions {
			b.WriteString("- " + s + "\n")
		}
	}
	if len(d.CrossCheck) > 0 {
		b.WriteString("\nCross-check: " + strings.Join(d.CrossCheck, "; ") + "\n")
	}
	return b.String()
}

func itostr(v int) string { return strconv.Itoa(v) }
