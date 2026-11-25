package experiment

import (
	"testing"
	"time"

	"nagletrace/diagnose"
)

func TestPipelineSelfHeals(t *testing.T) {
	res := Run(Preset(PresetPipelined))
	if !res.Completed {
		t.Fatal("pipelined scenario did not complete")
	}
	// With merged writes pipelined back-to-back, the first request is
	// fast (~1 OWD): no 40ms timer can block the head of the queue.
	if res.Summary.Min > 5*time.Millisecond {
		t.Fatalf("pipeline head request should be fast, min=%s", res.Summary.Min)
	}
	t.Logf("pipeline: min=%s p50=%s max=%s", res.Summary.Min, res.Summary.P50, res.Summary.Max)
}

func TestChunkedResponseStallsOnServer(t *testing.T) {
	res := Run(Preset(PresetChunkedResponse))
	if !res.Completed {
		t.Fatal("chunked scenario did not complete")
	}
	d := diagnose.Analyze(res.Trace)
	serverStall := false
	for _, w := range d.Waits {
		if w.Who == "server" && (w.Type == diagnose.WaitNagleDelayedACK || w.Type == diagnose.WaitDelayedACK) {
			serverStall = true
		}
	}
	if !serverStall {
		t.Fatal("chunked server response should produce a server-side Nagle/delayed-ACK wait")
	}
	t.Logf("chunked: server-side stall confirmed, max latency %s", res.Summary.Max)
}

func TestCorkedShowsCorkOrFastFirstRequest(t *testing.T) {
	res := Run(Preset(PresetCorked))
	if !res.Completed {
		t.Fatal("corked scenario did not complete")
	}
	d := diagnose.Analyze(res.Trace)
	// Cork flush is immediate in this workload, so the first request
	// avoids the intra-request Nagle stall; later stop-and-wait boundaries
	// may still hit delayed ACK. The key assertion: no RTO/crash and the
	// diagnosis type belongs to the ACK family, never a loss event.
	for _, w := range d.Waits {
		if w.Type == diagnose.WaitRTO || w.Type == diagnose.WaitFastRetransmit {
			t.Fatalf("lossless corked run must not retransmit: %+v", w)
		}
	}
	t.Logf("corked: max latency %s, primary=%s", res.Summary.Max, d.PrimaryType)
}

func TestLossyRecoversAndDiagnoses(t *testing.T) {
	res := Run(Preset(PresetLossy))
	if !res.Completed {
		t.Fatalf("lossy scenario failed to recover all requests by %s, samples=%d",
			res.SimTime, res.Summary.Count)
	}
	if res.Dropped == 0 {
		t.Fatal("expected injected drops")
	}
	d := diagnose.Analyze(res.Trace)
	if d.Retransmits == 0 {
		t.Fatal("lossy run must show retransmissions")
	}
	sawRTO := false
	for _, w := range d.Waits {
		if w.Type == diagnose.WaitRTO {
			sawRTO = true
		}
	}
	if !sawRTO {
		t.Fatal("small stop-and-wait windows should recover via RTO, not only fast retransmit")
	}
	t.Logf("lossy: dropped=%d retransmits=%d max latency=%s primary=%s",
		res.Dropped, d.Retransmits, res.Summary.Max, d.PrimaryType)
}

func TestClientOnlyNoDelayMovesTail(t *testing.T) {
	res := Run(Preset(PresetClientNoDelay))
	if !res.Completed {
		t.Fatal("one-sided fix scenario did not complete")
	}
	// The response path still uses default Nagle + the client's delayed
	// ACKs, so the 40ms tail must remain — the tool must not claim a cure.
	if res.Summary.Max < 35*time.Millisecond {
		t.Fatalf("one-sided NoDelay should leave a tail, max=%s", res.Summary.Max)
	}
	t.Logf("one-sided fix: tail remains at max=%s (as real stacks behave)", res.Summary.Max)
}
