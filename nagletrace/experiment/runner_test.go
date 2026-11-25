package experiment

import (
	"testing"
	"time"

	"nagletrace/diagnose"
)

func hasWait(d *diagnose.Diagnosis, t diagnose.WaitType) bool {
	for _, w := range d.Waits {
		if w.Type == t {
			return true
		}
	}
	return false
}

func TestClassicStallReproduces(t *testing.T) {
	res := Run(Preset(PresetClassicStall))
	if !res.Completed {
		t.Fatalf("scenario did not complete by %s (samples=%d)", res.SimTime, res.Summary.Count)
	}
	d := diagnose.Analyze(res.Trace)
	if d.PrimaryType != diagnose.WaitNagleDelayedACK {
		t.Fatalf("expected nagle-delayed-ack, got %s: %s", d.PrimaryType, d.Summary)
	}
	if res.Summary.Count != 20 {
		t.Fatalf("want 20 samples, got %d", res.Summary.Count)
	}
	// Every stop-and-wait request incurs roughly the 40ms delayed-ACK
	// timer plus propagation — the production "stuck at ~40ms" shape.
	if res.Summary.Min < 35*time.Millisecond {
		t.Fatalf("minimum latency %s below the delayed-ACK scale", res.Summary.Min)
	}
	if res.Summary.Max > 90*time.Millisecond {
		t.Fatalf("max latency %s unexpectedly large for a lossless link", res.Summary.Max)
	}
	t.Logf("stall waits=%d maxStall=%s p50=%s p99=%s",
		len(d.Waits), d.MaxStall, res.Summary.P50, res.Summary.P99)
}

func TestNoDelayRemovesTail(t *testing.T) {
	res := Run(Preset(PresetFixedNoDelay))
	if !res.Completed {
		t.Fatal("nodelay scenario did not complete")
	}
	d := diagnose.Analyze(res.Trace)
	if hasWait(d, diagnose.WaitNagleDelayedACK) {
		t.Fatal("TCP_NODELAY still shows a Nagle stall")
	}
	if hasWait(d, diagnose.WaitDelayedACK) {
		t.Fatal("TCP_NODELAY scenario still shows delayed-ACK-only waits")
	}
	if res.Summary.Max >= 10*time.Millisecond {
		t.Fatalf("tail latency remains: max=%s", res.Summary.Max)
	}
	t.Logf("TCP_NODELAY: max latency %s, diagnosis=%s", res.Summary.Max, d.PrimaryType)
}

func TestQuickAckRemovesTail(t *testing.T) {
	res := Run(Preset(PresetFixedQuickAck))
	if !res.Completed {
		t.Fatal("quickack scenario did not complete")
	}
	d := diagnose.Analyze(res.Trace)
	if hasWait(d, diagnose.WaitNagleDelayedACK) || hasWait(d, diagnose.WaitDelayedACK) {
		t.Fatalf("sustained TCP_QUICKACK still shows ACK stalls: %s", d.Summary)
	}
	if res.Summary.Max >= 10*time.Millisecond {
		t.Fatalf("tail latency remains: max=%s", res.Summary.Max)
	}
	t.Logf("TCP_QUICKACK: max latency %s, diagnosis=%s", res.Summary.Max, d.PrimaryType)
}

func TestWritevReducesStall(t *testing.T) {
	res := Run(Preset(PresetFixedWritev))
	if !res.Completed {
		t.Fatal("writev scenario did not complete")
	}
	d := diagnose.Analyze(res.Trace)
	// With writev there is no held *second* packet inside a request:
	// each request is one segment. Under stop-and-wait the next request
	// still meets Nagle at the request boundary, so the tool should
	// attribute it precisely rather than mislabel the original pattern.
	t.Logf("writev: waits=%d primary=%s max latency %s", len(d.Waits), d.PrimaryType, res.Summary.Max)
	for _, w := range d.Waits {
		if w.Type == diagnose.WaitPiggyback || w.Type == diagnose.WaitWindow ||
			w.Type == diagnose.WaitRTO || w.Type == diagnose.WaitFastRetransmit {
			t.Fatalf("unexpected wait type under lossless writev: %+v", w)
		}
	}
}

func TestMixedWorkloadShape(t *testing.T) {
	res := Run(Preset(PresetMixedWorkload))
	if !res.Completed {
		t.Fatal("mixed workload did not complete")
	}
	d := diagnose.Analyze(res.Trace)
	if !hasWait(d, diagnose.WaitNagleDelayedACK) {
		t.Fatal("the occasional split-write requests should reproduce Nagle stalls")
	}
	fast, slow := 0, 0
	for _, s := range res.Latency.Samples {
		if s.Latency >= 35*time.Millisecond {
			slow++
		} else {
			fast++
		}
	}
	if slow == 0 || fast == 0 {
		t.Fatalf("expected both fast and slow samples, got fast=%d slow=%d", fast, slow)
	}
	if fast <= slow*2 {
		t.Fatalf("production shape expects a large fast majority, got fast=%d slow=%d", fast, slow)
	}
	t.Logf("mixed: fast=%d slow=%d (tail fraction %.2f), diagnosis=%s",
		fast, slow, res.Summary.TailFraction, d.PrimaryType)

	// And the same workload with the fix applied must lose the tail.
	fixed := Preset(PresetMixedWorkload)
	fixed.ClientOpts.NoDelay = true
	fixed.ServerOpts.NoDelay = true
	fixedRes := Run(fixed)
	if fixedRes.Summary.Max >= 10*time.Millisecond {
		t.Fatalf("after TCP_NODELAY the 40ms tail must vanish, max=%s", fixedRes.Summary.Max)
	}
	if fixedRes.Summary.TailCount != 0 {
		t.Fatalf("fixed run still reports %d tail samples", fixedRes.Summary.TailCount)
	}
	t.Logf("fixed mixed: max latency %s, tail gone", fixedRes.Summary.Max)
}

func TestDeterminism(t *testing.T) {
	a := Run(Preset(PresetClassicStall))
	b := Run(Preset(PresetClassicStall))
	if len(a.Trace.Events) != len(b.Trace.Events) {
		t.Fatal("same scenario produced different trace lengths")
	}
	for i := range a.Trace.Events {
		ea, eb := a.Trace.Events[i], b.Trace.Events[i]
		if ea.Time != eb.Time || ea.Seq != eb.Seq || ea.Ack != eb.Ack || ea.Flags != eb.Flags {
			t.Fatalf("nondeterministic event %d: %+v vs %+v", i, ea, eb)
		}
	}
}
