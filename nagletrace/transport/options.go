package transport

import "time"

// Options are the per-connection knobs. They deliberately mirror the
// real TCP socket / sysctl names so a diagnosis can point at the exact
// setting that caused a wait.
type Options struct {
	// --- sender side -------------------------------------------------
	// NoDelay == TCP_NODELAY: disables Nagle's small-packet holding.
	NoDelay bool
	// Cork == TCP_CORK: while corked the sender accumulates writes and
	// never emits a (non-full) segment until Flush/Uncork.
	Cork bool
	// CorkTimeout auto-flushes corked data (Linux does this ~200ms).
	CorkTimeout time.Duration

	MSS         int // max payload bytes per segment, default 1460
	SendBuffer  int // unsent+unacked byte cap
	InitialCwnd int // congestion window in MSS-sized segments
	InitialRTO  time.Duration
	MinRTO      time.Duration
	MaxRTO      time.Duration

	// --- receiver side ----------------------------------------------
	// DelayAck == the delayed-ACK policy. When false every in-order
	// segment is acknowledged immediately.
	DelayAck bool
	// DelayAckTimeout is how long the receiver holds a pure ACK
	// (Linux default 40ms, hence the famous 40ms tail).
	DelayAckTimeout time.Duration
	// AckAfterPackets acknowledges at once once this many data segments
	// have arrived since the last ACK (RFC 1122 "at least every second").
	AckAfterPackets int
	// QuickAck == TCP_QUICKACK: the next QuickAckCount segments are
	// acknowledged immediately, then the receiver falls back to delayed.
	QuickAck bool
	// QuickAckCount is how many immediate ACKs quick mode grants
	// (Linux re-arms up to ~2; default 2). A value of 0 with QuickAck
	// on means sustained quick mode (the app keeps re-arming).
	QuickAckCount int
	// QuickAckRearmRate models the kernel re-entering quick-ACK mode on
	// its own: each time an in-order segment arrives while the delayed
	// ACK path would otherwise arm, this fraction of segments get an
	// immediate ACK instead. Linux does this after path idleness, window
	// events and congestion-state changes — it is why production traces
	// show the 40ms tail on only ~10% of requests rather than 100%.
	// 0 = strict delayed ACK (deterministic, easy to reason about).
	QuickAckRearmRate float64
	// QuickAckRearmSeed seeds the deterministic re-arm decision.
	QuickAckRearmSeed int64

	ReceiveBuffer int // bytes the receiver advertises as its window
}

// DefaultOptions returns the "Linux-ish" baseline: Nagle on, delayed ACK
// on with a 40ms timer, ACK every second packet.
func DefaultOptions() Options {
	return Options{
		NoDelay:         false,
		Cork:            false,
		CorkTimeout:     200 * time.Millisecond,
		MSS:             1460,
		SendBuffer:      64 * 1024,
		InitialCwnd:     10,
		InitialRTO:      1 * time.Second,
		MinRTO:          200 * time.Millisecond,
		MaxRTO:          60 * time.Second,
		DelayAck:        true,
		DelayAckTimeout: 40 * time.Millisecond,
		AckAfterPackets: 2,
		QuickAck:        false,
		QuickAckCount:   2,
		ReceiveBuffer:   64 * 1024,
	}
} // NoDelayOptions is TCP_NODELAY applied to the baseline.
func NoDelayOptions() Options {
	o := DefaultOptions()
	o.NoDelay = true
	return o
}

// QuickAckOptions is sustained TCP_QUICKACK applied to the baseline:
// the receiver acknowledges every in-order segment immediately.
func QuickAckOptions() Options {
	o := DefaultOptions()
	o.QuickAck = true
	o.QuickAckCount = 0
	return o
}

// OneShotQuickAckOptions mirrors a single Linux TCP_QUICKACK setsockopt:
// the next two segments are ACKed immediately, then delayed ACK resumes.
func OneShotQuickAckOptions() Options {
	o := DefaultOptions()
	o.QuickAck = true
	o.QuickAckCount = 2
	return o
}
