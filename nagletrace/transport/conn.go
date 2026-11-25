// Package transport is a user-space, TCP-like reliable transport running
// on top of the deterministic netem link. It implements the pieces that
// create (and cure) the Nagle + delayed-ACK interaction:
//
//   - per-connection Nagle (TCP_NODELAY) and TCP_CORK
//   - delayed ACKs with the "every second packet" rule, piggybacking
//     and a TCP_QUICKACK mode
//   - byte stream sequencing, sliding window, RTO + fast retransmit
//   - receiver-side flow control windows
//
// Everything is single goroutine and driven by the virtual clock, so a
// given Options + workload always produces the same packet timeline.
package transport

import (
	"errors"
	"math/rand"
	"strconv"
	"time"

	"nagletrace/clock"
	"nagletrace/netem"
)

// App is the application bound to a connection. All callbacks run
// synchronously inside virtual-time event processing.
type App interface {
	OnConnected(c *Conn)
	OnRead(c *Conn, b []byte)
	OnClose(c *Conn)
}

// NopApp lets concrete apps implement only the callbacks they need.
type NopApp struct{}

func (NopApp) OnConnected(*Conn)    {}
func (NopApp) OnRead(*Conn, []byte) {}
func (NopApp) OnClose(*Conn)        {}

var (
	ErrClosed       = errors.New("connection closed")
	ErrWouldBlock   = errors.New("send buffer full")
	ErrNotConnected = errors.New("not connected")
)

type connState string

const (
	stateInit    connState = "init"
	stateSynSent connState = "syn-sent"
	stateSynRcvd connState = "syn-rcvd"
	stateEstab   connState = "established"
	stateFinWait connState = "fin-wait"
	stateClosed  connState = "closed"
)

// chunk is one contiguous application write moving through the sender.
// Retransmissions slice out the still-unacked prefix.
type chunk struct {
	data    []byte
	txCount int
	firstTx time.Duration
	lastTx  time.Duration
}

// Conn is one transport connection endpoint.
type Conn struct {
	Name  string // "client" or "server", used in traces
	Role  string
	Local string
	Peer  string

	clk   *clock.Clock
	net   *netem.Network
	opts  Options
	app   App
	trace *Trace

	state connState

	// send sequence space
	iss    uint32
	sndUna uint32 // oldest unacked sequence
	sndNxt uint32 // next sequence to use
	// receive sequence space
	rcvNxt uint32 // next expected sequence
	// peer's advertised receive window, refreshed by incoming ACKs
	peerWindow uint32

	unsent  []*chunk
	unacked []*chunk

	// RTO machinery
	rtoTimer *clock.Event
	rto      time.Duration
	srtt     time.Duration
	rttvar   time.Duration
	rttSeen  bool
	dupAcks  int

	// cork machinery
	corked    bool
	corkBuf   []byte
	corkTimer *clock.Event

	// receiver machinery
	ooo          map[uint32][]byte
	pendingAck   bool
	pktsSinceAck int
	delackTimer  *clock.Event
	quickLeft    int
	appPaused    bool
	appQueue     [][]byte
	recvHeld     int
	peerFinSeen  bool
	synackTimer  *clock.Event
	ackRand      *rand.Rand
}

// Dialer actively opens a connection.
type Dialer struct {
	Clk   *clock.Clock
	Net   *netem.Network
	Trace *Trace
}

// Dial sends the SYN. The app's OnConnected fires once the handshake
// completes (later, in virtual time, when the clock runs).
func (d *Dialer) Dial(local, peer string, opts Options, app App) (*Conn, error) {
	c := newConn("client", local, peer, d.Clk, d.Net, opts, app, d.Trace)
	c.state = stateSynSent
	// The client address is unique to this connection.
	d.Net.Connect(local, c.handleSegment)
	syn := &Segment{
		Src: local, Dst: peer, Seq: c.iss, Flags: SYN,
		Window: c.rcvWindow(),
		TSval:  c.clk.Now().Nanoseconds(),
	}
	c.sendRaw(syn, "syn")
	// SYN retransmissions use a dedicated doubling timer: the SYN is not
	// part of the data unacked queue.
	synRTO := c.opts.InitialRTO
	c.rtoTimer = c.clk.After(synRTO, func(time.Duration) {
		c.retransmitSyn(syn, synRTO, 1)
	})
	return c, nil
}

// retransmitSyn re-sends the SYN with exponential backoff until the
// handshake completes (processAck cancels rtoTimer on establishment).
func (c *Conn) retransmitSyn(syn *Segment, rto time.Duration, attempt int) {
	if c.state != stateSynSent {
		return
	}
	backoff := rto * 2
	if backoff > c.opts.MaxRTO {
		backoff = c.opts.MaxRTO
	}
	c.traceTimer("rto-fire", "SYN timeout #"+itoaUint(uint32(attempt))+", retransmitting")
	c.sendRaw(syn, "syn-retransmit")
	c.rtoTimer = c.clk.After(backoff, func(time.Duration) {
		c.retransmitSyn(syn, backoff, attempt+1)
	})
}

func itoaUint(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

// Listener answers SYNs on one address.
type Listener struct {
	Addr       string
	Clk        *clock.Clock
	Net        *netem.Network
	Trace      *Trace
	AcceptFunc func(c *Conn) App
	ServerOpts Options

	conns map[string]*Conn // peer address -> accepted connection
}

// Listen registers the listener with the network. The listener keeps
// ownership of the address's netem callback and demultiplexes incoming
// segments to the per-peer connection.
func (l *Listener) Listen() {
	l.conns = map[string]*Conn{}
	l.Net.Connect(l.Addr, func(pkt any) {
		seg, ok := pkt.(*Segment)
		if !ok {
			return
		}
		if conn, ok := l.conns[seg.Src]; ok {
			conn.handleSegment(pkt)
			return
		}
		if !seg.Flags.Has(SYN) || seg.Flags.Has(ACK) {
			return
		}
		c := newConn("server", l.Addr, seg.Src, l.Clk, l.Net, l.ServerOpts, nil, l.Trace)
		c.state = stateSynRcvd
		c.rcvNxt = seg.Seq + 1
		l.conns[seg.Src] = c
		c.app = l.AcceptFunc(c)
		// The SYNACK itself is always immediate. After the handshake the
		// data phase starts in delayed-ACK mode (Linux default): the
		// quick-ACK budget is consumed by the SYN handling unless the
		// option explicitly requests TCP_QUICKACK.
		c.quickLeft = 0
		if l.ServerOpts.QuickAck {
			c.quickLeft = l.ServerOpts.QuickAckCount
		}
		synack := c.buildSynAck(seg)
		c.sendRaw(synack, "syn-ack")
		c.armSynAckTimer(synack, l.ServerOpts.InitialRTO, 1)
		c.traceState("handshake", "SYN received, SYNACK sent")
	})
}

func (c *Conn) buildSynAck(seg *Segment) *Segment {
	return &Segment{
		Src: c.Local, Dst: c.Peer, Seq: c.sndNxt,
		Ack: c.rcvNxt, Flags: SYN | ACK, Window: c.rcvWindow(),
		TSval: c.clk.Now().Nanoseconds(), TSecr: seg.TSval,
	}
}

// armSynAckTimer re-sends the SYNACK with backoff while the connection
// stays in syn-rcvd; establishment cancels the timer.
func (c *Conn) armSynAckTimer(synack *Segment, rto time.Duration, attempt int) {
	c.synackTimer = c.clk.After(rto, func(time.Duration) {
		if c.state != stateSynRcvd {
			return
		}
		backoff := rto * 2
		if backoff > c.opts.MaxRTO {
			backoff = c.opts.MaxRTO
		}
		c.traceTimer("rto-fire", "SYNACK timeout #"+itoaUint(uint32(attempt))+", retransmitting")
		c.sendRaw(synack, "syn-ack-retransmit")
		c.armSynAckTimer(synack, backoff, attempt+1)
	})
}

func newConn(name, local, peer string, clk *clock.Clock, n *netem.Network, opts Options, app App, tr *Trace) *Conn {
	if opts.MSS <= 0 {
		opts = DefaultOptions()
	}
	return &Conn{
		Name:      name,
		Role:      name,
		Local:     local,
		Peer:      peer,
		clk:       clk,
		net:       n,
		opts:      opts,
		app:       app,
		trace:     tr,
		state:     stateInit,
		iss:       initialSeq(name),
		ooo:       map[uint32][]byte{},
		rto:       opts.InitialRTO,
		quickLeft: opts.QuickAckCount,
		ackRand:   rand.New(rand.NewSource(opts.QuickAckRearmSeed + rearmSeedOffset(name))),
	}
}

func rearmSeedOffset(role string) int64 {
	if role == "client" {
		return 7
	}
	return 991
}

// deterministic initial sequence numbers per role
func initialSeq(role string) uint32 {
	if role == "client" {
		return 1000
	}
	return 5000
}

// Trace exposes the timeline (shared by both endpoints).
func (c *Conn) Trace() *Trace { return c.trace }

// Options returns the current options.
func (c *Conn) Options() Options { return c.opts }

// SetNoDelay toggles TCP_NODELAY at runtime and re-runs the send pump.
func (c *Conn) SetNoDelay(v bool) {
	c.opts.NoDelay = v
	c.traceState("set-nodelay", b(v))
	if v {
		c.pump()
	}
}

// SetQuickAck re-arms TCP_QUICKACK for the next QuickAckCount segments.
func (c *Conn) SetQuickAck(v bool) {
	c.opts.QuickAck = v
	if v {
		c.quickLeft = c.opts.QuickAckCount
	}
	c.traceState("set-quickack", b(v))
}

func b(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
