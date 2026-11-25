package transport

import (
	"testing"
	"time"

	"nagletrace/clock"
	"nagletrace/netem"
)

// harness builds a connected client/server pair on the virtual clock.
type harness struct {
	clk       *clock.Clock
	net       *netem.Network
	tr        *Trace
	client    *Conn
	server    *Conn
	clientApp *recApp
	serverApp *recApp
}

type recApp struct {
	NopApp
	reads [][]byte
	conn  *Conn
}

func (a *recApp) OnConnected(c *Conn) { a.conn = c }
func (a *recApp) OnRead(c *Conn, b []byte) {
	a.reads = append(a.reads, append([]byte(nil), b...))
}

func newHarness(t *testing.T, clientOpts, serverOpts Options) *harness {
	t.Helper()
	h := &harness{
		clk:       clock.New(),
		tr:        NewTrace(),
		clientApp: &recApp{},
		serverApp: &recApp{},
	}
	h.net = netem.New(h.clk, 1)
	h.net.SetLink("c", "s", netem.LinkConfig{Delay: time.Millisecond})
	h.net.SetLink("s", "c", netem.LinkConfig{Delay: time.Millisecond})

	ln := &Listener{
		Addr: "s", Clk: h.clk, Net: h.net, Trace: h.tr, ServerOpts: serverOpts,
		AcceptFunc: func(*Conn) App { return h.serverApp },
	}
	ln.Listen()

	d := &Dialer{Clk: h.clk, Net: h.net, Trace: h.tr}
	c, err := d.Dial("c", "s", clientOpts, h.clientApp)
	if err != nil {
		t.Fatal(err)
	}
	h.client = c
	if !h.clk.Run(5 * time.Second) {
		t.Fatal("handshake did not finish")
	}
	// fetch the server connection from the listener's demux table
	h.server = ln.conns["c"]
	if h.server == nil {
		t.Fatal("server connection not established")
	}
	return h
}

func (h *harness) packetCount() (data, ack int) {
	for _, ev := range h.tr.Events {
		if ev.Kind != KindPacket || ev.Direction != "send" {
			continue
		}
		if ev.PayloadLen > 0 {
			data++
		} else if ev.Flags.Has(ACK) {
			ack++
		}
	}
	return
}

func TestNagleHoldsSecondSmallPacket(t *testing.T) {
	h := newHarness(t, DefaultOptions(), DefaultOptions())
	beforeData, _ := h.packetCount()
	_, _ = h.client.Write([]byte("head"))
	_, _ = h.client.Write([]byte("body"))
	// immediately: only the first packet may be on the wire
	dataNow, _ := h.packetCount()
	if dataNow-beforeData != 1 {
		t.Fatalf("Nagle should hold the second small packet, got %d new data segments", dataNow-beforeData)
	}
	h.clk.Run(200 * time.Millisecond)
	if len(h.serverApp.reads) == 0 {
		t.Fatal("server never received the buffered data")
	}
	if got := concat(h.serverApp.reads); got != "headbody" {
		t.Fatalf("stream reassembly mismatch: %q", got)
	}
}

func TestNoDelaySendsBothSmallPackets(t *testing.T) {
	h := newHarness(t, NoDelayOptions(), DefaultOptions())
	beforeData, _ := h.packetCount()
	_, _ = h.client.Write([]byte("head"))
	_, _ = h.client.Write([]byte("body"))
	dataNow, _ := h.packetCount()
	if dataNow-beforeData != 2 {
		t.Fatalf("TCP_NODELAY should emit both small packets, got %d", dataNow-beforeData)
	}
}

func TestWritevMergesIntoOneSegment(t *testing.T) {
	h := newHarness(t, DefaultOptions(), DefaultOptions())
	beforeData, _ := h.packetCount()
	_, _ = h.client.WriteMulti([]byte("head"), []byte("body"))
	dataNow, _ := h.packetCount()
	if dataNow-beforeData != 1 {
		t.Fatalf("writev should produce one segment, got %d", dataNow-beforeData)
	}
}

func TestDelayedACKTimerFiresAtTimeout(t *testing.T) {
	// NoDelay client isolates the receiver-side policy under test.
	h := newHarness(t, NoDelayOptions(), DefaultOptions())
	baseline := countAcks(h.tr, "server")
	// one in-order small segment: ACK must be held, then fire 40ms after
	// the segment arrives (1ms OWD on this link).
	_, _ = h.client.WriteMulti([]byte("single"))
	h.clk.Run(2 * time.Millisecond) // let the segment arrive, timer armed
	armedAt := h.clk.Now()
	early := countAcks(h.tr, "server")
	if early != baseline {
		t.Fatalf("ACK must be held, got %d before the timer (baseline %d)", early, baseline)
	}
	// Fire everything up to just before the 40ms deadline: the ACK must
	// still be held.
	h.clk.RunUntil(armedAt + 39*time.Millisecond)
	if got := countAcks(h.tr, "server"); got != baseline {
		t.Fatalf("ACK fired before the 40ms timeout")
	}
	// Then let the delayed-ACK timer itself fire.
	h.clk.Run(armedAt + 42*time.Millisecond)
	if got := countAcks(h.tr, "server"); got != baseline+1 {
		t.Fatalf("expected the delayed ACK at ~40ms, got %d (baseline %d)", got, baseline)
	}
}

func TestSecondPacketAcksImmediately(t *testing.T) {
	h := newHarness(t, NoDelayOptions(), DefaultOptions())
	baseline := countAcks(h.tr, "server")
	_, _ = h.client.WriteMulti([]byte("one"))
	_, _ = h.client.WriteMulti([]byte("two"))
	h.clk.RunUntil(h.clk.Now() + 5*time.Millisecond)
	// two data packets -> the every-second-packet rule must ACK without
	// waiting for the 40ms timer.
	if got := countAcks(h.tr, "server") - baseline; got == 0 {
		t.Fatal("second packet should trigger an immediate ACK")
	}
}

func TestSustainedQuickACK(t *testing.T) {
	h := newHarness(t, NoDelayOptions(), QuickAckOptions())
	baseline := countAcks(h.tr, "server")
	_, _ = h.client.WriteMulti([]byte("one"))
	h.clk.RunUntil(h.clk.Now() + 2*time.Millisecond)
	first := countAcks(h.tr, "server")
	if first <= baseline {
		t.Fatal("quick ACK did not fire immediately")
	}
	_, _ = h.client.WriteMulti([]byte("two"))
	h.clk.RunUntil(h.clk.Now() + 2*time.Millisecond)
	second := countAcks(h.tr, "server")
	if second <= first {
		t.Fatal("sustained quick ACK should acknowledge every segment")
	}
}

func TestCorkAccumulatesThenFlushes(t *testing.T) {
	h := newHarness(t, DefaultOptions(), DefaultOptions())
	h.client.Cork()
	before, _ := h.packetCount()
	_, _ = h.client.Write([]byte("a"))
	_, _ = h.client.Write([]byte("b"))
	if now, _ := h.packetCount(); now != before {
		t.Fatal("corked writes must not hit the wire before flush")
	}
	_ = h.client.Uncork()
	now, _ := h.packetCount()
	if now-before != 1 {
		t.Fatalf("flush should emit one merged segment, got %d", now-before)
	}
	h.clk.Run(10 * time.Millisecond)
	if got := concat(h.serverApp.reads); got != "ab" {
		t.Fatalf("corked data mismatch: %q", got)
	}
}

func TestRetransmitAfterLoss(t *testing.T) {
	// 100% loss on the server->client direction for the first data ACK
	// is hard to scope; instead inject total loss briefly and verify the
	// RTO path eventually delivers everything.
	clk := clock.New()
	tr := NewTrace()
	n := netem.New(clk, 7)
	n.SetLink("c", "s", netem.LinkConfig{Delay: time.Millisecond})
	n.SetLink("s", "c", netem.LinkConfig{Delay: time.Millisecond, LossRate: 0.5})
	ca, sa := &recApp{}, &recApp{}
	ln := &Listener{Addr: "s", Clk: clk, Net: n, Trace: tr, ServerOpts: DefaultOptions(),
		AcceptFunc: func(*Conn) App { return sa }}
	ln.Listen()
	d := &Dialer{Clk: clk, Net: n, Trace: tr}
	c, _ := d.Dial("c", "s", DefaultOptions(), ca)
	clk.Run(5 * time.Second)
	_, _ = c.WriteMulti([]byte("hello-reliability"))
	if !clk.Run(30 * time.Second) {
		t.Fatal("data never delivered despite retransmits")
	}
	if got := concat(sa.reads); got != "hello-reliability" {
		t.Fatalf("reliable delivery failed: %q", got)
	}
}

func countAcks(tr *Trace, endpoint string) int {
	n := 0
	for _, ev := range tr.Events {
		if ev.Kind == KindPacket && ev.Direction == "send" && ev.Endpoint == endpoint &&
			ev.PayloadLen == 0 && ev.Flags.Has(ACK) {
			n++
		}
	}
	return n
}

func concat(parts [][]byte) string {
	out := make([]byte, 0)
	for _, p := range parts {
		out = append(out, p...)
	}
	return string(out)
}
