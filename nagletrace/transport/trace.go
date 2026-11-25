package transport

import "time"

// EventKind classifies trace entries. The diagnosis engine only relies
// on KindPacket entries; the state/timer annotations are ground truth
// used by the UI and by tests that validate the diagnosis.
type EventKind string

const (
	KindPacket EventKind = "packet"
	KindState  EventKind = "state"
	KindTimer  EventKind = "timer"
)

// TraceEvent is one row of a connection's timeline.
type TraceEvent struct {
	Time       time.Duration `json:"time"`
	Kind       EventKind     `json:"kind"`
	Name       string        `json:"name"`
	Endpoint   string        `json:"endpoint"`            // "client" | "server"
	Direction  string        `json:"direction,omitempty"` // "send" | "recv"
	Seq        uint32        `json:"seq"`
	Ack        uint32        `json:"ack"`
	Flags      Flags         `json:"flags"`
	PayloadLen int           `json:"payloadLen"`
	Window     uint32        `json:"window"`
	Detail     string        `json:"detail,omitempty"`
}

// Trace is the append-only timeline shared by the two endpoints of a
// connection (both write into it, ordered by virtual time).
type Trace struct {
	Events []*TraceEvent
}

func NewTrace() *Trace { return &Trace{} }

func (t *Trace) add(e *TraceEvent) { t.Events = append(t.Events, e) }

// Packets returns only wire events, which is what the diagnosis engine
// consumes — it must reach its verdict from what a sniffer would see.
func (t *Trace) Packets() []*TraceEvent {
	out := make([]*TraceEvent, 0, len(t.Events))
	for _, e := range t.Events {
		if e.Kind == KindPacket {
			out = append(out, e)
		}
	}
	return out
}

// Named returns the internal state/timer events of a given name.
func (t *Trace) Named(name string) []*TraceEvent {
	var out []*TraceEvent
	for _, e := range t.Events {
		if e.Name == name {
			out = append(out, e)
		}
	}
	return out
}
