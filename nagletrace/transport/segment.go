package transport

// Flags mirrors the TCP flag bits that matter for this simplified stack.
type Flags uint8

const (
	FIN Flags = 1 << iota
	SYN
	RST
	PSH
	ACK
)

func (f Flags) Has(x Flags) bool { return f&x != 0 }

// String renders flags the way tcpdump-style timelines show them.
func (f Flags) String() string {
	out := ""
	if f.Has(SYN) {
		out += "S"
	}
	if f.Has(ACK) {
		out += "A"
	}
	if f.Has(FIN) {
		out += "F"
	}
	if f.Has(RST) {
		out += "R"
	}
	if f.Has(PSH) {
		out += "P"
	}
	if out == "" {
		out = "."
	}
	return out
}

// Segment is the wire packet exchanged through netem. It carries the
// TCP-like header fields the stack actually uses: sequence/ack numbers,
// flags, advertised window and a timestamp option for RTT estimation.
type Segment struct {
	Src     string
	Dst     string
	Seq     uint32
	Ack     uint32
	Flags   Flags
	Window  uint32
	Payload []byte
	// TSval: sender timestamp (virtual ns). TSecr: echoed TSval of the
	// last segment received from the peer, 0 when none.
	TSval int64
	TSecr int64
}

func (s *Segment) Length() int {
	n := len(s.Payload)
	if s.Flags.Has(SYN) || s.Flags.Has(FIN) {
		n++
	}
	return n
}
