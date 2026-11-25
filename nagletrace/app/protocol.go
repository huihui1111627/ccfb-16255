// Package app implements the application-level workloads that exercise
// the transport: classic request/response with a separate header write,
// a merged writev write, corked writes, pipelining and a server that
// streams a large response in small chunks.
package app

import (
	"bytes"
	"strconv"
)

// Tiny text protocol, deliberately human-readable in traces:
//
//	request:  "REQ <id>\n" + nBody bytes of 'x' + "\n"
//	response: "RESP <id>\n" + nResp bytes of 'y' + "END\n"
//
// Framing lives entirely in this package; transport only sees a byte
// stream.
const (
	reqPrefix  = "REQ "
	respPrefix = "RESP "
	endMarker  = "END\n"
)

// Reader reassembles the byte stream delivered by OnRead into framed
// messages. It handles segment boundaries that split a message.
type Reader struct {
	buf bytes.Buffer
}

// Push appends newly arrived bytes.
func (r *Reader) Push(b []byte) { r.buf.Write(b) }

// NextRequest pops one complete request: header line + body + trailing
// newline. Returns nil when more bytes are needed.
func (r *Reader) NextRequest(bodySize int) (id int, body []byte, ok bool) {
	raw := r.buf.Bytes()
	nl := bytes.IndexByte(raw, '\n')
	if nl < 0 {
		return 0, nil, false
	}
	header := string(raw[:nl])
	if len(header) <= len(reqPrefix) || header[:len(reqPrefix)] != reqPrefix {
		// resync: drop the bad line
		r.buf.Next(nl + 1)
		return 0, nil, false
	}
	id, err := strconv.Atoi(header[len(reqPrefix):])
	if err != nil {
		r.buf.Next(nl + 1)
		return 0, nil, false
	}
	total := nl + 1 + bodySize + 1
	if r.buf.Len() < total {
		return 0, nil, false
	}
	msg := r.buf.Next(total)
	body = msg[nl+1 : nl+1+bodySize]
	return id, body, true
}

// NextResponse pops a complete response (body ends at END marker).
func (r *Reader) NextResponse() (id int, body []byte, ok bool) {
	raw := r.buf.Bytes()
	nl := bytes.IndexByte(raw, '\n')
	if nl < 0 {
		return 0, nil, false
	}
	header := string(raw[:nl])
	if len(header) <= len(respPrefix) || header[:len(respPrefix)] != respPrefix {
		r.buf.Next(nl + 1)
		return 0, nil, false
	}
	id, err := strconv.Atoi(header[len(respPrefix):])
	if err != nil {
		r.buf.Next(nl + 1)
		return 0, nil, false
	}
	endIdx := bytes.Index(raw, []byte(endMarker))
	if endIdx < 0 {
		return 0, nil, false
	}
	total := endIdx + len(endMarker)
	msg := r.buf.Next(total)
	body = msg[nl+1 : endIdx]
	return id, body, true
}

func encodeRequest(id, bodySize int) (header, body []byte) {
	header = []byte(reqPrefix + strconv.Itoa(id) + "\n")
	body = make([]byte, bodySize+1)
	for i := 0; i < bodySize; i++ {
		body[i] = 'x'
	}
	body[bodySize] = '\n'
	return header, body
}

func encodeResponseHeader(id int) []byte {
	return []byte(respPrefix + strconv.Itoa(id) + "\n")
}

func encodeResponseEnd() []byte { return []byte(endMarker) }
