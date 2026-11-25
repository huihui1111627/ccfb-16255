package app

import (
	"time"

	"nagletrace/clock"
	"nagletrace/transport"
)

// ResponseMode is how the server emits one response.
type ResponseMode string

const (
	// RespFull assembles the whole response and writes it in one call.
	RespFull ResponseMode = "full"
	// RespChunked emits header, then small body chunks separated by an
	// application-level delay, then the end marker.
	RespChunked ResponseMode = "chunked"
)

// ServerConfig describes the server workload.
type ServerConfig struct {
	Mode       ResponseMode
	RespSize   int
	ThinkDelay time.Duration // app processing time before the response
	ChunkSize  int
	ChunkDelay time.Duration // virtual time between body chunks
}

// ServerApp answers framed requests.
type ServerApp struct {
	clk      *clock.Clock
	cfg      ServerConfig
	conn     *transport.Conn
	rd       Reader
	bodySize int
}

func NewServer(clk *clock.Clock, cfg ServerConfig, clientBodySize int) *ServerApp {
	return &ServerApp{clk: clk, cfg: cfg, bodySize: clientBodySize}
}

func (a *ServerApp) OnConnected(c *transport.Conn) { a.conn = c }

func (a *ServerApp) OnRead(c *transport.Conn, b []byte) {
	a.rd.Push(b)
	for {
		id, _, ok := a.rd.NextRequest(a.bodySize)
		if !ok {
			return
		}
		a.scheduleResponse(id)
	}
}

func (a *ServerApp) scheduleResponse(id int) {
	if a.cfg.ThinkDelay > 0 {
		a.clk.After(a.cfg.ThinkDelay, func(time.Duration) { a.respond(id) })
		return
	}
	a.respond(id)
}

func (a *ServerApp) respond(id int) {
	header := encodeResponseHeader(id)
	end := encodeResponseEnd()
	switch a.cfg.Mode {
	case RespChunked:
		// header first — may itself be held by Nagle while the client's
		// ACK is outstanding.
		_, _ = a.conn.Write(header)
		remaining := a.cfg.RespSize
		chunkSeq := 0
		var emitNext func()
		emitNext = func() {
			if remaining <= 0 {
				_, _ = a.conn.Write(end)
				return
			}
			n := a.cfg.ChunkSize
			if n > remaining {
				n = remaining
			}
			chunk := make([]byte, n)
			for i := range chunk {
				chunk[i] = 'y'
			}
			_, _ = a.conn.Write(chunk)
			remaining -= n
			chunkSeq++
			if remaining > 0 {
				a.clk.After(a.cfg.ChunkDelay, func(time.Duration) { emitNext() })
			} else {
				a.clk.After(a.cfg.ChunkDelay, func(time.Duration) {
					_, _ = a.conn.Write(end)
				})
			}
		}
		emitNext()
	default:
		body := make([]byte, a.cfg.RespSize)
		for i := range body {
			body[i] = 'y'
		}
		_, _ = a.conn.WriteMulti(header, body, end)
	}
}

func (a *ServerApp) OnClose(*transport.Conn) {}
