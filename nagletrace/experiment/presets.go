package experiment

import (
	"time"

	"nagletrace/app"
	"nagletrace/transport"
)

// Preset names used by the CLI and web UI.
const (
	PresetClassicStall    = "classic-stall"  // Nagle + delayed ACK, split write
	PresetClientNoDelay   = "client-nodelay" // one-sided fix: tail moves to the server direction
	PresetFixedNoDelay    = "fixed-nodelay"  // TCP_NODELAY both ends
	PresetFixedQuickAck   = "fixed-quickack" // server TCP_QUICKACK
	PresetFixedWritev     = "fixed-writev"   // merged write
	PresetMixedWorkload   = "mixed-workload" // 90% writev, 10% split
	PresetPipelined       = "pipelined"
	PresetCorked          = "corked"
	PresetChunkedResponse = "chunked-response"
	PresetLossy           = "lossy"
)

func baseClient() app.ClientConfig {
	return app.ClientConfig{
		Pattern:  app.WriteSplit,
		Mode:     app.StopAndWait,
		Requests: 20,
		BodySize: 64,
	}
}

func baseServer() app.ServerConfig {
	return app.ServerConfig{
		Mode:     app.RespFull,
		RespSize: 128,
	}
}

func baseLink() LinkSpec {
	return LinkSpec{OneWayDelay: 500 * time.Microsecond, Seed: 42}
}

// Preset builds one named scenario.
func Preset(name string) Scenario {
	switch name {
	case PresetClassicStall:
		return Scenario{
			Name:       "Nagle + delayed ACK: split header/body write",
			ClientOpts: transport.DefaultOptions(),
			ServerOpts: transport.DefaultOptions(),
			Client:     baseClient(),
			Server:     baseServer(),
			Link:       baseLink(),
		}
	case PresetClientNoDelay:
		s := Preset(PresetClassicStall)
		s.Name = "One-sided fix: TCP_NODELAY on the client only — the tail moves to the response path"
		s.ClientOpts = transport.NoDelayOptions()
		return s
	case PresetFixedNoDelay:
		s := Preset(PresetClassicStall)
		s.Name = "Fix: TCP_NODELAY on both ends"
		s.ClientOpts = transport.NoDelayOptions()
		s.ServerOpts = transport.NoDelayOptions()
		return s
	case PresetFixedQuickAck:
		s := Preset(PresetClassicStall)
		s.Name = "Fix: TCP_QUICKACK on the server"
		s.ServerOpts = transport.QuickAckOptions()
		return s
	case PresetFixedWritev:
		s := Preset(PresetClassicStall)
		s.Name = "Fix: merged writev(header, body)"
		s.Client = baseClient()
		s.Client.Pattern = app.WriteMerged
		return s
	case PresetMixedWorkload:
		c := baseClient()
		c.Pattern = app.WriteMerged
		c.MixedSplitFraction = 0.1
		c.Requests = 50
		// Both receivers mostly re-enter quick-ACK mode on their own, as
		// real kernels do on idle paths; ~10% of requests still land on
		// the delayed-ACK timer and reproduce the 40ms tail.
		clientOpts := transport.DefaultOptions()
		clientOpts.QuickAckRearmRate = 0.9
		clientOpts.QuickAckRearmSeed = 11
		serverOpts := transport.DefaultOptions()
		serverOpts.QuickAckRearmRate = 0.9
		serverOpts.QuickAckRearmSeed = 23
		return Scenario{
			Name:       "Production shape: ~90% sub-ms, ~10% stuck at 40ms",
			ClientOpts: clientOpts,
			ServerOpts: serverOpts,
			Client:     c,
			Server:     baseServer(),
			Link:       baseLink(),
		}
	case PresetPipelined:
		c := baseClient()
		c.Mode = app.Pipeline
		c.Requests = 10
		// Merged writes: once the first request is in flight every later
		// request goes out immediately, and the receiver's every-second-
		// packet rule keeps ACKs coming — the pipeline hides the timer.
		c.Pattern = app.WriteMerged
		return Scenario{
			Name:       "Pipelined merged writes: the second-packet rule keeps ACKs flowing",
			ClientOpts: transport.DefaultOptions(),
			ServerOpts: transport.DefaultOptions(),
			Client:     c,
			Server:     baseServer(),
			Link:       baseLink(),
		}
	case PresetCorked:
		c := baseClient()
		c.Pattern = app.WriteCorked
		return Scenario{
			Name:       "TCP_CORK: header+body accumulated then flushed",
			ClientOpts: transport.DefaultOptions(),
			ServerOpts: transport.DefaultOptions(),
			Client:     c,
			Server:     baseServer(),
			Link:       baseLink(),
		}
	case PresetChunkedResponse:
		srv := baseServer()
		srv.Mode = app.RespChunked
		srv.RespSize = 300
		srv.ChunkSize = 50
		srv.ChunkDelay = 2 * time.Millisecond
		c := baseClient()
		c.Pattern = app.WriteMerged
		return Scenario{
			Name:       "Chunked server response: Nagle stalls on the server side",
			ClientOpts: transport.DefaultOptions(),
			ServerOpts: transport.DefaultOptions(),
			Client:     c,
			Server:     srv,
			Link:       baseLink(),
		}
	case PresetLossy:
		c := baseClient()
		c.Pattern = app.WriteMerged
		c.Requests = 12
		link := baseLink()
		link.LossRate = 0.15
		return Scenario{
			Name:       "15% packet loss: retransmission meets delayed ACK",
			ClientOpts: transport.DefaultOptions(),
			ServerOpts: transport.DefaultOptions(),
			Client:     c,
			Server:     baseServer(),
			Link:       link,
			MaxSimTime: 30 * time.Second,
		}
	default:
		return Preset(PresetClassicStall)
	}
}

// AllPresets in display order.
func AllPresets() []string {
	return []string{
		PresetClassicStall,
		PresetMixedWorkload,
		PresetClientNoDelay,
		PresetFixedNoDelay,
		PresetFixedQuickAck,
		PresetFixedWritev,
		PresetPipelined,
		PresetCorked,
		PresetChunkedResponse,
		PresetLossy,
	}
}
