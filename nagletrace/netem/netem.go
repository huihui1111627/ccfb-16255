// Package netem is a lossy in-process link between two transport
// endpoints. Each direction has its own one-way delay and loss rate,
// so the simulated RTT is delayAB + delayBA. Delivery goes through the
// shared virtual clock, which makes every drop and retransmission land
// at a deterministic timestamp.
package netem

import (
	"math/rand"
	"time"

	"nagletrace/clock"
)

// LinkConfig describes one direction of the link.
type LinkConfig struct {
	Delay       time.Duration // one-way propagation + queueing baseline
	Jitter      time.Duration // uniform delay added in [0, Jitter)
	LossRate    float64       // probability a segment is dropped
	Duplicate   float64       // probability a segment is delivered twice
	ReorderRate float64       // probability a segment gets an extra delay slot
	ReorderGap  time.Duration
}

// Handler receives a delivered packet.
type Handler func(pkt any)

// Network is a two-host link. Endpoints register by address.
type Network struct {
	clk       *clock.Clock
	rng       *rand.Rand
	links     map[string]map[string]LinkConfig // src -> dst -> config
	handlers  map[string]Handler
	stats     map[string]*DirStats
	delivered int64
	dropped   int64
}

// DirStats counters for one direction.
type DirStats struct {
	Sent       int64
	Delivered  int64
	Dropped    int64
	Duplicated int64
}

// New builds a network. rngSeed makes runs reproducible.
func New(clk *clock.Clock, rngSeed int64) *Network {
	return &Network{
		clk:      clk,
		rng:      rand.New(rand.NewSource(rngSeed)),
		links:    map[string]map[string]LinkConfig{},
		handlers: map[string]Handler{},
		stats:    map[string]*DirStats{},
	}
}

// Connect registers a host and the per-direction link configs.
func (n *Network) Connect(addr string, h Handler) {
	n.handlers[addr] = h
}

// SetLink configures src->dst. Missing configs default to zero delay.
func (n *Network) SetLink(src, dst string, cfg LinkConfig) {
	if n.links[src] == nil {
		n.links[src] = map[string]LinkConfig{}
	}
	n.links[src][dst] = cfg
	n.stats[src+"->"+dst] = &DirStats{}
}

func (n *Network) link(src, dst string) LinkConfig {
	if m, ok := n.links[src]; ok {
		return m[dst]
	}
	return LinkConfig{}
}

func (n *Network) dirStats(src, dst string) *DirStats {
	key := src + "->" + dst
	s := n.stats[key]
	if s == nil {
		s = &DirStats{}
		n.stats[key] = s
	}
	return s
}

// Send hands a segment to the link. The segment is dropped, delayed,
// possibly duplicated or reordered, then delivered to the dst handler.
func (n *Network) Send(src, dst string, pkt any) {
	cfg := n.link(src, dst)
	st := n.dirStats(src, dst)
	st.Sent++

	if n.rng.Float64() < cfg.LossRate {
		st.Dropped++
		n.dropped++
		return
	}
	delay := cfg.Delay
	if cfg.Jitter > 0 {
		delay += time.Duration(n.rng.Int63n(int64(cfg.Jitter)))
	}
	if cfg.ReorderRate > 0 && n.rng.Float64() < cfg.ReorderRate {
		delay += cfg.ReorderGap
	}
	n.clk.After(delay, func(now time.Duration) {
		h, ok := n.handlers[dst]
		if !ok {
			return
		}
		st.Delivered++
		n.delivered++
		h(pkt)
		if cfg.Duplicate > 0 && n.rng.Float64() < cfg.Duplicate {
			n.clk.After(cfg.Delay/2+1, func(time.Duration) {
				st.Duplicated++
				h(pkt)
			})
		}
	})
}

// Stats returns per-direction counters.
func (n *Network) Stats() map[string]DirStats {
	out := make(map[string]DirStats, len(n.stats))
	for k, v := range n.stats {
		out[k] = *v
	}
	return out
}

// Totals across all directions.
func (n *Network) Totals() (delivered, dropped int64) {
	return n.delivered, n.dropped
}
