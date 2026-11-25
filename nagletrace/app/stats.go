package app

import (
	"sort"
	"time"
)

// Summarize builds the latency distribution. TailThreshold defines what
// counts as the long tail (defaults to the delayed-ACK scale).
func (l *Latency) Summarize(tailThreshold time.Duration) Stats {
	if len(l.Samples) == 0 {
		return Stats{}
	}
	vals := make([]time.Duration, len(l.Samples))
	var sum time.Duration
	for i, s := range l.Samples {
		vals[i] = s.Latency
		sum += s.Latency
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	n := len(vals)
	tail := 0
	for _, v := range vals {
		if v >= tailThreshold {
			tail++
		}
	}
	return Stats{
		Count:        n,
		Min:          vals[0],
		P50:          vals[pct(n, 50)],
		P90:          vals[pct(n, 90)],
		P99:          vals[pct(n, 99)],
		Max:          vals[n-1],
		Mean:         sum / time.Duration(n),
		TailCount:    tail,
		TailFraction: float64(tail) / float64(n),
	}
}
func pct(n, p int) int {
	idx := n * p / 100
	if idx >= n {
		idx = n - 1
	}
	return idx
}
