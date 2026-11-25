// Package clock provides a deterministic virtual clock for the whole
// simulated stack. There are no goroutines and no real time: every timer
// is a callback registered on a priority heap, and time only advances
// when Run / RunUntil drains the heap. This makes packet timelines
// (including the classic Nagle + delayed-ACK stall) exactly reproducible.
package clock

import (
	"container/heap"
	"time"
)

// Event is one scheduled timer callback.
type Event struct {
	time time.Duration
	fn   func(now time.Duration)
	seq  int64
}

type timerHeap struct {
	items []*Event
}

func (h timerHeap) Len() int { return len(h.items) }
func (h timerHeap) Less(i, j int) bool {
	if h.items[i].time != h.items[j].time {
		return h.items[i].time < h.items[j].time
	}
	return h.items[i].seq < h.items[j].seq
}
func (h timerHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *timerHeap) Push(x any)   { h.items = append(h.items, x.(*Event)) }
func (h *timerHeap) Pop() any {
	old := h.items
	n := len(old)
	it := old[n-1]
	h.items = old[:n-1]
	return it
}

// Clock is the virtual time source and timer wheel for one simulation.
type Clock struct {
	now     time.Duration
	timers  timerHeap
	nextSeq int64
}

// New returns a zeroed virtual clock.
func New() *Clock {
	c := &Clock{}
	heap.Init(&c.timers)
	return c
}

// Now is the current simulated time.
func (c *Clock) Now() time.Duration { return c.now }

// Schedule registers fn to run at absolute time t. A handle is returned
// so it can be cancelled.
func (c *Clock) Schedule(t time.Duration, fn func(now time.Duration)) *Event {
	c.nextSeq++
	e := &Event{time: t, fn: fn, seq: c.nextSeq}
	heap.Push(&c.timers, e)
	return e
}

// After schedules fn after delay d from now.
func (c *Clock) After(d time.Duration, fn func(now time.Duration)) *Event {
	return c.Schedule(c.now+d, fn)
}

// Cancel removes a pending timer. A timer that already fired is a no-op.
func (c *Clock) Cancel(e *Event) {
	if e == nil {
		return
	}
	for i, it := range c.timers.items {
		if it == e {
			heap.Remove(&c.timers, i)
			return
		}
	}
}

// Pending reports how many timers are queued.
func (c *Clock) Pending() int { return c.timers.Len() }

// Run advances until the heap is empty. maxTime is a safety net against
// runaway simulations; reaching it returns false.
func (c *Clock) Run(maxTime time.Duration) (completed bool) {
	for c.timers.Len() > 0 {
		next := c.timers.items[0]
		if next.time > maxTime {
			return false
		}
		heap.Pop(&c.timers)
		// A callback scheduled in the past (or for now) still jumps the
		// clock forward to its scheduled time — never backwards.
		if next.time > c.now {
			c.now = next.time
		}
		next.fn(c.now)
	}
	return true
}

// RunUntil drains every timer scheduled at or before deadline, then
// stops with the clock at the last fired event (which may equal
// deadline). Timers scheduled strictly after deadline remain pending.
func (c *Clock) RunUntil(deadline time.Duration) (completed bool) {
	for c.timers.Len() > 0 {
		next := c.timers.items[0]
		if next.time > deadline {
			return false
		}
		heap.Pop(&c.timers)
		if next.time > c.now {
			c.now = next.time
		}
		next.fn(c.now)
	}
	return true
}

// Advance jumps the clock without firing timers (used by tests); normally
// code should use After/Run instead.
func (c *Clock) Advance(d time.Duration) { c.now += d }
