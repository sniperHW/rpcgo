package rpcgo

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

var timerPool = sync.Pool{
	New: func() interface{} { return &Timer{} },
}

type Timer struct {
	deadline time.Time
	idx      int
	fn       func()
	fired    atomic.Bool
}

func (t *Timer) Stop() bool {
	if t.fired.CompareAndSwap(false, true) {
		return true
	} else {
		return false
	}
}

func (t *Timer) call() {
	if t.fired.CompareAndSwap(false, true) {
		t.fn()
	}
}

type timedHeap struct {
	timers     []*Timer
	timer      *time.Timer
	checkTimer bool
}

func (h timedHeap) Len() int {
	return len(h.timers)
}
func (h timedHeap) Less(i, j int) bool {
	return h.timers[i].deadline.Before(h.timers[j].deadline)
}
func (h timedHeap) Swap(i, j int) {
	h.timers[i], h.timers[j] = h.timers[j], h.timers[i]
	h.timers[i].idx = i
	h.timers[j].idx = j
}

func (h *timedHeap) Push(x interface{}) {
	h.timers = append(h.timers, x.(*Timer))
	n := len(h.timers)
	h.timers[n-1].idx = n - 1
}

func (h *timedHeap) Pop() interface{} {
	old := h.timers
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.timers = old[0 : n-1]
	return x
}

func (c *Client) afterFunc(d time.Duration, fn func()) *Timer {
	c.Lock()
	defer c.Unlock()
	t := timerPool.Get().(*Timer)
	t.deadline = time.Now().Add(d)
	t.fn = fn
	t.fired.Store(false)
	head := c.timers[0]
	heap.Push(&c.timedHeap, t)
	if !c.checkTimer {
		if t == c.timers[0] || head != t {
			//新定时器是时间最近的定时器
			if c.timer == nil {
				c.timer = time.AfterFunc(d, func() {
					c.Lock()
					c.checkTimer = true
					now := time.Now()
					for len(c.timers) > 0 {
						near := c.timers[0]
						if now.After(near.deadline) {
							heap.Pop(&c.timedHeap)
							c.Unlock()
							near.call()
							c.Lock()
							timerPool.Put(near)
						} else {
							break
						}
					}
					if len(c.timers) > 0 {
						c.timer.Reset(time.Until(c.timers[0].deadline))
					}
					c.checkTimer = false
					c.Unlock()
				})
			} else {
				if !c.timer.Stop() {
					<-c.timer.C
				}
				c.timer.Reset(d)
			}
		}
	}
	return t
}
