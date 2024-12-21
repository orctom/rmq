package queue

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type MetricsCounter struct {
	total int64
	ins   int64
	outs  int64
	sync.Mutex
}

func NewMetricsCounter() *MetricsCounter {
	return &MetricsCounter{
		total: 0,
		ins:   0,
		outs:  0,
	}
}

func (c *MetricsCounter) String() string {
	return fmt.Sprintf("total: %d, ins: %d, outs: %d", c.total, c.ins, c.outs)
}

func (c *MetricsCounter) Set(val int64) {
	c.Lock()
	defer c.Unlock()
	c.total = val
}

func (c *MetricsCounter) MarkIns() {
	c.Lock()
	defer c.Unlock()
	c.total++
	c.ins++
}

func (c *MetricsCounter) MarkOuts() {
	c.Lock()
	defer c.Unlock()
	c.total--
	c.outs++
}

func (c *MetricsCounter) MarkInsAndGet() int64 {
	c.Lock()
	defer c.Unlock()
	c.total++
	c.ins++
	return c.total
}

func (c *MetricsCounter) MarkOutsAndGet() int64 {
	c.Lock()
	defer c.Unlock()
	c.total--
	c.outs++
	return c.total
}

func (c *MetricsCounter) Get() int64 {
	c.Lock()
	defer c.Unlock()
	return c.total
}

func (c *MetricsCounter) Reset() {
	c.Lock()
	defer c.Unlock()
	c.total = 0
	c.ins = 0
	c.outs = 0
}

func (c *MetricsCounter) GetInsAndReset() int64 {
	c.Lock()
	defer c.Unlock()
	val := c.ins
	c.ins = 0
	return val
}

func (c *MetricsCounter) GetOutsAndReset() int64 {
	c.Lock()
	defer c.Unlock()
	val := c.outs
	c.outs = 0
	return val
}

type InOutRates struct {
	in  float64
	out float64
}

func (r *InOutRates) String() string {
	return fmt.Sprintf("in: %.1f/s, out: %.1f/s", r.in, r.out)
}

type Metrics struct {
	counters map[Priority]*MetricsCounter
	interval time.Duration
	rates    *map[Priority]*InOutRates
}

func NewMetrics(interval time.Duration) *Metrics {
	meter := &Metrics{
		counters: make(map[Priority]*MetricsCounter),
		interval: interval,
	}
	meter.counters[PRIORITY_URGENT] = NewMetricsCounter()
	meter.counters[PRIORITY_HIGH] = NewMetricsCounter()
	meter.counters[PRIORITY_NORM] = NewMetricsCounter()
	go meter.intervalChecker()
	return meter
}

func (m *Metrics) intervalChecker() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rates := make(map[Priority]*InOutRates)
			for priority, counter := range m.counters {
				ins := counter.GetInsAndReset()
				in := float64(ins) / m.interval.Seconds()
				outs := counter.GetOutsAndReset()
				out := float64(outs) / m.interval.Seconds()
				rates[priority] = &InOutRates{
					in:  in,
					out: out,
				}
				log.Trace().Msgf("<%s> %s, in: %.1f/s, out: %.1f/s", priority, counter, in, out)
			}
			m.rates = &rates
		}
	}
}

func (m *Metrics) SetSize(priority Priority, size int64) {
	m.counters[priority].Set(size)
}

func (m *Metrics) MarkIn(priority Priority) {
	m.counters[priority].MarkIns()
}

func (m *Metrics) MarkOut(priority Priority) {
	m.counters[priority].MarkOuts()
}

func (m *Metrics) Rates() map[Priority]*InOutRates {
	return *m.rates
}

func (m *Metrics) Sizes() map[Priority]int64 {
	sizes := make(map[Priority]int64)
	for priority, counter := range m.counters {
		sizes[priority] = counter.Get()
	}
	return sizes
}

func (m *Metrics) Size(priority Priority) int64 {
	return m.counters[priority].Get()
}

func (m *Metrics) String() string {
	return strings.Join([]string{
		fmt.Sprintf("<%s> %s", PRIORITY_URGENT, m.counters[PRIORITY_URGENT].String()),
		fmt.Sprintf("<%s> %s", PRIORITY_HIGH, m.counters[PRIORITY_HIGH].String()),
		fmt.Sprintf("<%s> %s", PRIORITY_NORM, m.counters[PRIORITY_NORM].String()),
	}, ", ")
}
