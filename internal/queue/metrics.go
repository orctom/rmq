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
	return fmt.Sprintf("size: %d", c.total)
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

type Stats struct {
	Name   string
	Urgent int64
	High   int64
	Norm   int64
	In     float64
	Out    float64
}

func (s *Stats) String() string {
	return fmt.Sprintf("[%s] urgent: %d, high: %d, norm: %d, in: %.1f/s, out: %.1f/s", s.Name, s.Urgent, s.High, s.Norm, s.In, s.Out)
}

type Metrics struct {
	name     string
	counters map[Priority]*MetricsCounter
	interval time.Duration
	stats    *Stats
}

func NewMetrics(name string, interval time.Duration) *Metrics {
	log.Info().Msgf("create metrics: %s, %v", name, interval)
	counters := map[Priority]*MetricsCounter{
		PRIORITY_URGENT: NewMetricsCounter(),
		PRIORITY_HIGH:   NewMetricsCounter(),
		PRIORITY_NORM:   NewMetricsCounter(),
	}
	m := &Metrics{
		name:     name,
		counters: counters,
		interval: interval,
		stats:    &Stats{Name: name},
	}
	go m.intervalChecker()

	return m
}

func (m *Metrics) intervalChecker() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var ins, outs int64 = 0, 0
			for priority, counter := range m.counters {
				switch priority {
				case PRIORITY_URGENT:
					m.stats.Urgent = counter.total
				case PRIORITY_HIGH:
					m.stats.High = counter.total
				case PRIORITY_NORM:
					m.stats.Norm = counter.total
				}

				ins += counter.GetInsAndReset()
				outs += counter.GetOutsAndReset()
			}
			duration := m.interval.Seconds()
			m.stats.In = float64(ins) / duration
			m.stats.Out = float64(outs) / duration
			log.Debug().Msg(m.stats.String())
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

func (m *Metrics) Size(priority Priority) int64 {
	return m.counters[priority].Get()
}

func (m *Metrics) GetStats() *Stats {
	return m.stats
}

func (m *Metrics) String() string {
	return strings.Join([]string{
		fmt.Sprintf("\n  <%s> %s", PRIORITY_URGENT, m.counters[PRIORITY_URGENT].String()),
		fmt.Sprintf("\n  <%s> %s", PRIORITY_HIGH, m.counters[PRIORITY_HIGH].String()),
		fmt.Sprintf("\n  <%s> %s", PRIORITY_NORM, m.counters[PRIORITY_NORM].String()),
	}, "")
}
