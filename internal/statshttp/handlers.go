package statshttp

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/queue"
	"orctom.com/rmq/internal/utils"
)

type RmqCollector struct {
	sizes *prometheus.Desc
	rates *prometheus.Desc
	mem   *prometheus.Desc
}

func NewRmqCollector() *RmqCollector {
	return &RmqCollector{
		sizes: prometheus.NewDesc(
			"rmq_sizes",
			"Shows sizes of queues",
			[]string{"queue", "priority"},
			nil,
		),
		rates: prometheus.NewDesc(
			"rmq_rates",
			"Shows in / out speed of queues",
			[]string{"queue", "type"},
			nil,
		),
		mem: prometheus.NewDesc(
			"rmq_mem",
			"Shows used mem",
			nil,
			nil,
		),
	}
}

func (c *RmqCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.sizes
	ch <- c.rates
	ch <- c.mem
}

func (c *RmqCollector) Collect(ch chan<- prometheus.Metric) {
	for name, stats := range queue.RMQ().Stats() {
		ch <- prometheus.MustNewConstMetric(c.sizes, prometheus.GaugeValue, float64(stats.Urgent), name, "urgent")
		ch <- prometheus.MustNewConstMetric(c.sizes, prometheus.GaugeValue, float64(stats.High), name, "high")
		ch <- prometheus.MustNewConstMetric(c.sizes, prometheus.GaugeValue, float64(stats.Norm), name, "norm")
		ch <- prometheus.MustNewConstMetric(c.rates, prometheus.GaugeValue, stats.In, name, "in")
		ch <- prometheus.MustNewConstMetric(c.rates, prometheus.GaugeValue, stats.Out, name, "out")
		log.Trace().Msg(stats.String())
	}
	ch <- prometheus.MustNewConstMetric(c.mem, prometheus.GaugeValue, float64(utils.GetMem()))
	log.Trace().Msgf("[mem] %s", utils.GetMemString())
}

func StatsHandler(w http.ResponseWriter, _ *http.Request) {
	stats := queue.RMQ().Stats()
	json.NewEncoder(w).Encode(stats)
}
