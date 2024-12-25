/*
metrics
|-- queues (option 1)
|  |-- urgent total / in rate / out rate
|  |-- high total / in rate / out rate
|  |-- norm total / in rate / out rate
|-- queues (option 2, prefered)
|  |-- in rate
|  |-- out rata
|  |-- urgent total
|  |-- high total
|  |-- norm total
|-- mem
   |-- xx M/G
|-- disk
   |-- xx M/G
*/

package prometheus

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var (
	queues = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   "mysql",
		Name:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Help:        "Number of mysql connections open.",
		ConstLabels: prometheus.Labels{"destination": "primary"},
	},
		func() float64 {
			log.Info().Msg("in queues gauge fu")
			return float64(time.Now().Unix())
		},
	)
	opsQueued = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "our_company",
			Subsystem: "blob_storage",
			Name:      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Help:      "Number of blob storage operations waiting to be processed, partitioned by user and type.",
		},
		[]string{
			// Which user has requested the operation?
			"queue",
			// Of what type is the operation?
			"priority",
		},
	)
)

func StartExporter() {

	opsQueued.WithLabelValues("dummy-queue", "urgent").Set(100)
	opsQueued.WithLabelValues("dummy-queue", "high").Set(30)
	opsQueued.WithLabelValues("dummy-queue", "norm").Set(10)
	opsQueued.WithLabelValues("dummy-queue", "in").Set(12.235)
	opsQueued.WithLabelValues("dummy-queue", "out").Set(4562.232)

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
