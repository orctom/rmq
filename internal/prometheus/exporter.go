package prometheus

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

func StartExporter() {
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewRmqCollector())

	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	http.Handle("/metrics", handler)

	log.Info().Msg("metrics serving on port :7002")
	log.Info().Err(http.ListenAndServe(":7002", nil)).Send()
}
