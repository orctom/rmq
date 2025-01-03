package main

import (
	"context"

	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"

	zmq "github.com/go-zeromq/zmq4"
	"orctom.com/rmq/internal/queue"
	"orctom.com/rmq/internal/statshttp"
)

func StartStatsHttp() {
	// default register
	// prometheus.MustRegister(statshttp.NewRmqCollector())
	// http.Handle("/metrics", promhttp.Handler())

	// blanck register
	registry := prometheus.NewRegistry()
	registry.MustRegister(statshttp.NewRmqCollector())
	metricsHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	http.Handle("/metrics", metricsHandler)

	http.HandleFunc("/stats", statshttp.StatsHandler)

	log.Info().Msg("[stats] serving on port :7002")
	log.Info().Err(http.ListenAndServe(":7002", nil)).Send()
}

func main() {
	go StartStatsHttp()

	ctx := context.Background()
	socket := zmq.NewRep(ctx)
	defer socket.Close()
	log.Info().Msg("[RMQ] serving on port :7001")
	if err := socket.Listen("tcp://*:7001"); err != nil {
		log.Panic().Err(err).Msg("listening")
	}

	for {
		msg, err := socket.Recv()
		if err != nil {
			log.Panic().Err(err).Msg("receiveing")
		}

		if err := socket.Send(queue.HandleRequest(msg.Bytes())); err != nil {
			log.Panic().Err(err).Msg("failed to send reply")
		}
	}
}
