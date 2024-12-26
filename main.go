package main

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/prometheus"
	"orctom.com/rmq/internal/queue"
)

func StartExporter() {
	prometheus.StartExporter()
}

func afterFunc() {
	// log.Debug().Msg("started")
	// time.AfterFunc(time.Second * 5, func() {
	// 	log.Debug().Msg("in after-func")
	// })

	// log.Debug().Msg("wait")
	// time.Sleep(time.Second * 10)
	// log.Debug().Msg("ended")
}

func dummy() {
	queue.RMQ()
	time.Sleep(time.Second * 5)
	queue.RMQ()
	fmt.Println("done")
}

func main() {
	log.Info().Msg("starting")
	StartExporter()
	// dummy()
	log.Info().Msg("exiting")
}
