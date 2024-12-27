package main

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/prometheus"
	"orctom.com/rmq/internal/queue"
	"orctom.com/rmq/internal/utils"
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

func consumer() {
	rmq := queue.RMQ()
	q := rmq.GetQueue("dummy")
	msg := q.Get()
	if msg == nil {
		log.Info().Msg("failed to get message")
		return
	}
	fmt.Println(msg)
	ts := utils.DecodeTime(msg.Data)
	took := time.Since(ts)
	log.Debug().Msgf("received message: [%d] <%s> %s took: %v", msg.ID, msg.Priority, ts, took)
	q.Ack(msg.Priority, msg.ID)
	time.Sleep(time.Second * 2)
	log.Debug().Msg("consumer done")
}

func dummy() {
	store := queue.NewStore(queue.NewKey("dummy", queue.PRIORITY_NORM, queue.ID(0)))
	log.Info().Msg(store.String())
}

func main() {
	log.Info().Msg("starting")
	// StartExporter()
	dummy()
	consumer()
	log.Info().Msg("exiting")
}
