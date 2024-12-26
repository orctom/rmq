package main

import (
	"context"
	"fmt"

	zmq "github.com/go-zeromq/zmq4"
	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/prometheus"
)

func main() {
	go prometheus.StartExporter()

	ctx := context.Background()
	// Socket to talk to clients
	socket := zmq.NewRep(ctx)
	defer socket.Close()
	log.Info().Msg("RMQ serving on port :7001")
	if err := socket.Listen("tcp://*:7001"); err != nil {
		log.Panic().Err(err).Msg("listening")
	}

	for {
		msg, err := socket.Recv()
		if err != nil {
			log.Panic().Err(err).Msg("receiveing")
		}
		fmt.Println("Received ", msg)

		if err := socket.Send(msg); err != nil {
			log.Panic().Err(err).Msg("sending reply")
		}
	}
}
