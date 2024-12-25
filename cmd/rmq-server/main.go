package main

import (
	"context"
	"fmt"

	zmq "github.com/go-zeromq/zmq4"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx := context.Background()
	// Socket to talk to clients
	socket := zmq.NewRep(ctx)
	defer socket.Close()
	if err := socket.Listen("tcp://*:5555"); err != nil {
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
