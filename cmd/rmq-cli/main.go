package main

import (
	"context"
	"fmt"
	"time"

	zmq "github.com/go-zeromq/zmq4"
	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/utils"
)

func main() {
	ctx := context.Background()
	socket := zmq.NewReq(ctx, zmq.WithDialerRetry(time.Second))
	defer socket.Close()

	fmt.Printf("Connecting to hello world server...")
	if err := socket.Dial("tcp://localhost:5555"); err != nil {
		log.Panic().Err(err).Msg("dialing")
	}

	for i := 0; i < 10; i++ {
		m := zmq.NewMsgFrom(utils.EncodeTime(time.Now()))
		if err := socket.Send(m); err != nil {
			log.Panic().Err(err).Msg("sending")
		}

		r, err := socket.Recv()
		if err != nil {
			log.Panic().Err(err).Msg("receiving")
		}
		t := utils.DecodeTime(r.Bytes())
		fmt.Println("received ", time.Since(t))
	}
	fmt.Printf("Done.\n")
}
