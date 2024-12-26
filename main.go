package main

import (
	"orctom.com/rmq/internal/prometheus"
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

func main() {
	StartExporter()

}
