package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"orctom.com/rmq/internal/queue"
	"orctom.com/rmq/internal/ui"
)

func startUI() {
	go ui.Start()
	fmt.Println("hello started")
}

func main() {
	// startUI()
	// time.Sleep(time.Second * 10)
	testQueue()
	// dummy()
	// testOnce()
}

func testQueue() {
	rmq := queue.RMQ()
	q := rmq.GetQueue("dummy")
	// q := queue.NewQueue("dummy")
	println(rand.Intn(100))

	counter := queue.NewMetricsCounter()
	// go func() {
	// 	time.Sleep(10 * time.Second)
	// 	log.Debug().Msg(q.String())
	// 	log.Debug().Msg("consumer started")
	// 	for {
	// 		msg := q.BGet()
	// 		if msg == nil {
	// 			time.Sleep(1000 * time.Millisecond)
	// 			log.Debug().Msg("\t\t\t===")
	// 			continue
	// 		}
	// 		// t := decodeTime2(msg.Data)
	// 		// took := time.Since(t).String()
	// 		// log.Debug().Msgf("\t consumer \t\t %d, took %s", msg.ID, took)
	// 		q.Ack(msg.Priority, msg.ID)
	// 		counter.MarkIns()
	// 		// time.Sleep(200 * time.Millisecond)
	// 	}
	// }()

	go func() {
		// time.Sleep(2 * time.Second)
		log.Debug().Msg("producer started")
		for i := 0; i < 5_000_000; i++ {
			priority := queue.Priority(rand.Intn(3))
			priority = queue.PRIORITY_NORM
			now := time.Now()
			data := encodeTime2(now)
			q.Put(queue.MessageData(data), priority)
		}
		time.Sleep(1 * time.Second)
		priority := queue.Priority(rand.Intn(3))
		q.Put(encodeTime2(time.Now()), priority)
	}()

	log.Info().Msg("wait before exit")
	time.Sleep(time.Second * 30)
	log.Debug().Msg(q.String())
	log.Info().Msg(counter.String())
	log.Info().Msg("exit")
}

func dummy() {
	counter := queue.NewMetricsCounter()
	counter.MarkIns()
	log.Debug().Msgf("value %d", counter.Get())
	metrics := queue.NewMetrics("dummy", time.Second*2)
	for i := 0; i < 20; i++ {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
		metrics.MarkIn(queue.PRIORITY_HIGH)
		metrics.MarkOut(queue.PRIORITY_HIGH)
	}
	log.Info().Msgf("metrics sizes: %v", metrics.Sizes())
	log.Info().Msgf("metrics rates: %v", metrics.Rates())
	time.Sleep(time.Second * 20)
}

func testOnce() {
	log.Info().Msg("start test once")
	var mu sync.Mutex
	var val = 0
	var fn = func() {
		if val >= 25 {
			mu.Lock()
			if val >= 25 {
				fmt.Printf("\t%d -> 0\n", val)
				val = 0
			}
			mu.Unlock()
		}
		// mu.Lock()
		val++
		// mu.Unlock()
	}
	for i := 0; i < 100; i++ {
		go fn()
	}
	time.Sleep(time.Second * 2)
	log.Info().Msgf("end test once, val: %d", val)
}
