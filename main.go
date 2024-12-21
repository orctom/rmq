package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/rs/zerolog/log"

	"orctom.com/rmq/internal/queue"
	"orctom.com/rmq/internal/ui"
	"orctom.com/rmq/internal/utils"
)

func startUI() {
	go ui.Start()
	fmt.Println("hello started")
}

func main() {
	// startUI()
	// testMmap()
	// time.Sleep(time.Second * 10)
	testID()
	// dummy()
}

func intToBytearray(num uint64) []byte {
	bytearray := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytearray, uint64(num))
	return bytearray
}

func bytearrayToInt(bytearray []byte) uint64 {
	return binary.LittleEndian.Uint64(bytearray)
}

func testMmap() {
	path := utils.ExpandHome("~/temp/dummy.txt")
	// os.Remove(path)
	mm, err := utils.NewMmap(path)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	// if err = mm.Append([]byte("hello")); err != nil {
	// 	log.Fatal().Err(err).Send()
	// }
	// if err = mm.Append([]byte(" world")); err != nil {
	// 	log.Fatal().Err(err).Send()
	// }
	// if err := mm.Write([]byte(" hi   "), 0); err != nil {
	// 	log.Fatal().Err(err).Send()
	// }
	err = mm.WriteAt([]byte("hello world"), 150, true)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	println("size:", mm.Size())
	// err = mm.Write([]byte("wanderful"), 0)
	// if err != nil {
	// 	log.Fatal().Err(err).Send()
	// }
	// println("size:", mm.Size())

	data := make([]byte, 200)
	err = mm.ReadAt(data, 0)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	println("data:", string(data), "!")

	raw, err := os.ReadFile(path)
	if err != nil {
		log.Fatal().Msgf("could not read back data: %+v", err)
	}
	fmt.Printf("[%s]\n", raw)
}

func testID() {
	q := queue.NewQueue("dummy")
	println(rand.Intn(100))

	go func() {
		// time.Sleep(2 * time.Second)
		// log.Debug().Msg(q.String())
		log.Debug().Msg("consumer started")
		for {
			msg := q.BGet()
			if msg == nil {
				time.Sleep(1000 * time.Millisecond)
				continue
			}
			t := decodeTime2(msg.Data)
			took := time.Since(t).String()
			log.Debug().Msgf("\t consumer \t\t %d, took %s", msg.ID, took)
			q.Ack(msg.Priority, msg.ID)
			// time.Sleep(200 * time.Millisecond)
		}
	}()

	go func() {
		log.Debug().Msg("producer started")
		for i := 0; i < 10; i++ {
			priority := queue.Priority(rand.Intn(3))
			// priority = queue.PRIORITY_NORMAL
			now := time.Now()
			data := encodeTime2(now)
			q.Put(queue.MessageData(data), priority)
		}
		time.Sleep(1 * time.Second)
		priority := queue.Priority(rand.Intn(3))
		q.Put(encodeTime2(time.Now()), priority)
	}()

	log.Info().Msg("wait before exit")
	time.Sleep(time.Second * 20)
	log.Debug().Msg(q.String())
	log.Info().Msg("exit")
}

func encodeTime1(t time.Time) []byte {
	b, err := cbor.Marshal(t)
	if err != nil {
		log.Error().Err(err).Send()
		return nil
	}
	return b
}

func decodeTime1(b []byte) time.Time {
	var t time.Time
	err := cbor.Unmarshal(b, &t)
	if err != nil {
		log.Error().Err(err).Send()
	}
	return t
}

func encodeTime2(t time.Time) []byte {
	b, err := t.MarshalBinary()
	if err != nil {
		log.Error().Err(err).Send()
		return nil
	}
	return b
}

func decodeTime2(b []byte) time.Time {
	var t time.Time
	err := t.UnmarshalBinary(b)
	if err != nil {
		log.Error().Err(err).Send()
	}
	return t
}

func dummy() {
	counter := queue.NewMetricsCounter()
	counter.MarkIns()
	log.Debug().Msgf("value %d", counter.Get())
	metrics := queue.NewMetrics(time.Second * 2)
	for i := 0; i < 20; i++ {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
		metrics.MarkIn(queue.PRIORITY_HIGH)
		metrics.MarkOut(queue.PRIORITY_HIGH)
	}
	log.Info().Msgf("metrics sizes: %v", metrics.Sizes())
	log.Info().Msgf("metrics rates: %v", metrics.Rates())
	time.Sleep(time.Second * 20)
}
