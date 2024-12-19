package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"time"

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

	// go func() {
	//  log.Debug().Msg("producer started")
	// 	for i := 0; i < 10; i++ {
	// 		priority := queue.Priority(rand.Intn(3))
	// 		// priority = queue.PRIORITY_NORMAL
	// 		q.Put(queue.MessageDataFromStr(fmt.Sprintf("[%d] hello world", i)), priority)
	// 	}
	// 	time.Sleep(1 * time.Second)
	// 	priority := queue.Priority(rand.Intn(3))
	// 	q.Put(queue.MessageDataFromStr(fmt.Sprintf("[%d] == done ==", 100)), priority)
	// }()

	go func() {
		// time.Sleep(2 * time.Second)
		// log.Debug().Msg(q.String())
		log.Debug().Msg("consumer started")
		for {
			msg := q.BGet()
			if msg == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			log.Debug().Msgf("\t consumer \t\t %s", msg)
			q.Ack(msg.ID)
			// time.Sleep(200 * time.Millisecond)
		}
	}()

	// items := make(chan string, 3)

	// go func() {
	// 	for i := 0; i < 10; i++ {
	// 		items <- fmt.Sprintf("[%d] %d", i, rand.Intn(10))
	// 		fmt.Printf("\t\t\tpushed %d\n", i)
	// 	}
	// }()
	// go func() {
	// 	for {
	// 		item := <-items
	// 		fmt.Println(item)
	// 		time.Sleep(1 * time.Second)
	// 	}
	// }()

	time.Sleep(time.Second * 10)
	// log.Debug().Msg(q.String())
	log.Info().Msg("exit")
}
