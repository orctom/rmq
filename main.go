package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/exp/rand"

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

	go func() {
		for i := 0; i < 10; i++ {
			priority := queue.Priority(rand.Intn(3))
			q.Put(queue.MessageDataFromStr(fmt.Sprintf("[%d] hello world", i)), priority)
		}
		time.Sleep(10 * time.Second)
		priority := queue.Priority(rand.Intn(3))
		q.Put(queue.MessageDataFromStr(fmt.Sprintf("[%d] ======= done =======", 100)), priority)
	}()

	go func() {
		for {
			msg := q.Get()
			if msg == nil {
				time.Sleep(5 * time.Second)
				continue
			}
			fmt.Println(msg)
			time.Sleep(1 * time.Second)
		}
	}()

	time.Sleep(time.Second * 30)
}
