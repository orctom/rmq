package main

import (
	"encoding/binary"
	"fmt"
	"os"

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
	stores := queue.NewStores("dummy")
	stores.Put(queue.NewMessageDataFromStr("1 a"), queue.PRIORITY_NORMAL)
	stores.Put(queue.NewMessageDataFromStr("2 bb"), queue.PRIORITY_HIGH)
	stores.Put(queue.NewMessageDataFromStr("3 ccc"), queue.PRIORITY_URGENT)
	stores.Put(queue.NewMessageDataFromStr("4 dddd"), queue.PRIORITY_URGENT)
	stores.Put(queue.NewMessageDataFromStr("5 eeeee"), queue.PRIORITY_HIGH)

	stores.Debug()
	println("-------------------------------------------------------------------------")
	for i := 0; i < 10; i++ {
		msg, err := stores.Get()
		if err != nil {
			log.Debug().Err(err).Send()
		}
		if msg == nil {
			println("no more messages")
			break
		}
		fmt.Printf("%d > [%s]\n", i, msg)
	}

	println("-------------------------------------------------------------------------")
	stores.Debug()
}
