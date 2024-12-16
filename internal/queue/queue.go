package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
)

type Consumer struct {
}

type Queue struct {
	Name   string
	InID   uint64
	OutID  uint64
	AckID  uint64
	ins    chan []byte
	outs   chan []byte
	reader *Store
	writer *Store
}

func NewQueue(name string) *Queue {
	queueDir := filepath.Join(BASE_PATH, "data", name)
	os.MkdirAll(queueDir, 0644)
	id, err := findID(queueDir)
	if err != nil {
		log.Error().Err(err).Send()
		id = 0
	}
	// reader := NewStore(queueDir)
	fmt.Println(id)
	// fmt.Println(reader)
	return &Queue{
		Name:  name,
		InID:  0,
		OutID: 0,
		AckID: 0,
		ins:   make(chan []byte, 10000),
		outs:  make(chan []byte, 10000),
	}
}

func findID(path string) (uint64, error) {
	var filename string = ""
	err := filepath.Walk(path, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !fi.IsDir() && strings.HasSuffix(fi.Name(), ".index") {
			name := strings.TrimRight(filepath.Base(p), filepath.Ext(p))
			if filename == "" || name > filename {
				filename = name
			}
		}
		return nil
	})
	if filename == "" {
		return 0, err
	}

	if err != nil {
		fmt.Println("遍历目录时出错:", err)
	}
	return 0, err
}

func (q *Queue) Push(message []byte) {
	q.ins <- message
}

func (q *Queue) Subscribe() {

}

func Beginning() {
	file, err := os.OpenFile("test.txt", os.O_RDWR, 0644)

	if err != nil {
		log.Fatal().Msgf("failed opening file: %s", err)
	}
	defer file.Close()

	len, err := file.WriteAt([]byte{'G'}, 0) // Write at 0 beginning
	if err != nil {
		log.Fatal().Msgf("failed writing to file: %s", err)
	}
	fmt.Printf("\nLength: %d bytes", len)
	fmt.Printf("\nFile Name: %s", file.Name())
}
