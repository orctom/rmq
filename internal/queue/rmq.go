package queue

import (
	"fmt"
	"os"
	"sync"

	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/utils"
)

var instance *rmq

type rmq struct {
	queues map[string]*Queue
	sync.Mutex
}

func RMQ() *rmq {
	return instance
}

func init() {
	log.Info().Msg("[rmq] init")
	instance = &rmq{
		queues: make(map[string]*Queue),
	}
	instance.init()
	log.Info().Msg("[rmq] initialized")
}

func (r *rmq) init() {
	if utils.IsNotExists(utils.QUEUES_PATH) {
		err := os.MkdirAll(utils.QUEUES_PATH, 0744)
		if err != nil {
			log.Panic().Err(err).Msgf("[init] failed to create %s", utils.QUEUES_PATH)
		}
	}

	dirEntries, err := os.ReadDir(utils.QUEUES_PATH)
	if err != nil {
		log.Panic().Err(err).Msgf("[init] failed to list files in %s", utils.QUEUES_PATH)
	}
	for _, entry := range dirEntries {
		if entry.IsDir() {
			name := entry.Name()
			r.queues[name] = NewQueue(name)
		}
	}
}

func (r *rmq) GetQueue(name string) *Queue {
	r.Lock()
	defer r.Unlock()
	queue, exists := r.queues[name]
	if !exists {
		queue = NewQueue(name)
		r.queues[name] = queue
	}
	return queue
}

func (r *rmq) Stats() map[string]*Stats {
	var stats = make(map[string]*Stats)
	for name, queue := range r.queues {
		stats[name] = queue.Stats()
	}
	return stats
}

func (r *rmq) Debug() {
	for _, queue := range r.queues {
		fmt.Println(queue.String())
	}
}
