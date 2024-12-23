package queue

import "sync"

type rmq struct {
	queues map[string]*Queue
	sync.Mutex
}

func RMQ() *rmq {
	var once sync.Once
	var instance *rmq
	once.Do(func() {
		instance = &rmq{
			queues: make(map[string]*Queue),
		}
	})
	return instance
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
