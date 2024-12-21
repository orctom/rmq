package queue

import "sync"

type RMQ struct {
	queues map[string]*Queue
	sync.Mutex
}

func NewRMQ() *RMQ {
	return &RMQ{
		queues: make(map[string]*Queue),
	}
}

func (rmq *RMQ) Put(name string, data MessageData, priority Priority) {
	q, exists := rmq.queues[name]
	rmq.Lock()
	if !exists {
		q, exists = rmq.queues[name]
		if !exists {
			q = NewQueue(name)
			rmq.queues[name] = q
		}
	}
	rmq.Unlock()
	q.Put(data, priority)
}

