package queue

type RMQ struct {
	queues map[string]*Queue
}

func (rmq *RMQ) push(queueName string, payload []byte) {
	if q, exists := rmq.queues[queueName]; !exists {
		q = NewQueue(queueName)
		rmq.queues[queueName] = q
	}
}
