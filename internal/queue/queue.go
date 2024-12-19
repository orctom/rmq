package queue

import (
	"fmt"
)

type Consumer struct {
}

type Queue struct {
	Name         string
	buffer       *Buffer
	sentMessages *SentMessages
	stores       *Stores
}

func NewQueue(name string) *Queue {
	normChan := make(chan *Message, BUFFER_SIZE_DEFAULT)
	highChan := make(chan *Message, BUFFER_SIZE_DEFAULT)
	urgentChan := make(chan *Message, BUFFER_SIZE_DEFAULT)
	timeoutChan := make(chan *Message, 1000)
	sentChan := make(chan *Message, 1000)
	queue := &Queue{
		Name:         name,
		buffer:       NewBuffer(normChan, highChan, urgentChan, sentChan),
		sentMessages: NewSentMessages(TTL_1_MINUTE, timeoutChan),
		stores:       NewStores(name, normChan, highChan, urgentChan),
	}

	go func() {
		for {
			msg := <-timeoutChan
			queue.stores.Put(msg.Data, msg.Priority)
		}
	}()

	go func() {
		for {
			msg := <-sentChan
			queue.stores.UpdateStatus(msg.Priority, msg.ID, STATUS_SENT)
		}
	}()

	return queue
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue: %s\n  Buffer : %s\n  Sent   : %d\n  Stores : \n%s\n", q.Name, q.buffer.String(), q.sentMessages.Size(), q.stores)
}

func (q *Queue) Put(message MessageData, priority Priority) {
	q.stores.Put(message, priority)
}

func (q *Queue) Get() *Message {
	return q.buffer.Get()
}

func (q *Queue) BGet() *Message {
	return q.buffer.BGet()
}

func (q *Queue) Ack(id ID) {
	q.sentMessages.Ack(id)
}
