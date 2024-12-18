package queue

import "time"

type Consumer struct {
}

type Queue struct {
	Name         string
	buffer       buffer
	sentMessages *SentMessages
	stores       *Stores
}

func NewQueue(name string) *Queue {
	timeoutChan := make(chan *Message, 1000)
	sentChan := make(chan *Message, 1000)
	queue := &Queue{
		Name:         name,
		buffer:       *NewBuffer(sentChan),
		sentMessages: NewSentMessages(TTL_1_MINUTE, timeoutChan),
		stores:       NewStores(name),
	}

	go func() {
		for {
			msg, err := queue.stores.Pull(PRIORITY_URGENT)
			if err != nil || msg == nil {
				time.Sleep(time.Second * 2)
				continue
			}
			queue.buffer.Put(msg)
		}
	}()
	go func() {
		for {
			msg, err := queue.stores.Pull(PRIORITY_HIGH)
			if err != nil || msg == nil {
				time.Sleep(time.Second * 2)
				continue
			}
			queue.buffer.Put(msg)
		}
	}()
	go func() {
		for {
			msg, err := queue.stores.Pull(PRIORITY_NORMAL)
			if err != nil || msg == nil {
				time.Sleep(time.Second * 2)
				continue
			}
			queue.buffer.Put(msg)
		}
	}()

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

func (q *Queue) Put(message MessageData, priority Priority) {
	q.stores.Put(message, priority)
}

func (q *Queue) Get() *Message {
	msg := q.buffer.Get()
	return msg
}

func (q *Queue) Ack(id ID) {
	q.sentMessages.Ack(id)
}
