package queue

import "github.com/rs/zerolog/log"

type buffer struct {
	norm     chan *Message
	high     chan *Message
	urgent   chan *Message
	sentChan chan *Message
}

func NewBuffer(sentChan chan *Message) *buffer {
	return NewBufferOfSize(BUFFER_SIZE_DEFAULT, sentChan)
}

func NewBufferOfSize(size int, sentChan chan *Message) *buffer {
	return &buffer{
		norm:     make(chan *Message, size),
		high:     make(chan *Message, size),
		urgent:   make(chan *Message, size),
		sentChan: sentChan,
	}
}

func (bf buffer) Put(msg *Message) {
	switch msg.Priority {
	case PRIORITY_NORMAL:
		bf.norm <- msg
	case PRIORITY_HIGH:
		bf.high <- msg
	case PRIORITY_URGENT:
		bf.urgent <- msg
	default:
		log.Error().Msgf("Invalid priority: %d", msg.Priority)
	}
}

func (bf buffer) Get() *Message {
	select {
	case msg := <-bf.norm:
		bf.sentChan <- msg
		return msg
	case msg := <-bf.high:
		bf.sentChan <- msg
		return msg
	case msg := <-bf.urgent:
		bf.sentChan <- msg
		return msg
	default:
		return nil
	}
}
