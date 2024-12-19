package queue

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

type Buffer struct {
	normChan   chan *Message
	highChan   chan *Message
	urgentChan chan *Message
	sentChan   chan *Message
}

func NewBuffer(normChan chan *Message, highChan chan *Message, urgentChan chan *Message, sentChan chan *Message) *Buffer {
	return &Buffer{
		normChan:   normChan,
		highChan:   highChan,
		urgentChan: urgentChan,
		sentChan:   sentChan,
	}
}

func (bf Buffer) String() string {
	return fmt.Sprintf("norm: %d, high: %d, urgent: %d, sent: %d", len(bf.normChan), len(bf.highChan), len(bf.urgentChan), len(bf.sentChan))
}

func (bf Buffer) Put(msg *Message) {
	switch msg.Priority {
	case PRIORITY_NORMAL:
		bf.normChan <- msg
	case PRIORITY_HIGH:
		bf.highChan <- msg
	case PRIORITY_URGENT:
		bf.urgentChan <- msg
	default:
		log.Error().Msgf("Invalid priority: %d", msg.Priority)
	}
}

func (bf Buffer) Get() *Message {
	select {
	case msg := <-bf.normChan:
		bf.sentChan <- msg
		return msg
	case msg := <-bf.highChan:
		bf.sentChan <- msg
		return msg
	case msg := <-bf.urgentChan:
		bf.sentChan <- msg
		return msg
	default:
		return nil
	}
}
