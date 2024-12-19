package queue

import (
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
)

type Buffer struct {
	normChan   chan *Message
	highChan   chan *Message
	urgentChan chan *Message
	sentChan   chan *Message
	cond       *sync.Cond
}

func NewBuffer(normChan chan *Message, highChan chan *Message, urgentChan chan *Message, sentChan chan *Message) *Buffer {
	return &Buffer{
		normChan:   normChan,
		highChan:   highChan,
		urgentChan: urgentChan,
		sentChan:   sentChan,
		cond:       sync.NewCond(&sync.Mutex{}),
	}
}

func (bf *Buffer) String() string {
	return fmt.Sprintf("norm: %d, high: %d, urgent: %d, sent: %d", len(bf.normChan), len(bf.highChan), len(bf.urgentChan), len(bf.sentChan))
}

func (bf *Buffer) Put(msg *Message) {
	fmt.Println("Putting message:", msg)
	bf.cond.L.Lock()
	defer bf.cond.L.Unlock()

	switch msg.Priority {
	case PRIORITY_NORMAL:
		bf.normChan <- msg
		bf.cond.Signal()
	case PRIORITY_HIGH:
		bf.highChan <- msg
		bf.cond.Signal()
	case PRIORITY_URGENT:
		bf.urgentChan <- msg
		bf.cond.Signal()
	default:
		log.Error().Msgf("Invalid priority: %d", msg.Priority)
	}
}

func (bf *Buffer) Get() *Message {
	select {
	case msg := <-bf.urgentChan:
		bf.sentChan <- msg
		return msg
	default:
		select {
		case msg := <-bf.highChan:
			bf.sentChan <- msg
			return msg
		default:
			select {
			case msg := <-bf.normChan:
				bf.sentChan <- msg
				return msg
			default:
				return nil
			}
		}
	}
}

func (bf *Buffer) BGet() *Message {
	for {
		bf.cond.L.Lock()
		defer bf.cond.L.Unlock()

		select {
		case msg := <-bf.urgentChan:
			bf.sentChan <- msg
			return msg
		default:
			select {
			case msg := <-bf.highChan:
				bf.sentChan <- msg
				return msg
			default:
				select {
				case msg := <-bf.normChan:
					bf.sentChan <- msg
					return msg
				default:
					log.Info().Msg("waiting")
					bf.cond.Wait()
					log.Info().Msg("got notified, checking again")
					continue
				}
			}
		}
	}
}
