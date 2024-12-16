package queue

type buffer struct {
	norm   chan *Message
	high   chan *Message
	urgent chan *Message
}

func NewBuffer() *buffer {
	return NewBufferOfSize(BUFFER_SIZE)
}

func NewBufferOfSize(size int) *buffer {
	return &buffer{
		norm:   make(chan *Message, size),
		high:   make(chan *Message, size),
		urgent: make(chan *Message, size),
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
		panic("Invalid priority")
	}
}

func (bf buffer) Get() *Message {
	select {
	case msg := <-bf.norm:
		return msg
	case msg := <-bf.high:
		return msg
	case msg := <-bf.urgent:
		return msg
	default:
		return nil
	}
}
