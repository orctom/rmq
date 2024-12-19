package queue

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

type ID int64

func (id ID) String() string {
	return fmt.Sprintf("%0*d", 20, id)
}

// --------------------------------- message id ---------------------------------

type MessageMeta struct {
	ID     ID
	Offset int64
	Length int64
	Status Status
}

func NewMessageMeta(id ID, offset int64, length int64) *MessageMeta {
	return &MessageMeta{
		ID:     id,
		Offset: offset,
		Length: length,
		Status: STATUS_QUEUED,
	}
}

func (meta *MessageMeta) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, ORDER, meta)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeMessageMeta(data []byte) (*MessageMeta, error) {
	buf := bytes.NewBuffer(data)
	var meta MessageMeta
	if err := binary.Read(buf, ORDER, &meta); err != nil {
		return &meta, err
	}
	return &meta, nil
}

// --------------------------------- message data ---------------------------------

type MessageData []byte

func (md MessageData) Size() int64 {
	return int64(len(md))
}

func MessageDataFromStr(msg string) MessageData {
	return []byte(msg)
}

// --------------------------------- message ---------------------------------

type Message struct {
	ID       ID
	Priority Priority
	Data     MessageData
}

func (m *Message) String() string {
	return fmt.Sprintf("Message{<%s> id: %d, data: %s}", m.Priority, m.ID, string(m.Data))
}

// --------------------------------- sent messages ---------------------------------

type SentMessage struct {
	msg  *Message
	time int64
}

type SentMessages struct {
	sent map[ID]*SentMessage
}

func NewSentMessages(ttl int64, timeoutChan chan *Message) *SentMessages {
	sm := &SentMessages{
		sent: make(map[ID]*SentMessage),
	}
	go func() {
		for now := range time.Tick(time.Second * 30) {
			for k, v := range sm.sent {
				if now.Unix()-v.time >= ttl {
					delete(sm.sent, k)
					timeoutChan <- v.msg
				}
			}
		}
	}()
	return sm
}

func (sm *SentMessages) Size() int {
	return len(sm.sent)
}

func (sm *SentMessages) Sent(msg *Message) {
	sm.sent[msg.ID] = &SentMessage{
		msg:  msg,
		time: time.Now().Unix(),
	}
}

func (sm *SentMessages) Ack(id ID) {
	delete(sm.sent, id)
}
