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

func NewMessageDataFromStr(msg string) MessageData {
	return []byte(msg)
}

// --------------------------------- message ---------------------------------

type Message struct {
	ID       ID
	Priority Priority
	Data     MessageData
}

// --------------------------------- message on flight ---------------------------------

type SentMessage struct {
	mid    *MessageMeta
	offset int64
	time   int64
}

type sentMessages struct {
	sent map[ID]*SentMessage
}

type MessageTimeoutHandler func(msg *MessageMeta)

func NewSentMessages(ttl int64, handler MessageTimeoutHandler) *sentMessages {
	sm := &sentMessages{
		sent: make(map[ID]*SentMessage),
	}
	sm.ttlChecker(ttl, handler)
	return sm
}

func (sm *sentMessages) ttlChecker(ttl int64, handler MessageTimeoutHandler) {
	go func() {
		for now := range time.Tick(time.Second * 30) {
			for k, v := range sm.sent {
				if now.Unix()-v.time >= ttl {
					delete(sm.sent, k)
					handler(v.mid)
				}
			}
		}
	}()
}

func (sm *sentMessages) Sent(mid *MessageMeta, offset int64) {
	sm.sent[mid.ID] = &SentMessage{
		mid:    mid,
		offset: offset,
		time:   time.Now().Unix(),
	}
}

func (sm *sentMessages) Ack(id ID) {
	delete(sm.sent, id)
}
