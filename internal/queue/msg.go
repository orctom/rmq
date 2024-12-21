package queue

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type ID int64

func (id ID) String() string {
	return fmt.Sprintf("%0*d", 20, id)
}

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

type MessageData []byte

func (md MessageData) Size() int64 {
	return int64(len(md))
}

func MessageDataFromStr(msg string) MessageData {
	return []byte(msg)
}

type Message struct {
	ID       ID
	Priority Priority
	Data     MessageData
}

func (m *Message) String() string {
	return fmt.Sprintf("Message{<%s> id: %d, data: %s}", m.Priority, m.ID, string(m.Data))
}

type Unacked struct {
	msg  *Message
	time int64
}

type Acked struct {
	ID       ID
	Priority Priority
}
