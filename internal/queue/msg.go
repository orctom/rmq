package queue

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"orctom.com/rmq/internal/utils"
)

// ============== id ==============

type ID int64

func (id ID) String() string {
	return fmt.Sprintf("%0*d", 20, id)
}

func (id ID) S() string {
	return strconv.Itoa(int(id))
}

// ============== priority ==============

type Priority uint8

const (
	PRIORITY_NORM Priority = iota
	PRIORITY_HIGH
	PRIORITY_URGENT
)

func (p Priority) String() string {
	switch p {
	case PRIORITY_NORM:
		return "norm"
	case PRIORITY_HIGH:
		return "high"
	case PRIORITY_URGENT:
		return "urgent"
	default:
		return "unknown"
	}
}

func (p Priority) S() string {
	return strconv.Itoa(int(p))
}

func ParsePriority(p string) Priority {
	switch p {
	case "norm":
		return PRIORITY_NORM
	case "high":
		return PRIORITY_HIGH
	case "urgent":
		return PRIORITY_URGENT
	default:
		return PRIORITY_NORM
	}
}

// ============== status ==============

type Status uint8

const (
	STATUS_QUEUED Status = iota
	STATUS_PULLED
	STATUS_SENT
	STATUS_ACKED
	STATUS_UNKONWN = 255
)

func (s Status) String() string {
	switch s {
	case STATUS_QUEUED:
		return "queued"
	case STATUS_PULLED:
		return "pulled"
	case STATUS_SENT:
		return "sent"
	case STATUS_ACKED:
		return "acked"
	case STATUS_UNKONWN:
		return "unknown"
	default:
		return "unknown"
	}
}

// ============== message meta ==============

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
	err := binary.Write(buf, utils.ORDER, meta)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeMessageMeta(data []byte) (*MessageMeta, error) {
	buf := bytes.NewBuffer(data)
	var meta MessageMeta
	if err := binary.Read(buf, utils.ORDER, &meta); err != nil {
		return &meta, err
	}
	return &meta, nil
}

// ============== message data ==============

type MessageData []byte

func (md MessageData) Size() int64 {
	return int64(len(md))
}

func MessageDataFromStr(msg string) MessageData {
	return []byte(msg)
}

// ============== message ==============

type Message struct {
	ID       ID
	Priority Priority
	Data     MessageData
}

func (m *Message) String() string {
	return fmt.Sprintf("Message{<%s> id: %d, data: %s}", m.Priority, m.ID, string(m.Data))
}

// ============== ack ==============

type Unacked struct {
	msg  *Message
	time int64
}

type Acked struct {
	ID       ID
	Priority Priority
}
