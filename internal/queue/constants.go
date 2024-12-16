package queue

import (
	"encoding/binary"

	"orctom.com/rmq/internal/utils"
)

var (
	BASE_PATH = utils.ExpandHome("~/.rmq/")
	ORDER     = binary.BigEndian
)

const (
	ONE_MINUTE  = int64(60)
	BUFFER_SIZE = 10_000

	MESSAGE_META_SIZE = 25
)

type Priority uint8

const (
	PRIORITY_NORMAL Priority = iota
	PRIORITY_HIGH
	PRIORITY_URGENT
)

func (p Priority) String() string {
	switch p {
	case PRIORITY_NORMAL:
		return "norm"
	case PRIORITY_HIGH:
		return "high"
	case PRIORITY_URGENT:
		return "urgent"
	default:
		return "unknown"
	}
}

type Status uint8

const (
	STATUS_READY Status = iota
	STATUS_SENT
)

func (s Status) String() string {
	switch s {
	case STATUS_READY:
		return "ready"
	case STATUS_SENT:
		return "sent"
	default:
		return "unknown"
	}
}
