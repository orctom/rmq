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
	ONE_MINUTE          = int64(60)
	BUFFER_SIZE_10K     = 10_000
	BUFFER_SIZE_1M      = 1_000_000
	BUFFER_SIZE_DEFAULT = BUFFER_SIZE_10K

	TTL_1_MINUTE   = int64(60 * 60)
	TTL_2_MINUTES  = int64(60 * 60 * 2)

	MESSAGE_META_SIZE = 25

	SIZE_500M = 1 << 29
	SIZE_1G   = 1 << 30
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
	STATUS_QUEUED Status = iota
	STATUS_PULLED
	STATUS_SENT
	STATUS_ACKED
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
	default:
		return "unknown"
	}
}
