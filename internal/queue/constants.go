package queue

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"orctom.com/rmq/internal/utils"
)

var (
	BASE_PATH   = utils.ExpandHome("~/.rmq/")
	QUEUES_PATH = fmt.Sprintf("%s/queue/", BASE_PATH)
	ORDER       = binary.BigEndian
)

const (
	ONE_MINUTE          = int64(60)
	BUFFER_SIZE_10K     = 10_000
	BUFFER_SIZE_1M      = 1_000_000
	BUFFER_SIZE_DEFAULT = BUFFER_SIZE_10K

	TTL_1_MINUTE  = int64(60 * 60)
	TTL_2_MINUTES = int64(60 * 60 * 2)

	MESSAGE_META_SIZE = 25

	SIZE_500M      = 1 << 29
	SIZE_1G        = 1 << 30
	STORE_MAX_SIZE = 1024 * 1024 * 50
)

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
