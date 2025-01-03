package utils

import (
	"encoding/binary"
	"fmt"
)

var (
	BASE_PATH   = ExpandHome("~/.rmq/")
	QUEUES_PATH = fmt.Sprintf("%s/queue/", BASE_PATH)
	ORDER       = binary.BigEndian
)

const (
	DATETIME_FMT_ZONE = "2006-01-02T15:04:05.000 CST"
	DATETIME_FMT_PATH = "060102-150405"

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
