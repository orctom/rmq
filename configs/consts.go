package configs

import (
	"regexp"
)

const (
	DATETIME_FMT_ZONE = "2006-01-02T15:04:05.000 CST"
	DATETIME_FMT_PATH = "060102-150405"
)

var (
	P_SKIP_ACCESS_LOG = regexp.MustCompile("(?:/assets|favicon.ico)")
)
