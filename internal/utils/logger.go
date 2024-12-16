package utils

import (
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog/pkgerrors"
	"orctom.com/rmq/conf"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	zerolog.TimeFieldFormat = conf.DATETIME_FMT_ZONE
	if conf.Config.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	cwd, _ := os.Getwd()
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		file = strings.TrimPrefix(file, cwd)
		return file + ":" + strconv.Itoa(line)
	}
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
}
