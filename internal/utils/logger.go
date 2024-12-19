package utils

import (
	"io"
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
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	var writer io.Writer
	if conf.Config.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		writer = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: conf.DATETIME_FMT_ZONE}
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		cwd, _ := os.Getwd()
		zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
			file = strings.TrimPrefix(file, cwd)
			return file + ":" + strconv.Itoa(line)
		}
		writer = os.Stdout
	}
	log.Logger = zerolog.New(writer).With().Timestamp().Caller().Logger()
}
