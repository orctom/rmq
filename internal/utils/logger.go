package utils

import (
	"io"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/rs/zerolog/pkgerrors"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func IsDebug() bool {
	val := os.Getenv("DEBUG")
	if val == "true" || val == "1" {
		return true
	}
	vcsTime := GetVcsTime()
	if vcsTime == "" {
		return true
	}
	log.Info().Msgf("vcs time: %s", vcsTime)
	return false
}

func GetVcsTime() string {
	info, _ := debug.ReadBuildInfo()
	for _, setting := range info.Settings {
		if setting.Key == "vcs.time" {
			return setting.Value
		}
	}
	return ""
}

func init() {
	zerolog.TimeFieldFormat = DATETIME_FMT_ZONE
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	var writer io.Writer
	if IsDebug() {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		writer = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: DATETIME_FMT_ZONE}
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
