package utils

import (
	"encoding/json"
	"fmt"
)

type Size struct {
	Bytes uint64 `json:"bytes"`
	Human string `json:"human"`
}

func BytesToHuman(bytes uint64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%dB", bytes)
	} else if bytes < 1048576 {
		return fmt.Sprintf("%.2fk", float64(bytes)/1024)
	} else if bytes < 1073741824 {
		return fmt.Sprintf("%.2fm", float64(bytes)/1048576)
	} else if bytes < 1099511627776 {
		return fmt.Sprintf("%.2fG", float64(bytes)/1073741824)
	} else {
		return fmt.Sprintf("%.2fT", float64(bytes)/1099511627776)
	}
}

func Bits(b uint64) *Size {
	return Bytes(b / 8)
}

func Bytes(b uint64) *Size {
	return &Size{
		Bytes: b,
		Human: BytesToHuman(b),
	}
}

func (bytes *Size) Incr(b uint64) *Size {
	bytes.Bytes += b
	bytes.Human = BytesToHuman(bytes.Bytes)
	return bytes
}

func (bytes *Size) String() string {
	s, _ := json.Marshal(bytes)
	return string(s)
}
