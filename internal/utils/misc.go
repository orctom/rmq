package utils

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type Counter struct {
	counter map[interface{}]int
	sync.Mutex
}

func NewCounter() *Counter {
	return &Counter{
		counter: make(map[interface{}]int),
	}
}

func (c *Counter) Count(key interface{}) {
	c.Lock()
	defer c.Unlock()
	c.counter[key]++
}

func (c *Counter) GetCount(key interface{}) int {
	c.Lock()
	defer c.Unlock()
	return c.counter[key]
}

func (c *Counter) Reset() {
	c.Lock()
	defer c.Unlock()
	c.counter = make(map[interface{}]int)
}

func (c *Counter) String() string {
	pairs := []string{}
	for key, value := range c.counter {
		pairs = append(pairs, fmt.Sprintf("%s: %v", key, value))
	}
	return strings.Join(pairs, ", ")
}

func JoinKV(dict map[interface{}]interface{}, sep string) string {
	pairs := []string{}
	for key, value := range dict {
		pairs = append(pairs, fmt.Sprintf("%s: %s", key, value))
	}
	return strings.Join(pairs, sep)
}

func EncodeTime(t time.Time) []byte {
	b, err := t.MarshalBinary()
	if err != nil {
		log.Error().Err(err).Send()
		return nil
	}
	return b
}

func DecodeTime(b []byte) time.Time {
	var t time.Time
	err := t.UnmarshalBinary(b)
	if err != nil {
		log.Error().Err(err).Send()
	}
	return t
}
