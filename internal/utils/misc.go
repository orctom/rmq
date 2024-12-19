package utils

import (
	"fmt"
	"strings"
	"sync"
	"unsafe"
)

func IntToByteArray(num uint64) []byte {
	size := int(unsafe.Sizeof(num))
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		byt := *(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&num)) + uintptr(i)))
		arr[i] = byt
	}
	return arr
}

func ByteArrayToInt(arr []byte) uint64 {
	val := uint64(0)
	size := len(arr)
	for i := 0; i < size; i++ {
		*(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&val)) + uintptr(i))) = arr[i]
	}
	return val
}

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
