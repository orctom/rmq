package queue

import (
	"fmt"
	"sync"
	"time"
)

type item struct {
	value      string
	lastAccess int64
}

type Flight struct {
	m map[string]*item
	l sync.Mutex
}

func New(ln int, maxTTL int) (m *Flight) {
	m = &Flight{m: make(map[string]*item, ln)}
	go func() {
		for now := range time.Tick(time.Second) {
			m.l.Lock()
			for k, v := range m.m {
				if now.Unix()-v.lastAccess > int64(maxTTL) {
					delete(m.m, k)
				}
			}
			m.l.Unlock()
		}
	}()
	return
}

func (m *Flight) Len() int {
	return len(m.m)
}

func (m *Flight) Put(k, v string) {
	m.l.Lock()
	it, ok := m.m[k]
	if !ok {
		it = &item{value: v}
		m.m[k] = it
	}
	it.lastAccess = time.Now().Unix()
	m.l.Unlock()
}

func (m *Flight) Get(k string) (v string) {
	m.l.Lock()
	if it, ok := m.m[k]; ok {
		v = it.value
		it.lastAccess = time.Now().Unix()
	}
	m.l.Unlock()
	return

}

func main() {
	m := New(10000, 1)
	for i := 0; i < 10000; i++ {
		k, v := fmt.Sprint("key", i), fmt.Sprint("value", i)
		m.Put(k, v)
	}
	fmt.Println("len(m):", m.Len())
	time.Sleep(2 * time.Second)
	fmt.Println("len(m) (this will be empty):", m.Len())

}
