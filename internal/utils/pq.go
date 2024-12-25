package utils

import (
	"container/heap"
	"sync"
	"time"
)

const ERROR_PRIORITY = 255

type PQEntry struct {
	Val      []byte
	Priority uint8
}

type priorityQueue struct {
	sync.Mutex
	entries  []*PQEntry
	notEmpty chan struct{}
}

func NewPriorityQueue() *priorityQueue {
	pq := &priorityQueue{
		entries:  make([]*PQEntry, 0),
		notEmpty: make(chan struct{}, 1),
	}
	heap.Init(pq)
	return pq
}

// Len implements sort.Interface
func (pq *priorityQueue) Len() int {
	return len(pq.entries)
}

// Less implements sort.Interface
func (pq *priorityQueue) Less(i, j int) bool {
	return pq.entries[i].Priority > pq.entries[j].Priority
}

// Swap implements sort.Interface
func (pq *priorityQueue) Swap(i, j int) {
	pq.entries[i], pq.entries[j] = pq.entries[j], pq.entries[i]
}

// Push implements heap.Interface
func (pq *priorityQueue) Push(x any) {
	item := x.(*PQEntry)
	pq.entries = append(pq.entries, item)
}

// Pop implements heap.Interface
func (pq *priorityQueue) Pop() any {
	old := pq.entries
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	pq.entries = old[0 : n-1]
	return item
}

func (pq *priorityQueue) Put(val []byte, priority uint8) {
	entry := &PQEntry{
		Val:      val,
		Priority: priority,
	}
	pq.Lock()
	defer pq.Unlock()

	heap.Push(pq, entry)
	if len(pq.entries) == 1 {
		select {
		case pq.notEmpty <- struct{}{}:
		default:
		}
	}
}

func (pq *priorityQueue) Get() (val []byte, priority uint8) {
	pq.Lock()
	defer pq.Unlock()
	if pq.Len() == 0 {
		return nil, ERROR_PRIORITY
	}
	entry := heap.Pop(pq).(*PQEntry)
	return entry.Val, entry.Priority
}

func (pq *priorityQueue) GetWithTTL(ttl time.Duration) (val []byte, priority uint8) {
	for {
		pq.Lock()
		defer pq.Unlock()
		if len(pq.entries) > 0 {
			entry := heap.Pop(pq).(*PQEntry)
			return entry.Val, entry.Priority
		}

		select {
		case <-pq.notEmpty:
			continue
		case <-time.After(ttl):
			return nil, ERROR_PRIORITY
		}
	}
}

func (pq *priorityQueue) IsEmpty() bool {
	pq.Lock()
	defer pq.Unlock()
	return pq.Len() == 0
}
