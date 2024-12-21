package queue

import (
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type Queue struct {
	Name       string
	sm         *StoreManager
	reads      map[Priority]*Store
	writes     map[Priority]*Store
	normChan   chan *Message
	highChan   chan *Message
	urgentChan chan *Message
	ackedChan  chan *Acked
	unacked    map[Priority]map[ID]*Unacked
	metrics    *Metrics
	queueCond  *sync.Cond
}

func NewQueue(name string) *Queue {
	sm := NewStoreManager()
	normRead, err := FindReadStore(sm, name, PRIORITY_NORM)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load norm read store for queue: %s", name)
	}
	normWrite, err := FindWriteStore(sm, name, PRIORITY_NORM)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load norm write store for queue: %s", name)
	}
	highRead, err := FindReadStore(sm, name, PRIORITY_HIGH)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load high read store for queue: %s", name)
	}
	highWrite, err := FindWriteStore(sm, name, PRIORITY_HIGH)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load high write store for queue: %s", name)
	}
	urgentRead, err := FindReadStore(sm, name, PRIORITY_URGENT)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load urgent read store for queue: %s", name)
	}
	urgentWrite, err := FindWriteStore(sm, name, PRIORITY_URGENT)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load urgent write store for queue: %s", name)
	}
	normChan := make(chan *Message, BUFFER_SIZE_DEFAULT)
	highChan := make(chan *Message, BUFFER_SIZE_DEFAULT)
	urgentChan := make(chan *Message, BUFFER_SIZE_DEFAULT)
	ackedChan := make(chan *Acked, 1000)
	queue := &Queue{
		Name: name,
		sm:   sm,
		reads: map[Priority]*Store{
			PRIORITY_NORM:   normRead,
			PRIORITY_HIGH:   highRead,
			PRIORITY_URGENT: urgentRead,
		},
		writes: map[Priority]*Store{
			PRIORITY_NORM:   normWrite,
			PRIORITY_HIGH:   highWrite,
			PRIORITY_URGENT: urgentWrite,
		},
		normChan:   normChan,
		highChan:   highChan,
		urgentChan: urgentChan,
		ackedChan:  ackedChan,
		unacked:    make(map[Priority]map[ID]*Unacked),
		metrics:    NewMetrics(time.Second * 5),
		queueCond:  sync.NewCond(&sync.Mutex{}),
	}
	// queue.initSizes()

	go queue.bufferLoader(PRIORITY_URGENT, urgentChan)
	go queue.bufferLoader(PRIORITY_HIGH, highChan)
	go queue.bufferLoader(PRIORITY_NORM, normChan)
	go queue.ackedTracker()
	go queue.unAckedChecker()

	return queue
}

func FindReadStore(sm *StoreManager, queue string, priority Priority) (*Store, error) {
	dir := os.DirFS(QueuePath(queue))
	files, err := fs.Glob(dir, fmt.Sprintf("%d-*.meta", priority))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return sm.GetStore(queue, priority, 0)
	}

	sort.Strings(files)
	var startID ID = 0
	for _, metaPath := range files {
		number, err := strconv.ParseUint(metaPath[2:22], 10, 64)
		if err != nil {
			return nil, err
		}
		store, err := sm.GetStore(queue, priority, ID(number))
		if err != nil {
			return nil, err
		}
		if store.IsReadEOF() {
			continue
		}
		startID = store.StartID
		break
	}
	return sm.GetStore(queue, priority, startID)
}

func FindWriteStore(sm *StoreManager, queue string, priority Priority) (*Store, error) {
	dir := os.DirFS(fmt.Sprintf("queue/%s/", queue))
	files, err := fs.Glob(dir, fmt.Sprintf("%d-*.meta", priority))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return sm.GetStore(queue, priority, 0)
	}

	sort.Strings(files)
	metaPath := files[len(files)-1]
	number, err := strconv.ParseUint(metaPath[2:22], 10, 64)
	if err != nil {
		return nil, err
	}
	return sm.GetStore(queue, priority, ID(number))
}

func (q *Queue) initSizes() {
	for priority := range []Priority{PRIORITY_URGENT, PRIORITY_HIGH, PRIORITY_NORM} {
		p := Priority(priority)
		q.metrics.SetSize(p, int64(q.writes[p].GetWriteID()-q.reads[p].GetReadID()))
	}
}

func (q *Queue) bufferLoader(priority Priority, buffer chan *Message) {
	for {
		msg, err := q.Pull(priority)
		if err != nil {
			log.Error().Err(err).Send()
			continue
		}
		log.Debug().Msgf("[buffer] <%s> id: %d", priority, msg.ID)
		buffer <- msg
		q.queueCond.L.Lock()
		q.queueCond.Signal()
		q.queueCond.L.Unlock()
	}
}

func (q *Queue) ackedTracker() {
	for {
		msg := <-q.ackedChan
		q.reads[msg.Priority].UpdateStatus(msg.ID, STATUS_ACKED)
		q.metrics.MarkOut(msg.Priority)
	}
}

func (q *Queue) unAckedChecker() {
	for now := range time.Tick(time.Second * 30) {
		for _, entries := range q.unacked {
			for id, unacked := range entries {
				if now.Unix()-unacked.time >= TTL_1_MINUTE {
					delete(entries, id)
					q.Put(unacked.msg.Data, unacked.msg.Priority)
				}
			}
		}
	}
}

func (q *Queue) String() string {
	return strings.Join([]string{
		fmt.Sprintf("Queue: %s", q.Name),
		fmt.Sprintf("  Buffer  : norm: %d, high: %d, urgent: %d, acked: %d", len(q.normChan), len(q.highChan), len(q.urgentChan), len(q.ackedChan)),
		fmt.Sprintf("  UnAcked : %d", len(q.unacked[PRIORITY_URGENT])+len(q.unacked[PRIORITY_HIGH])+len(q.unacked[PRIORITY_NORM])),
		fmt.Sprintf("  Metrics : %s", q.metrics.String()),
		"  Stores  :",
		"  ------------------------  reads  ------------------------",
		q.reads[PRIORITY_NORM].String(),
		q.reads[PRIORITY_HIGH].String(),
		q.reads[PRIORITY_URGENT].String(),
		"  ------------------------  writes ------------------------",
		q.writes[PRIORITY_NORM].String(),
		q.writes[PRIORITY_HIGH].String(),
		q.writes[PRIORITY_URGENT].String(),
	}, "\n")
}

func (q *Queue) Sizes() map[Priority]int64 {
	return q.metrics.Sizes()
}

func (q *Queue) Rates() map[Priority]*InOutRates {
	return q.metrics.Rates()
}

func (q *Queue) Put(message MessageData, priority Priority) error {
	err := q.writes[priority].Put(message)
	if err == nil {
		q.metrics.MarkIn(priority)
	}
	return err
}

func (q *Queue) Get() *Message {
	select {
	case msg := <-q.urgentChan:
		return msg
	default:
		select {
		case msg := <-q.highChan:
			return msg
		default:
			select {
			case msg := <-q.normChan:
				return msg
			default:
				return nil
			}
		}
	}
}

func (q *Queue) BGet() *Message {
	for {
		select {
		case msg := <-q.urgentChan:
			return msg
		default:
			select {
			case msg := <-q.highChan:
				return msg
			default:
				select {
				case msg := <-q.normChan:
					return msg
				default:
					q.queueCond.L.Lock()
					q.queueCond.Wait()
					q.queueCond.L.Unlock()
					continue
				}
			}
		}
	}
}

func (q *Queue) Pull(priority Priority) (*Message, error) {
	return q.writes[priority].Get()
}

func (q *Queue) Ack(priority Priority, id ID) {
	delete(q.unacked[priority], id)
	q.ackedChan <- &Acked{ID: id, Priority: priority}
}
