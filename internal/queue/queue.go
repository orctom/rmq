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
	writeLocks map[Priority]*sync.Mutex
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
	normChan := make(chan *Message, BUFFER_SIZE_DEFAULT)
	highChan := make(chan *Message, BUFFER_SIZE_DEFAULT)
	urgentChan := make(chan *Message, BUFFER_SIZE_DEFAULT)
	ackedChan := make(chan *Acked, 1000)
	queue := &Queue{
		Name: name,
		sm:   sm,
		reads: map[Priority]*Store{
			PRIORITY_NORM:   FindReadStore(sm, name, PRIORITY_NORM),
			PRIORITY_HIGH:   FindReadStore(sm, name, PRIORITY_HIGH),
			PRIORITY_URGENT: FindReadStore(sm, name, PRIORITY_URGENT),
		},
		writes: map[Priority]*Store{
			PRIORITY_NORM:   FindWriteStore(sm, name, PRIORITY_NORM),
			PRIORITY_HIGH:   FindWriteStore(sm, name, PRIORITY_HIGH),
			PRIORITY_URGENT: FindWriteStore(sm, name, PRIORITY_URGENT),
		},
		writeLocks: map[Priority]*sync.Mutex{
			PRIORITY_NORM:   new(sync.Mutex),
			PRIORITY_HIGH:   new(sync.Mutex),
			PRIORITY_URGENT: new(sync.Mutex),
		},
		normChan:   normChan,
		highChan:   highChan,
		urgentChan: urgentChan,
		ackedChan:  ackedChan,
		unacked:    make(map[Priority]map[ID]*Unacked),
		metrics:    NewMetrics(name, time.Second*5),
		queueCond:  sync.NewCond(new(sync.Mutex)),
	}
	queue.initSizes()

	go queue.bufferLoader(PRIORITY_URGENT, urgentChan)
	go queue.bufferLoader(PRIORITY_HIGH, highChan)
	go queue.bufferLoader(PRIORITY_NORM, normChan)
	go queue.ackedTracker()
	go queue.unAckedChecker()

	return queue
}

func FindReadStore(sm *StoreManager, queue string, priority Priority) *Store {
	dir := os.DirFS(QueuePath(queue))
	files, err := fs.Glob(dir, fmt.Sprintf("%d-*.meta", priority))
	if err != nil {
		log.Panic().Err(err).Msgf("[find-read-store] wrong glob pattern")
	}
	if len(files) == 0 {
		log.Debug().Msg("no files")
		return sm.GetStore(queue, priority, 0)
	}

	sort.Strings(files)
	var startID ID = 0
	for _, metaPath := range files {
		number, err := strconv.ParseUint(metaPath[2:22], 10, 64)
		if err != nil {
			log.Panic().Err(err).Msgf("[find-read-store] failed to parse id from meta file: %s", metaPath)
		}
		store := sm.GetStore(queue, priority, ID(number))
		if store.IsWriteEOF() && store.IsReadEOF() {
			continue
		}
		log.Debug().Msg(metaPath)
		log.Debug().Msgf(" is write eof: %v", store.IsWriteEOF())
		log.Debug().Msgf(" is read  eof: %v", store.IsReadEOF())
		startID = store.StartID
		break
	}
	return sm.GetStore(queue, priority, startID)
}

func FindWriteStore(sm *StoreManager, queue string, priority Priority) *Store {
	dir := os.DirFS(QueuePath(queue))
	files, err := fs.Glob(dir, fmt.Sprintf("%d-*.meta", priority))
	if err != nil {
		log.Panic().Err(err).Msgf("[find-write-store] wrong glob pattern")
	}
	if len(files) == 0 {
		return sm.GetStore(queue, priority, 0)
	}

	sort.Strings(files)
	metaPath := files[len(files)-1]
	number, err := strconv.ParseUint(metaPath[2:22], 10, 64)
	if err != nil {
		log.Panic().Err(err).Msgf("[find-write-store] failed to parse id from meta file: %s", metaPath)
	}
	return sm.GetStore(queue, priority, ID(number))
}

func (q *Queue) initSizes() {
	for priority := range []Priority{PRIORITY_URGENT, PRIORITY_HIGH, PRIORITY_NORM} {
		p := Priority(priority)
		size := int64(q.writes[p].GetWriteID() - q.reads[p].GetReadID())
		q.metrics.SetSize(p, size)
		log.Info().Msgf("[init-size] [%s] <%s> size: %d", q.Name, p, size)
		log.Info().Msgf(" read : %s, read  id: %d (write id: %d)", q.reads[p].Key(), q.reads[p].GetReadID(), q.reads[p].GetWriteID())
		log.Info().Msgf(" write: %s, write id: %d (read  id: %d)", q.writes[p].Key(), q.writes[p].GetWriteID(), q.writes[p].GetReadID())
	}
}

func (q *Queue) bufferLoader(priority Priority, buffer chan *Message) {
	for {
		msg, err := q.reads[priority].Get()
		if err != nil {
			if IsEOFError(err) {
				if q.sm.IsStoreExists(q.Name, priority, q.reads[priority].GetReadID()) {
					nextStore := q.sm.GetStore(q.Name, priority, q.writes[priority].GetReadID())
					oldId := q.reads[priority].GetReadID()
					newId := nextStore.GetReadID()
					q.sm.UnrefStore(q.reads[priority].Key())
					q.reads[priority] = nextStore
					log.Info().Msgf("[%s] <%s> read shift %d -> %d", q.Name, priority, oldId, newId)
					continue
				}
			}
			log.Error().Err(err).Send()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		log.Trace().Msgf("[buffer] <%s> id: %d", priority, msg.ID)
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
		delete(q.unacked[msg.Priority], msg.ID)
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
		fmt.Sprintf("Buffer  : norm: %d, high: %d, urgent: %d, acked: %d", len(q.normChan), len(q.highChan), len(q.urgentChan), len(q.ackedChan)),
		fmt.Sprintf("UnAcked : %d", len(q.unacked[PRIORITY_URGENT])+len(q.unacked[PRIORITY_HIGH])+len(q.unacked[PRIORITY_NORM])),
		fmt.Sprintf("Metrics : %s", q.metrics.String()),
		"Stores  :",
		"------------------------  reads  ------------------------",
		q.reads[PRIORITY_NORM].String(),
		q.reads[PRIORITY_HIGH].String(),
		q.reads[PRIORITY_URGENT].String(),
		"------------------------  writes ------------------------",
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
	if q.writes[priority].IsWriteEOF() {
		q.writeLocks[priority].Lock()
		if q.writes[priority].IsWriteEOF() {
			nextStore := q.sm.GetStore(q.Name, priority, q.writes[priority].GetWriteID())
			oldId := q.writes[priority].GetWriteID()
			newId := nextStore.GetWriteID()
			q.sm.UnrefStore(q.writes[priority].Key())
			q.writes[priority] = nextStore
			log.Info().Msgf("[%s] <%s> write shift %d -> %d", q.Name, priority, oldId, newId)
		}
		q.writeLocks[priority].Unlock()
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

func (q *Queue) Ack(priority Priority, id ID) {
	q.ackedChan <- &Acked{ID: id, Priority: priority}
}
