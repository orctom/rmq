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
	Name        string
	sm          *StoreManager
	reads       map[Priority]*Store
	writes      map[Priority]*Store
	normChan    chan *Message
	highChan    chan *Message
	urgentChan  chan *Message
	sentChan    chan *Message
	timeoutChan chan *Message
	unAcked     map[ID]*UnAcked
	queueCond   *sync.Cond
}

func NewQueue(name string) *Queue {
	sm := NewStoreManager()
	normRead, err := FindReadStore(sm, name, PRIORITY_NORMAL)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load norm read store for queue: %s", name)
	}
	normWrite, err := FindWriteStore(sm, name, PRIORITY_NORMAL)
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
	sentChan := make(chan *Message, 1000)
	queue := &Queue{
		Name: name,
		sm:   sm,
		reads: map[Priority]*Store{
			PRIORITY_NORMAL: normRead,
			PRIORITY_HIGH:   highRead,
			PRIORITY_URGENT: urgentRead,
		},
		writes: map[Priority]*Store{
			PRIORITY_NORMAL: normWrite,
			PRIORITY_HIGH:   highWrite,
			PRIORITY_URGENT: urgentWrite,
		},
		normChan:   normChan,
		highChan:   highChan,
		urgentChan: urgentChan,
		sentChan:   sentChan,
		unAcked:    make(map[ID]*UnAcked),
		queueCond:  sync.NewCond(&sync.Mutex{}),
	}

	go queue.bufferLoader(PRIORITY_URGENT, urgentChan)
	go queue.bufferLoader(PRIORITY_HIGH, highChan)
	go queue.bufferLoader(PRIORITY_NORMAL, normChan)
	go queue.sentStatusTracker()
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

func (q *Queue) sentStatusTracker() {
	for {
		msg := <-q.sentChan
		q.UpdateStatus(msg.Priority, msg.ID, STATUS_SENT)
	}
}

func (q *Queue) unAckedChecker() {
	for now := range time.Tick(time.Second * 30) {
		for k, v := range q.unAcked {
			if now.Unix()-v.time >= TTL_1_MINUTE {
				delete(q.unAcked, k)
				q.Put(v.msg.Data, v.msg.Priority)
			}
		}
	}
}

func (q *Queue) String() string {
	return strings.Join([]string{
		fmt.Sprintf("Queue: %s", q.Name),
		fmt.Sprintf("  Buffer  : n: %d, h: %d, u: %d, s: %d", len(q.normChan), len(q.highChan), len(q.urgentChan), len(q.sentChan)),
		fmt.Sprintf("  UnAcked : %d", len(q.unAcked)),
		"  Stores  :",
		"  ------------------------  reads  ------------------------",
		q.reads[PRIORITY_NORMAL].String(),
		q.reads[PRIORITY_HIGH].String(),
		q.reads[PRIORITY_URGENT].String(),
		"  ------------------------  writes ------------------------",
		q.writes[PRIORITY_NORMAL].String(),
		q.writes[PRIORITY_HIGH].String(),
		q.writes[PRIORITY_URGENT].String(),
	}, "\n")
}

func (q *Queue) Put(message MessageData, priority Priority) error {
	return q.writes[priority].Put(message)
}

func (q *Queue) Get() *Message {
	select {
	case msg := <-q.urgentChan:
		q.sentChan <- msg
		return msg
	default:
		select {
		case msg := <-q.highChan:
			q.sentChan <- msg
			return msg
		default:
			select {
			case msg := <-q.normChan:
				q.sentChan <- msg
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
			q.sentChan <- msg
			return msg
		default:
			select {
			case msg := <-q.highChan:
				q.sentChan <- msg
				return msg
			default:
				select {
				case msg := <-q.normChan:
					q.sentChan <- msg
					return msg
				default:
					log.Info().Msg("waiting")
					q.queueCond.L.Lock()
					q.queueCond.Wait()
					q.queueCond.L.Unlock()
					log.Info().Msg("got notified, checking again")
					continue
				}
			}
		}
	}
}

func (q *Queue) Pull(priority Priority) (*Message, error) {
	return q.writes[priority].Get()
}

func (q *Queue) UpdateStatus(priority Priority, id ID, status Status) error {
	return q.reads[priority].UpdateStatus(id, status)
}

func (q *Queue) Ack(id ID) {
	delete(q.unAcked, id)
}
