package queue

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/utils"
)

func Key(queue string, priority Priority, id ID) string {
	return fmt.Sprintf("%s/%d-%s", queue, priority, id)
}

func QueuePath(queue string) string {
	return fmt.Sprintf("%s/queue/%s/", BASE_PATH, queue)
}

func StorePath(queue string, priority Priority, id ID, ext string) string {
	return fmt.Sprintf("%s/queue/%s/%d-%s%s", BASE_PATH, queue, priority, id, ext)
}

// =========================== store ===========================

type Store struct {
	Queue       string
	Priority    Priority
	StartID     ID
	meta        *utils.Mmap
	data        *utils.Mmap
	readOffset  int64
	writeOffset int64
	id          ID
	lastTick    time.Time
	references  int
	sync.Mutex
}

type StoreManager struct {
	stores map[string]*Store
}

func NewStoreManager() *StoreManager {
	return &StoreManager{
		stores: make(map[string]*Store),
	}
}

func (sm *StoreManager) GetStore(queue string, priority Priority, startID ID) (*Store, error) {
	key := Key(queue, priority, startID)
	if store, exists := sm.stores[key]; exists {
		store.Ref()
		return store, nil
	}
	store, err := NewStore(queue, priority, startID)
	if err != nil {
		return nil, err
	}
	sm.stores[key] = store
	store.Ref()
	return store, nil
}

func (sm *StoreManager) UnrefStore(queue string, priority Priority, startID ID) {
	key := Key(queue, priority, startID)
	if store, exists := sm.stores[key]; exists {
		if store.Unref() {
			delete(sm.stores, key)
		}
	}
}

func NewStore(queue string, priority Priority, startID ID) (*Store, error) {
	metaPath := StorePath(queue, priority, startID, ".meta")
	dataPath := StorePath(queue, priority, startID, ".data")
	metaPath = utils.ExpandHome(metaPath)
	dataPath = utils.ExpandHome(dataPath)
	if utils.IsNotExists(metaPath) {
		utils.TouchFile(metaPath)
	}
	if utils.IsNotExists(dataPath) {
		utils.TouchFile(dataPath)
	}

	var id ID = startID
	var readOffset int64 = 0
	meta, err := utils.NewMmap(metaPath)
	if err != nil {
		return nil, err
	} else {
		id = findCurrentID(meta)
		readOffset = findReadOffset(meta)
	}

	var writeOffset int64 = 0
	data, err := utils.NewMmap(dataPath)
	if err != nil {
		return nil, err
	} else {
		writeOffset = data.Size()
	}

	key := Key(queue, priority, startID)
	log.Debug().Str("key", key).Int64("read", readOffset).Int64("write", writeOffset).Msg("new store")
	return &Store{
		Queue:       queue,
		Priority:    priority,
		StartID:     startID,
		meta:        meta,
		data:        data,
		readOffset:  readOffset,
		writeOffset: writeOffset,
		id:          id,
		lastTick:    time.Now(),
	}, nil
}

func findCurrentID(mmap *utils.Mmap) ID {
	if mmap.Size() <= 0 {
		return ID(0)
	}
	offset := mmap.Size() - MESSAGE_META_SIZE
	idBytes := make([]byte, 8)
	mmap.ReadAt(idBytes, offset)
	return ID(ORDER.Uint64(idBytes) + 1)
}

func (s *Store) Key() string {
	return Key(s.Queue, s.Priority, s.StartID)
}

func (s *Store) Debug() {
	fmt.Printf("--- [%s] ---\n", s.Key())
	var offset int64 = 0
	for offset = 0; offset < s.meta.Size(); offset += MESSAGE_META_SIZE {
		buffer := make([]byte, MESSAGE_META_SIZE)
		s.meta.ReadAt(buffer, offset)
		meta, _ := DecodeMessageMeta(buffer)
		fmt.Printf("  [%d] offset: %d, len: %d, status: %s\n", meta.ID, meta.Offset, meta.Length, meta.Status)
	}
}

func (s *Store) IsEmpty() bool {
	return s.meta.Size() == 0
}

func (s *Store) IsReadEOF() bool {
	return s.readOffset+MESSAGE_META_SIZE >= s.meta.Size()
}

func (s *Store) IsWriteEOF() bool {
	return s.data.Size() >= SIZE_500M
}

func findReadOffset(mmap *utils.Mmap) int64 {
	var offset int64 = 0
	for offset = 0; offset < mmap.Size(); offset += MESSAGE_META_SIZE {
		buffer := make([]byte, 1)
		mmap.ReadAt(buffer, offset+MESSAGE_META_SIZE-1)
		status := Status(buffer[0])
		if status == STATUS_QUEUED {
			break
		}
	}
	return offset
}

func (s *Store) Close() {
	s.meta.Close()
	s.data.Close()
}

func (s *Store) Ref() {
	s.references++
}

func (s *Store) Unref() bool {
	s.references--
	if s.references <= 0 {
		s.Close()
		return true
	}
	return false
}

func (s *Store) GetAndIncrease() ID {
	s.Lock()
	defer s.Unlock()
	id := s.id
	s.id++
	return id
}

func (s *Store) Put(message MessageData) error {
	err := s.data.Append(message)
	if err == nil {
		length := message.Size()
		meta := NewMessageMeta(s.GetAndIncrease(), s.writeOffset, length)
		fmt.Printf("[put msg] id: %d, offset: %d, len: %d, val: %s\n", meta.ID, meta.Offset, meta.Length, string(message))
		metaEncoded, err := meta.Encode()
		if err == nil {
			s.meta.Append(metaEncoded)
		}
		s.writeOffset += length
		s.lastTick = time.Now()
	} else {
		log.Err(err).Send()
	}
	return err
}

func (s *Store) Get() (*Message, error) {
	metaBuffer := make([]byte, MESSAGE_META_SIZE)
	if s.readOffset >= s.meta.Size() {
		return nil, errors.New("no more messages")
	}
	s.meta.ReadAt(metaBuffer, s.readOffset)
	meta, err := DecodeMessageMeta(metaBuffer)
	if err != nil {
		return nil, err
	}

	dataBuffer := make([]byte, meta.Length)
	s.data.ReadAt(dataBuffer, meta.Offset)
	msg := &Message{
		ID:       meta.ID,
		Priority: s.Priority,
		Data:     dataBuffer,
	}
	err = s.UpdateStatus(meta.ID, STATUS_PULLED)
	if err != nil {
		return nil, err
	}
	s.readOffset += MESSAGE_META_SIZE
	return msg, nil
}

func (s *Store) UpdateStatus(id ID, status Status) error {
	data := []byte{uint8(status)}
	offset := int64(id)*MESSAGE_META_SIZE + MESSAGE_META_SIZE - 1
	return s.meta.WriteAt(data, offset, false)
}

// =========================== stores ===========================

type Stores struct {
	sm     *StoreManager
	reads  map[Priority]*Store
	writes map[Priority]*Store
}

func NewStores(queue string) *Stores {
	sm := NewStoreManager()
	normRead, err := FindReadStore(sm, queue, PRIORITY_NORMAL)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load norm read store for queue: %s", queue)
	}
	normWrite, err := FindWriteStore(sm, queue, PRIORITY_NORMAL)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load norm write store for queue: %s", queue)
	}
	highRead, err := FindReadStore(sm, queue, PRIORITY_HIGH)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load high read store for queue: %s", queue)
	}
	highWrite, err := FindWriteStore(sm, queue, PRIORITY_HIGH)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load high write store for queue: %s", queue)
	}
	urgentRead, err := FindReadStore(sm, queue, PRIORITY_URGENT)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load urgent read store for queue: %s", queue)
	}
	urgentWrite, err := FindWriteStore(sm, queue, PRIORITY_URGENT)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load urgent write store for queue: %s", queue)
	}
	return &Stores{
		sm: sm,
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
	}
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

func (s *Stores) Debug() {
	fmt.Println("=================== read ==================")
	for _, store := range s.reads {
		store.Debug()
	}
	fmt.Println("=================== write ==================")
	for _, store := range s.writes {
		store.Debug()
	}
}

func (s *Stores) Put(message MessageData, priority Priority) error {
	return s.writes[priority].Put(message)
}

func (s *Stores) Get() (*Message, error) {
	if msg, err := s.reads[PRIORITY_URGENT].Get(); err == nil {
		return msg, nil
	}
	if msg, err := s.reads[PRIORITY_HIGH].Get(); err == nil {
		return msg, nil
	}
	if msg, err := s.reads[PRIORITY_NORMAL].Get(); err == nil {
		return msg, nil
	}

	return nil, errors.New("no messages available")
}

func (s *Stores) Pull(priority Priority) (*Message, error) {
	return s.writes[priority].Get()
}

func (s *Stores) UpdateStatus(priority Priority, id ID, status Status) error {
	return s.reads[priority].UpdateStatus(id, status)
}
