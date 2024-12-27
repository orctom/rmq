package queue

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/utils"
)

type Key struct {
	Queue    string
	Priority Priority
	ID       ID
}

func NewKey(queue string, priority Priority, id ID) *Key {
	return &Key{
		Queue:    queue,
		Priority: priority,
		ID:       id,
	}
}

func (k *Key) Equals(other *Key) bool {
	return k.Queue == other.Queue && k.Priority == other.Priority && k.ID == other.ID
}

func (k *Key) String() string {
	return fmt.Sprintf("%s/%d-%s", k.Queue, k.Priority, k.ID)
}

func QueuePath(queue string) string {
	return fmt.Sprintf("%s/queue/%s/", BASE_PATH, queue)
}

func StorePath(key *Key, ext string) string {
	return fmt.Sprintf("%s/queue/%s/%d-%s%s", BASE_PATH, key.Queue, key.Priority, key.ID, ext)
}

func StorePathFromKey(key string, ext string) string {
	return fmt.Sprintf("%s/queue/%s%s", BASE_PATH, key, ext)
}

type Store struct {
	Key         *Key
	metaPath    string
	meta        *utils.Mmap
	dataPath    string
	data        *utils.Mmap
	readOffset  int64
	writeOffset int64
	id          ID
	references  int
	sync.Mutex
	cond *sync.Cond
}

type StoreManager struct {
	stores map[Key]*Store
	norefs map[Key]interface{}
	sync.Mutex
}

func NewStoreManager() *StoreManager {
	sm := &StoreManager{
		stores: make(map[Key]*Store),
		norefs: make(map[Key]interface{}),
	}
	go sm.norefsCleaner()
	return sm
}

func (sm *StoreManager) norefsCleaner() {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if len(sm.norefs) == 0 {
				continue
			}
			var deleting = make([]Key, len(sm.norefs))
			for key := range sm.norefs {
				if _, exists := sm.stores[key]; exists {
					continue
				}

				if !sm.IsStoreExists(&key) {
					deleting = append(deleting, key)
					continue
				}

				store := NewStore(&key)
				if store.IsWriteEOF() && store.IsReadEOF() && store.IsAllAcked() {
					deleting = append(deleting, key)
					store.CloseAndDelete()
					continue
				}
			}
			if len(deleting) < 0 {
				continue
			}
			for _, key := range deleting {
				delete(sm.norefs, key)
			}
		}
	}
}

func (sm *StoreManager) GetStore(key *Key) *Store {
	if store, exists := sm.stores[*key]; exists {
		store.Ref()
		return store
	}
	sm.Lock()
	if store, exists := sm.stores[*key]; exists {
		store.Ref()
		sm.Unlock()
		return store
	}
	store := NewStore(key)
	sm.stores[*key] = store
	store.Ref()
	sm.Unlock()
	return store
}

func (sm *StoreManager) IsStoreExists(key *Key) bool {
	metaPath := StorePath(key, ".meta")
	dataPath := StorePath(key, ".data")
	return !utils.IsNotExists(metaPath) && !utils.IsNotExists(dataPath)
}

func (sm *StoreManager) UnrefStore(key *Key, deleteOnNoRef bool) {
	if store, exists := sm.stores[*key]; exists {
		if store.Unref() {
			sm.norefs[*key] = nil
		}
	}
}

func NewStore(key *Key) *Store {
	metaPath := StorePath(key, ".meta")
	dataPath := StorePath(key, ".data")
	if utils.IsNotExists(metaPath) {
		utils.TouchFile(metaPath)
	}
	if utils.IsNotExists(dataPath) {
		utils.TouchFile(dataPath)
	}

	var readOffset int64 = 0
	var id ID = key.ID
	meta := utils.NewMmap(metaPath)
	readOffset = findReadOffset(meta)
	if meta.Size() > 0 {
		id = findCurrentID(meta)
	}

	var writeOffset int64 = 0
	data := utils.NewMmap(dataPath)
	writeOffset = data.Size()

	store := &Store{
		Key:         key,
		metaPath:    metaPath,
		meta:        meta,
		dataPath:    dataPath,
		data:        data,
		readOffset:  readOffset,
		writeOffset: writeOffset,
		id:          id,
		cond:        sync.NewCond(&sync.Mutex{}),
	}
	readId := int64(store.GetReadID())
	writeId := int64(store.GetWriteID())
	size := writeId - readId
	log.Info().Str("key", key.String()).Int64("r", readId).Int64("w", writeId).Int64("z", size).Msg("[store]")
	return store
}

func findCurrentID(mmap *utils.Mmap) ID {
	offset := mmap.Size() - MESSAGE_META_SIZE
	idBytes := make([]byte, 8)
	mmap.ReadAt(idBytes, offset)
	return ID(ORDER.Uint64(idBytes) + 1)
}

func (s *Store) String() string {
	var items = make([]string, 0)
	metaSize := utils.BytesToHuman(uint64(s.meta.Size()))
	dataSize := utils.BytesToHuman(uint64(s.data.Size()))
	readID := s.GetReadID()
	writeID := s.GetWriteID()
	items = append(items, fmt.Sprintf("[%s] meta: %s, data: %s, read: %d, write: %d", s.Key, metaSize, dataSize, readID, writeID))

	var status Status = STATUS_UNKONWN
	var last, lastAdded *MessageMeta = nil, nil
	var counter = utils.NewCounter()

	var offset int64
	for offset = 0; offset < s.meta.Size(); offset += MESSAGE_META_SIZE {
		buffer := make([]byte, MESSAGE_META_SIZE)
		s.meta.ReadAt(buffer, offset)
		meta, _ := DecodeMessageMeta(buffer)
		if meta.Status != status {
			items = append(items, fmt.Sprintf("  [%d] offset: %d, len: %d, status: %s", meta.ID, meta.Offset, meta.Length, meta.Status))
			lastAdded = meta
		}
		status = meta.Status
		last = meta
		counter.Count(meta.Status)
	}
	if last != lastAdded {
		items = append(items, fmt.Sprintf("  [%d] offset: %d, len: %d, status: %s", last.ID, last.Offset, last.Length, last.Status))
	}
	items = append(items, fmt.Sprintf("  [counts] %s", counter.String()))
	return strings.Join(items, "\n")
}

func (s *Store) IsEmpty() bool {
	s.Lock()
	defer s.Unlock()
	return s.meta.Size() == 0
}

func (s *Store) IsReadEOF() bool {
	return s.readOffset >= s.meta.Size()
}

func (s *Store) IsWriteEOF() bool {
	return s.data.Size() >= STORE_MAX_SIZE
}

func findReadOffset(mmap *utils.Mmap) int64 {
	var offset int64 = 0
	for offset = 0; offset < mmap.Size(); offset += MESSAGE_META_SIZE {
		buffer := make([]byte, 1)
		mmap.ReadAt(buffer, offset+MESSAGE_META_SIZE-1)
		status := Status(buffer[0])
		if status != STATUS_ACKED {
			break
		}
	}
	return offset
}

func (s *Store) Close() {
	s.meta.Close()
	s.data.Close()
}

func (s *Store) CloseAndDelete() {
	s.Close()
	if s.IsWriteEOF() && s.IsAllAcked() {
		utils.DeleteFile(s.metaPath)
		utils.DeleteFile(s.dataPath)
	}
	log.Info().Str("key", s.Key.String()).Msg("[store] closed and deleted")
}

func (s *Store) IsAllAcked() bool {
	readOffset := findReadOffset(s.meta)
	return readOffset >= s.meta.Size()
}

func (s *Store) Ref() {
	s.Lock()
	defer s.Unlock()
	s.references++
}

func (s *Store) Unref() bool {
	s.Lock()
	defer s.Unlock()
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

func (s *Store) GetReadID() ID {
	s.Lock()
	defer s.Unlock()
	return s.Key.ID + ID(s.readOffset/MESSAGE_META_SIZE)
}

func (s *Store) GetWriteID() ID {
	s.Lock()
	defer s.Unlock()
	return s.id
}

func (s *Store) Put(message MessageData) error {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	err := s.data.Append(message)
	if err != nil {
		return err
	}

	length := message.Size()
	meta := NewMessageMeta(s.GetAndIncrease(), s.writeOffset, length)
	log.Trace().Msgf("[put] <%s> id: %d", s.Key.Priority, meta.ID)
	metaEncoded, err := meta.Encode()
	if err != nil {
		return err
	}
	s.meta.Append(metaEncoded)
	s.writeOffset += length
	s.cond.Signal()
	return nil
}

func (s *Store) Get() (*Message, error) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	for s.IsReadEOF() {
		s.Lock()
		if s.IsWriteEOF() && s.IsReadEOF() {
			s.Unlock()
			return nil, NewEOFError(s.Key.String())
		}
		s.Unlock()
		s.cond.Wait()
	}
	metaBuffer := make([]byte, MESSAGE_META_SIZE)
	s.meta.ReadAt(metaBuffer, s.readOffset)
	meta, err := DecodeMessageMeta(metaBuffer)
	if err != nil {
		return nil, err
	}

	if meta.Status == STATUS_ACKED {
		return s.Get()
	}

	dataBuffer := make([]byte, meta.Length)
	s.data.ReadAt(dataBuffer, meta.Offset)
	msg := &Message{
		ID:       meta.ID,
		Priority: s.Key.Priority,
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
	offset := (int64(id)-int64(s.Key.ID))*MESSAGE_META_SIZE + MESSAGE_META_SIZE - 1
	return s.meta.WriteAt(data, offset, false)
}
