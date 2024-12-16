package queue

// Store: 存储数据到硬盘，使用mmap，
// 一个文件存数据，一个文件存meta信息，meta信息包括了数据的id，offset，长度，优先级。这些都是encode为定长的byte数组。
// 读取数据时，根据id和offset定位到data文件中，然后根据meta信息的长度读取数据。

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/utils"
)

// =========================== store ===========================

type Store struct {
	Queue       string
	Priority    Priority
	meta        *utils.Mmap
	data        *utils.Mmap
	readOffset  int64
	writeOffset int64
	id          ID
	lastTick    time.Time
	sync.Mutex
}

func NewStore(queue string, priority Priority, startID ID) (*Store, error) {
	metaPath := fmt.Sprintf("~/.rmq/queue/%s/%d-%s.meta", queue, priority, startID)
	dataPath := fmt.Sprintf("~/.rmq/queue/%s/%d-%s.data", queue, priority, startID)
	log.Debug().Msgf("meta path: %s", metaPath)
	log.Debug().Msgf("data path: %s", dataPath)

	return newStore(queue, priority, startID, metaPath, dataPath)
}

func FindCurrentStore(queue string, priority Priority) (*Store, error) {
	dir := os.DirFS(fmt.Sprintf("queue/%s/", queue))
	files, err := fs.Glob(dir, fmt.Sprintf("%d-*.meta", priority))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		metaPath := fmt.Sprintf("~/.rmq/queue/%s/%d-%s.meta", queue, priority, ID(0))
		dataPath := fmt.Sprintf("~/.rmq/queue/%s/%d-%s.data", queue, priority, ID(0))
		fmt.Printf("create meta: %s\n", metaPath)
		fmt.Printf("create data: %s\n", dataPath)
		return newStore(queue, priority, 0, metaPath, dataPath)
	}

	sort.Strings(files)
	metaPath := files[len(files)-1]
	dataPath := strings.Replace(metaPath, ".meta", ".data", -1)
	return newStore(queue, priority, 0, metaPath, dataPath)
}

func newStore(queue string, priority Priority, startID ID, metaPath string, dataPath string) (*Store, error) {
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

	fmt.Printf("[store] read offset %d, write offset %d, id: %d\n", readOffset, writeOffset, id)
	return &Store{
		Queue:       queue,
		Priority:    priority,
		meta:        meta,
		data:        data,
		readOffset:  readOffset,
		writeOffset: writeOffset,
		id:          id,
		lastTick:    time.Now(),
	}, nil
}

func findCurrentID(mmap *utils.Mmap) ID {
	offset := mmap.Size() - MESSAGE_META_SIZE
	idBytes := make([]byte, 8)
	mmap.ReadAt(idBytes, offset)
	return ID(ORDER.Uint64(idBytes))
}

func (s *Store) Preview() {
	var offset int64 = 0
	for offset = 0; offset < s.meta.Size(); offset += MESSAGE_META_SIZE {
		buffer := make([]byte, MESSAGE_META_SIZE)
		s.meta.ReadAt(buffer, offset)
		meta, _ := DecodeMessageMeta(buffer)
		fmt.Printf("[preview] id: %d, offset: %d, len: %d, status: %s\n", meta.ID, meta.Offset, meta.Length, meta.Status)
	}
}

func findReadOffset(mmap *utils.Mmap) int64 {
	var offset int64 = 0
	for offset = 0; offset < mmap.Size(); offset += MESSAGE_META_SIZE {
		buffer := make([]byte, 1)
		mmap.ReadAt(buffer, offset+MESSAGE_META_SIZE-1)
		status := Status(buffer[0])
		if status == STATUS_READY {
			break
		}
	}
	return offset
}

func (s *Store) Close() {
	s.meta.Close()
	s.data.Close()
}

func (s *Store) NextID() ID {
	s.Lock()
	defer s.Unlock()
	s.id++
	return s.id
}

func (s *Store) Put(message MessageData) error {
	err := s.data.Append(message)
	if err == nil {
		length := message.Size()
		meta := NewMessageMeta(s.NextID(), s.writeOffset, length)
		fmt.Printf("[msg meta] id: %d, offset: %d, len: %d\n", meta.ID, meta.Offset, meta.Length)
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
	fmt.Printf("[get meta] id: %d, offset: %d, len: %d\n", meta.ID, meta.Offset, meta.Length)

	dataBuffer := make([]byte, meta.Length)
	s.data.ReadAt(dataBuffer, meta.Offset)
	msg := &Message{
		ID:       meta.ID,
		Priority: s.Priority,
		Data:     dataBuffer,
	}
	meta.Status = STATUS_SENT
	metaBytes, err := meta.Encode()
	if err != nil {
		return nil, err
	}
	s.meta.WriteAt(metaBytes, s.readOffset, false)
	return msg, nil
}

// =========================== stores ===========================

type Stores struct {
	PRIORITY_NORMAL *Store
	PRIORITY_HIGH   *Store
	PRIORITY_URGENT *Store
}

func NewStores(queue string) *Stores {
	storeNorm, err := FindCurrentStore(queue, PRIORITY_NORMAL)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load norm store for queue: %s", queue)
	}
	storeHigh, err := FindCurrentStore(queue, PRIORITY_HIGH)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load high store for queue: %s", queue)
	}
	storeUrgent, err := FindCurrentStore(queue, PRIORITY_URGENT)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load urgent store for queue: %s", queue)
	}
	return &Stores{
		PRIORITY_NORMAL: storeNorm,
		PRIORITY_HIGH:   storeHigh,
		PRIORITY_URGENT: storeUrgent,
	}
}

// =========================== mamager ===========================

type storeManager struct {
	stores map[string]*Stores
}

func StoreManager() *storeManager {
	var once sync.Once
	var instance *storeManager
	once.Do(func() {
		instance = &storeManager{
			stores: make(map[string]*Stores),
		}
	})
	return instance
}

func (sm *storeManager) GetStores(name string) *Stores {
	stores, exists := sm.stores[name]
	if !exists {
		// store = NewStore(name)
		// sm.stores[name] = store
	}
	return stores
}
