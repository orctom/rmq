package utils

// https://stackoverflow.com/questions/69247065/updating-mmap-file-with-struct-in-go

import (
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"syscall"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

const (
	SIZE_1G = 1 << 30
)

type Mmap struct {
	file *os.File
	size int64
	ref  []byte
	data *[SIZE_1G]byte
}

func NewMmap(path string) *Mmap {
	file, size := open(path)
	fd := int(file.Fd())
	ref, err := syscall.Mmap(fd, 0, SIZE_1G, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Panic().Err(err).Msgf("[store-crashed] failed to create mmap: %s", path)
	}
	mm := &Mmap{
		file: file,
		size: size,
		ref:  ref,
		data: (*[SIZE_1G]byte)(unsafe.Pointer(&ref[0])),
	}
	runtime.SetFinalizer(mm, (*Mmap).Close)
	return mm
}

func open(path string) (*os.File, int64) {
	fullpath := ExpandHome(path)
	f, err := os.OpenFile(fullpath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Panic().Err(err).Msgf("[store-crashed] failed to open: %s", path)
	}

	fi, err := f.Stat()
	if err != nil {
		log.Panic().Err(err).Msgf("[store-crashed] failed to get stat: %s", path)
	}

	size := fi.Size()
	if size < 0 {
		log.Panic().Msgf("[store-crashed] invalid (negative size): %s", path)
	}
	if size >= SIZE_1G {
		log.Panic().Msgf("[store-crashed] too large, max supported: %s, actual: %s, path: %s",
			BytesToHuman(SIZE_1G),
			BytesToHuman(uint64(size)),
			path)
	}
	return f, size
}

func (mm *Mmap) Size() int64 {
	return mm.size
}

func (mm *Mmap) FileSize() int64 {
	fi, err := mm.file.Stat()
	if err != nil {
		log.Error().Err(err).Send()
	}
	return fi.Size()
}

func (mm *Mmap) Close() error {
	if mm.ref == nil {
		return nil
	}
	defer mm.file.Close()

	err := syscall.Munmap(mm.ref)
	mm.data = nil
	mm.ref = nil
	return err
}

func (mm *Mmap) grow(size int64) error {
	if size <= 0 {
		return nil
	}
	log.Debug().Msgf("grow was: %v, deta: %v", mm.size, size)
	err := mm.file.Truncate(mm.size + size)
	if err != nil {
		return err
	}
	mm.size += size
	return nil
}

func (mm *Mmap) ReadAt(p []byte, offset int64) (err error) {
	if offset < 0 {
		return unix.EINVAL
	}
	if offset > mm.Size() {
		return io.EOF
	}

	old := debug.SetPanicOnFault(true)
	defer func() {
		debug.SetPanicOnFault(old)
		if recover() != nil {
			err = errors.New("Page fault occurred while reading from memory map")
		}
	}()

	n := copy(p, mm.data[offset:])
	if n < len(p) {
		err = io.EOF
	}
	return err
}

func (mm *Mmap) WriteAt(data []byte, offset int64, grow bool) error {
	if offset < 0 {
		offset = 0
	}
	if grow == false && offset >= mm.size {
		return io.EOF
	}
	delta := offset + int64(len(data)) - mm.size
	if delta > 0 && grow == true {
		if err := mm.grow(delta); err != nil {
			return err
		}
	}

	n := copy(mm.data[offset:], data)
	if grow == true && n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func (mm *Mmap) Append(data []byte) error {
	_, err := unix.Pwrite(int(mm.file.Fd()), data, mm.size)
	if err != nil {
		return err
	}
	mm.size += int64(len(data))
	return nil
}
