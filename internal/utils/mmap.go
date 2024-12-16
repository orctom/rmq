package utils

// https://stackoverflow.com/questions/69247065/updating-mmap-file-with-struct-in-go

import (
	"fmt"
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

func NewMmap(path string) (*Mmap, error) {
	file, size, err := open(path)
	if err != nil {
		return nil, err
	}
	fd := int(file.Fd())
	ref, err := syscall.Mmap(fd, 0, SIZE_1G, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "[mmap] failed to create mmap")
	}
	mm := &Mmap{
		file: file,
		size: size,
		ref:  ref,
		data: (*[SIZE_1G]byte)(unsafe.Pointer(&ref[0])),
	}
	runtime.SetFinalizer(mm, (*Mmap).Close)
	return mm, nil
}

func open(path string) (*os.File, int64, error) {
	fullpath := ExpandHome(path)
	f, err := os.OpenFile(fullpath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, 0, fmt.Errorf("[mmap] could not open: %s; %w", path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("[mmap] could not stat: %s; %w", path, err)
	}

	size := fi.Size()
	if size < 0 {
		return nil, size, fmt.Errorf("[mmap] negative size: %s", path)
	}
	if size >= SIZE_1G {
		return nil, size, fmt.Errorf(
			"[mmap] too large, max supported: %s, actual: %s, path: %s",
			BytesToHuman(SIZE_1G),
			BytesToHuman(uint64(size)),
			path,
		)
	}
	return f, size, nil
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
