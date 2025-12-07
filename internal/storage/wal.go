package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Write-Ahead Log stores changes to pages before they are written to disk
// allowing the application to recover from crashes

type WAL struct {
	file  *os.File
	pager *Pager
	size  int64
}

var ErrInvalidChecksum = errors.New("invalid checksum")

// Replay / Truncate every 25 writes (100MB+)
const WALCheckpointSize = 1024 * 1024 * 100

// WAL file structure
// Page ID: uint32
// Page Data: []byte PageSize
// Checksum: uint32

func OpenWAL(path string, pager *Pager) (*WAL, error) {
	f, err := os.OpenFile(path+".wal", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	info, _ := f.Stat()
	return &WAL{
		file:  f,
		pager: pager,
		size:  info.Size(),
	}, nil
}

func (wal *WAL) LogPage(page *Page) error {
	buf := make([]byte, 8+PageSize)

	binary.LittleEndian.PutUint32(buf[0:4], page.ID)
	copy(buf[4:], page.Data)

	// Add a checksum to verify the integrity of the log
	csum := crc32.ChecksumIEEE(page.Data)
	binary.LittleEndian.PutUint32(buf[4+PageSize:], csum)

	n, err := wal.file.Write(buf)
	if err != nil {
		return err
	}

	wal.size += int64(n)

	if wal.size >= WALCheckpointSize {
		return wal.Checkpoint()
	}
	return nil
}

func (wal *WAL) Checkpoint() error {
	if err := wal.Replay(); err != nil {
		return err
	}
	return wal.pager.Sync()
}

// This function reads any entries in our WAL and applies them to the DB file
func (wal *WAL) Replay() error {
	wal.pager.Replaying = true
	defer func() {
		wal.pager.Replaying = false
	}()

	wal.file.Seek(0, io.SeekStart)

	header := make([]byte, 4)
	for {
		_, err := io.ReadFull(wal.file, header)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			return err
		}

		id := binary.LittleEndian.Uint32(header)

		data := make([]byte, PageSize)
		_, err = io.ReadFull(wal.file, data)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			return err
		}

		csum := make([]byte, 4)
		_, err = io.ReadFull(wal.file, csum)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			return err
		}

		crc := binary.LittleEndian.Uint32(csum)

		if crc != crc32.ChecksumIEEE(data) {
			return fmt.Errorf("Page %d: %v", int(id), ErrInvalidChecksum)
		}

		page := NewPage()
		page.ID = id
		copy(page.Data, data)

		if err := wal.pager.WritePage(page); err != nil {
			return err
		}
	}

	if err := wal.file.Sync(); err != nil {
		return err
	}
	return wal.Truncate()
}

// Remove the log entries
func (wal *WAL) Truncate() error {
	if err := wal.file.Truncate(0); err != nil {
		return err
	}
	wal.size = 0
	return wal.file.Sync()
}
