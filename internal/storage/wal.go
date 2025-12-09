package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"go.store/internal/logger"
)

// Write-Ahead Log stores changes to pages before they are written to disk
// allowing the application to recover from crashes

type WAL struct {
	file     *os.File
	filePath string
	pager    *Pager
	log      *logger.Logger
	size     int64

	mu                sync.Mutex
	checkpointRunning int32
}

// Replay every 100MB
const WALCheckpointSize = 100 * 1024 * 1024

// WAL file structure
// Page ID: uint32
// Page Data: []byte PageSize
// Checksum: uint32

func OpenWAL(path string, pager *Pager, log *logger.Logger) (*WAL, error) {
	f, err := os.OpenFile(path+".wal", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	info, _ := f.Stat()
	wal := &WAL{
		file:     f,
		filePath: path + ".wal",
		pager:    pager,
		log:      log,
		size:     info.Size(),
	}

	return wal, nil
}

func (wal *WAL) LogPage(page *Page) error {
	buf := make([]byte, 8+PageSize)

	binary.LittleEndian.PutUint32(buf[0:4], page.ID)
	copy(buf[4:], page.Data)

	// Add a checksum to verify the integrity of the log
	csum := crc32.ChecksumIEEE(page.Data)
	binary.LittleEndian.PutUint32(buf[4+PageSize:], csum)

	wal.mu.Lock()
	defer wal.mu.Unlock()

	if _, err := wal.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	n, err := wal.file.Write(buf)
	if err != nil {
		return err
	}

	wal.size += int64(n)

	if wal.size >= WALCheckpointSize {
		wal.maybeRequestCheckpoint()
	}
	return nil
}

func (wal *WAL) maybeRequestCheckpoint() {
	if atomic.LoadInt32(&wal.checkpointRunning) == 1 {
		return
	}
	if wal.size >= WALCheckpointSize {
		if !atomic.CompareAndSwapInt32(&wal.checkpointRunning, 0, 1) {
			return
		}
		go func() {
			defer atomic.StoreInt32(&wal.checkpointRunning, 0)
			wal.Checkpoint()
		}()
	}
}

func (wal *WAL) Checkpoint() error {
	wal.pager.write.Lock()
	defer wal.pager.write.Unlock()
	if err := wal.pager.flushDirty(); err != nil {
		fmt.Printf("Error: %v\n", err)
		return err
	}
	return wal.Truncate()
}

// This function reads any entries in our WAL and applies them to the DB file
func (wal *WAL) Replay() error {
	wal.pager.replaying = true
	defer func() {
		wal.pager.replaying = false
	}()

	f, err := os.Open(wal.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	header := make([]byte, 4)
	data := make([]byte, PageSize)
	csum := make([]byte, 4)

	for {
		_, err := io.ReadFull(f, header[:])
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			return err
		}

		id := binary.LittleEndian.Uint32(header)
		_, err = io.ReadFull(f, data[:])
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			return err
		}

		_, err = io.ReadFull(f, csum[:])
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			return err
		}

		crc := binary.LittleEndian.Uint32(csum)

		if crc != crc32.ChecksumIEEE(data) {
			wal.log.Errorf("Replay: checksum does not match on page %d", id)
			return fmt.Errorf("Replay: %w (page=%d)", ErrChecksumMismatch, id)
		}

		page := NewPage()
		page.ID = id
		copy(page.Data, data)

		if err := wal.pager.WritePage(page); err != nil {
			return err
		}
	}

	return wal.Truncate()
}

// Remove the log entries
func (wal *WAL) Truncate() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if err := wal.file.Truncate(0); err != nil {
		return err
	}
	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	wal.size = 0
	return nil
}
