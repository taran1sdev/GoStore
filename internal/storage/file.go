package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type Storage struct {
	File        *os.File
	writeOffset int64
	path        string
}

var sig = []byte{'G', 'S', 't', 'o', 'r', 'e', '2', '5', '\n'}

// Open a database file and return the storage object to handle file operations
func Open(path string) (*Storage, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if errors.Is(err, os.ErrNotExist) {
		// If database file does not exist then create it
		f, err = createDatabase(path)
	}

	if err != nil {
		return nil, err
	}

	// Check the file has a valid signature
	if valid, sigErr := checkSignature(f); !valid {
		return nil, sigErr
	}

	f.Seek(0, io.SeekStart)
	return &Storage{
		File: f,
		path: path,
	}, nil
}

func createDatabase(path string) (*os.File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("Unable to create file %s: %w", path, err)
	}

	// Write the header to the file
	_, wErr := f.Write(sig)
	if wErr != nil {
		return nil, fmt.Errorf("Failed to write header to db file %s: %w", path, wErr)
	}

	f.Sync()
	f.Seek(0, io.SeekStart)

	return f, nil
}

func checkSignature(f *os.File) (bool, error) {
	h := make([]byte, len(sig))

	_, err := f.Read(h)
	if err != nil {
		return false, fmt.Errorf("Unable to read db file %s: %w", f.Name(), err)
	}

	if !bytes.Equal(h, sig) {
		return false, fmt.Errorf("Invalid file signature")
	}

	f.Seek(0, io.SeekStart)
	return true, nil
}

var ERROR_CORRUPT_RECORD = fmt.Errorf("Encountered a Corrupt Record")

const (
	maxKeyLength uint16 = 1024
	maxValLength uint16 = 32768
)

// This is extremely inefficient but later we will optimize file parsing
// For now accuracy matters more
func parseRecord(f *os.File) (*Record, error) {
	keyLenRaw := make([]byte, 2)
	valLenRaw := make([]byte, 2)

	// Parse key / value lengths from file
	kRead, kErr := io.ReadFull(f, keyLenRaw)
	if errors.Is(kErr, io.EOF) && kRead == 0 {
		return nil, io.EOF
	} else if errors.Is(kErr, io.ErrUnexpectedEOF) {
		return nil, ERROR_CORRUPT_RECORD
	} else if kErr != nil {
		return nil, fmt.Errorf("Encountered an error during record parsing: %w", kErr)
	}

	_, vErr := io.ReadFull(f, valLenRaw)
	if vErr != nil {
		return nil, ERROR_CORRUPT_RECORD
	}

	// Convert the values
	keyLen := binary.LittleEndian.Uint16(keyLenRaw)
	valLen := binary.LittleEndian.Uint16(valLenRaw)

	// Make sure key and value lengths are within bounds
	if keyLen > maxKeyLength || valLen > maxValLength {
		return nil, ERROR_CORRUPT_RECORD
	}

	flagRaw := make([]byte, 1)
	// Parse and convert flags
	_, fErr := io.ReadFull(f, flagRaw)
	if fErr != nil {
		return nil, ERROR_CORRUPT_RECORD
	}

	flag := Flag(flagRaw[0])

	// Parse the key and value
	key := make([]byte, keyLen)
	_, keyErr := io.ReadFull(f, key)
	if keyErr != nil {
		return nil, ERROR_CORRUPT_RECORD
	}

	value := make([]byte, valLen)
	_, valErr := io.ReadFull(f, value)
	if valErr != nil {
		return nil, ERROR_CORRUPT_RECORD
	}

	return &Record{
		Key:   key,
		Value: value,
		Flag:  flag,
	}, nil
}

func (s *Storage) Replay() ([]*Record, error) {
	var records []*Record

	// Skip the signature
	s.File.Seek(int64(len(sig)), io.SeekStart)

	for {
		r, err := parseRecord(s.File)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			s.File.Seek(0, io.SeekEnd)
			return nil, err
		}

		records = append(records, r)
	}

	pos, _ := s.File.Seek(0, io.SeekCurrent)
	s.writeOffset = pos
	return records, nil
}
