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

var (
	ErrCorruptRecord = fmt.Errorf("Encountered a corrupt record")
	ErrKeyTooLarge   = fmt.Errorf("Maximum key length is 1024 bytes")
	ErrValTooLarge   = fmt.Errorf("Maximum value length is 32768 bytes")
)

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
		return nil, ErrCorruptRecord
	} else if kErr != nil {
		return nil, fmt.Errorf("Encountered an error during record parsing: %w", kErr)
	}

	_, vErr := io.ReadFull(f, valLenRaw)
	if vErr != nil {
		return nil, ErrCorruptRecord
	}

	// Convert the values
	keyLen := binary.LittleEndian.Uint16(keyLenRaw)
	valLen := binary.LittleEndian.Uint16(valLenRaw)

	// Make sure key and value lengths are within bounds
	if keyLen > maxKeyLength || valLen > maxValLength {
		return nil, ErrCorruptRecord
	}

	flagRaw := make([]byte, 1)
	// Parse and convert flags
	_, fErr := io.ReadFull(f, flagRaw)
	if fErr != nil {
		return nil, ErrCorruptRecord
	}

	flag := Flag(flagRaw[0])

	// Parse the key and value
	key := make([]byte, keyLen)
	_, keyErr := io.ReadFull(f, key)
	if keyErr != nil {
		return nil, ErrCorruptRecord
	}

	value := make([]byte, valLen)
	_, valErr := io.ReadFull(f, value)
	if valErr != nil {
		return nil, ErrCorruptRecord
	}

	return &Record{
		Key:   key,
		Value: value,
		Flag:  flag,
	}, nil
}

// Currently our database is actually a log of operations - this functions replays those operations
// and loads the result into memory
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

// I repeat myself with these functions but im not too worried about optimization at this stage

func (s *Storage) AppendSet(key, value []byte) error {
	// Check field lengths are withing parameters
	if len(key) > maxKeyLength {
		return ErrKeyTooLarge
	} else if len(value) > maxValLength {
		return ErrValTooLarge
	}

	r := &Record{
		Key:   key,
		Value: value,
		Flag:  FlagSet,
	}

	err := s.writeRecord(r)

	if err != nil {
		return fmt.Errorf("Failed to write record: %w", err)
	}
	return nil
}

func (s *Storage) AppendDelete(key []byte) error {
	// Make sure it's a valid key
	if len(key) > maxKeyLength {
		return ErrKeyTooLarge
	}

	r := &Record{
		Key:   key,
		Value: nil,
		Flag:  FlagDel,
	}

	err := s.writeRecord(r)

	if err != nil {
		return fmt.Errorf("Failed to write record: %w", err)
	}
	return nil
}

// This function appends records to our operation log
func (s *Storage) writeRecord(r *Record) error {

	// Convert our values correctly
	keyLen, valLen := uint16(len(r.Key)), uint16(len(r.Value))

	// the info fields will always be 5 bytes then add the length of the data
	recordLen := 5 + len(r.Key) + len(r.Value)

	var err error

	// We use a closure to avoid verbosity
	write := func(n int, e error) {
		if err == nil && e != nil {
			err = e
		}
	}

	writeByte := func(e error) {
		if err == nil && e != nil {
			err = e
		}
	}

	// Using a buffer we only need to write to the file once and check for errors less
	buf := bytes.NewBuffer(make([]byte, 0, recordLen))

	var keySlice = [2]byte
	var valSlice = [2]byte

	binary.LittleEndian.PutUint16(keySlice[:], keyLen)
	write(buf.Write(keySlice[:]))

	binary.LittleEndian.PutUint16(valSlice[:], valLen)
	write(buf.Write(valSlice[:]))

	writeByte(buf.WriteByte(byte(r.Flag)))

	write(buf.Write(r.Key))

	if r.Flag == FlagSet {
		write(buf.Write(r.Value))
	}

	if err != nil {
		return fmt.Errorf("Encountered an error writing data to buffer: %w", err)
	}

	// Make sure our pointer is at EOF
	if _, seekErr := s.File.Seek(0, io.SeekEnd); seekErr != nil {
		return fmt.Errorf("Failed to seek to end of file: %w", seekErr)
	}

	// Write the data in the buffer to the file
	l, writeErr := s.File.Write(buf.Bytes())
	if writeErr != nil {
		err = fmt.Errorf("Encountered an error while writing to DB file: %w", writeErr)
	} else if l != recordLen {
		err = fmt.Errorf("Bytes written mismatch: Expected: %d Actual: %d", recordLen, l)
	}

	// Always reset the write offset even when encountering errors
	s.writeOffset = s.File.Seek(0, io.SeekCurrent)
	// err should be null if everything worked
	return err
}
