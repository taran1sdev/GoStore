package storage

import (
	"bytes"
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

	// Get file info for size
	fi, _ := f.Stat()
	// Seek the end of the file for appending records
	f.Seek(0, io.SeekEnd)
	return &Storage{
		File:        f,
		writeOffset: fi.Size(),
		path:        path,
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
