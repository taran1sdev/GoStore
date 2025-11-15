package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

type Pager struct {
	file     *os.File
	pageSize int
	numPages uint32
}

func Open(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if errors.Is(err, os.ErrNotExist) {
		f, err = createDatabase(path)
	}

	if err != nil {
		return nil, fmt.Errorf("Error opening DB file: %s", err)
	}

	if sigErr := checkSignature(f); sigErr != nil {
		f.Seek(0, io.SeekEnd)
		return nil, sigErr
	}

	info, statErr := f.Stat()
	if statErr != nil {
		f.Seek(0, io.SeekEnd)
		return nil, fmt.Errorf("Error getting file stats: %s", statErr)
	}

	size := info.Size()
	if size%PageSize != 0 {
		f.Seek(0, io.SeekEnd)
		return nil, fmt.Errorf("Corrupt DB file")
	}

	return &Pager{
		file:     f,
		pageSize: PageSize,
		numPages: uint32(size / PageSize),
	}, nil
}

func checkSignature(f *os.File) error {
	if _, sErr := f.Seek(0, io.SeekStart); sErr != nil {
		return fmt.Errorf("Error seeking start of file: %s", sErr)
	}

	h := make([]byte, len(sig))
	if _, err := f.Read(h); err != nil {
		return fmt.Errorf("Error reading magic bytes: %s", err)
	}

	if !bytes.Equal(h, sig) {
		return fmt.Errorf("Invalid file signature")
	}
	return nil
}

func createDatabase(path string) (*os.File, error) {
	f, cErr := os.Create(path)
	if cErr != nil {
		return nil, fmt.Errorf("Unable to create file %s: %s", path, cErr)
	}

	mPage := NewPage()
	lPage := NewPage()

	metaPage := NewMetaPage(mPage)
	leafPage := NewLeafPage(lPage)

	f.Seek(0, io.SeekStart)

	metaSize, wMetaErr := f.Write(metaPage.Page.Data)
	if wMetaErr != nil {
		return f, fmt.Errorf("Error writing new Meta page to file: %s", wMetaErr)
	} else if metaSize != PageSize {
		return f, fmt.Errorf("Size mismatch writing Meta page to file: Expected %d Actual: %d", PageSize, metaSize)
	}

	f.Seek(0, io.SeekEnd)

	leafSize, wLeafErr := f.Write(leafPage.Page.Data)
	if wLeafErr != nil {
		return f, fmt.Errorf("Error writing new Leaf page to file: %s", wLeafErr)
	} else if leafSize != PageSize {
		return f, fmt.Errorf("Size mismatch writing Leaf page to file: Expected: %d Actual: %d", PageSize, leafSize)
	}

	f.Seek(0, io.SeekEnd)
	return f, nil
}

func (pager *Pager) ReadPage(id uint32) (*Page, error) {

}

func (pager *Pager) WritePage(p *Page) error {

}

func (pager *Pager) AllocatePage() (*Page, error) {

}
