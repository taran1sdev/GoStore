package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var sig = []byte{'G', 'o', 'S', 't', 'o', 'r', 'e', '2', '5'}

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

	metaPage := getMetaPage()
	leafPage := getLeafPage()

	f.Seek(0, io.SeekStart)

	metaSize, wMetaErr := f.Write(metaPage[:])
	if wMetaErr != nil {
		return f, fmt.Errorf("Error writing new Meta page to file: %s", wMetaErr)
	} else if metaSize != PageSize {
		return f, fmt.Errorf("Size mismatch writing Meta page to file: Expected %d Actual: %d", PageSize, metaSize)
	}

	f.Seek(0, io.SeekEnd)

	leafSize, wLeafErr := f.Write(leafPage)
	if wLeafErr != nil {
		return f, fmt.Errorf("Error writing new Leaf page to file: %s", wLeafErr)
	} else if leafSize != PageSize {
		return f, fmt.Errorf("Size mismatch writing Leaf page to file: Expected: %d Actual: %d", PageSize, leafSize)
	}

	f.Seek(0, io.SeekEnd)
	return f, nil
}

func getMetaPage() []byte {
	var pSize [2]byte
	binary.LittleEndian.PutUint16(pSize[:], uint16(PageSize))

	var rootId [4]byte
	binary.LittleEndian.PutUint32(rootId[:], uint32(1))

	page := make([]byte, PageSize)

	copy(page[0:], sig)
	copy(page[len(sig):], pSize[:])
	copy(page[len(sig)+len(pSize):], rootId[:])

	return page
}

func getLeafPage() []byte {
	pType := byte(PageTypeLeaf)

	var nCells [2]byte
	binary.LittleEndian.PutUint16(nCells[:], uint16(0))

	var fStart [2]byte
	binary.LittleEndian.PutUint16(fStart[:], uint16(7))

	var fEnd [2]byte
	binary.LittleEndian.PutUint16(fEnd[:], uint16(4096))

	page := make([]byte, PageSize)

	page[0] = pType
	copy(page[1:], nCells[:])
	copy(page[len(nCells)+1:], fStart[:])
	copy(page[len(fStart)+len(nCells)+1:], fEnd[:])
	return page
}

func ReadPage(id uint32) (*Page, error) {

}

func WritePage(p *Page) error {

}

func AllocatePage() (*Page, error) {

}
