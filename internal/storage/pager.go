package storage

import (
	"bytes"
	"encoding/binary"
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
	if _, err := io.ReadFull(f, h); err != nil {
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
	page := NewPage()
	page.ID = id

	offset := int64(id) * PageSize

	if _, sErr := pager.file.Seek(offset, io.SeekStart); sErr != nil {
		return nil, fmt.Errorf("Failed to seek to page with id %d: %s", id, sErr)
	}

	read, rErr := io.ReadFull(pager.file, page.Data)
	if rErr != nil {
		return nil, fmt.Errorf("Error occured while reading page: %s", rErr)
	}

	if read != PageSize {
		return nil, fmt.Errorf("Data read does not match page size: Expected %d Actual: %d", PageSize, read)
	}

	page.Type = PageType(page.Data[0])
	return page, nil
}

func (pager *Pager) WritePage(page *Page) error {
	offset := int64(page.ID) * PageSize

	if _, sErr := pager.file.Seek(offset, io.SeekStart); sErr != nil {
		return fmt.Errorf("Failed to seek to page with id %d: %s", int64(page.ID), sErr)
	}

	wrote, wErr := pager.file.Write(page.Data)
	if wErr != nil {
		return fmt.Errorf("Failed to write page: %s", wErr)
	}

	if wrote != PageSize {
		return fmt.Errorf("Data written does not match page size: Expected %d Actual: %d", PageSize, wrote)
	}

	return nil
}

func (pager *Pager) AllocatePage() *Page {
	metaP, _ := pager.ReadPage(0)
	meta := WrapMetaPage(metaP)

	head := meta.GetFreeHead()
	if head != InvalidPage && head < pager.numPages {
		fmt.Printf("Reallocating Free Page: %d\n", int(head))

		freePage, err := pager.ReadPage(head)
		if err != nil {
			fmt.Printf("AllocatePage: Unable to read free page %d: %v, resetting freeHead\n",
				head, err)
			meta.SetFreeHead(InvalidPage)
			_ = pager.WritePage(meta.Page)
			goto newPage
		}

		nextPage := binary.LittleEndian.Uint32(freePage.Data[1:5])
		if nextPage != InvalidPage && nextPage >= pager.numPages {
			fmt.Printf("AllocatePage: invalid next free page %d, resetting list\n", nextPage)
			nextPage = InvalidPage
		}

		meta.SetFreeHead(nextPage)
		pager.WritePage(meta.Page)

		freePage.Data = make([]byte, PageSize)
		freePage.Type = PageTypeFree
		freePage.ID = head

		return freePage
	}

newPage:
	id := pager.numPages
	pager.numPages++
	p := NewPage()
	p.ID = id
	return p
}
