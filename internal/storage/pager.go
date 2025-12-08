package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"go.store/internal/logger"
)

type Pager struct {
	file      *os.File
	filePath  string
	wal       *WAL
	log       *logger.Logger
	pageSize  int
	numPages  uint32
	Replaying bool
}

func Open(path string, log *logger.Logger) (*Pager, error) {
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
		return nil, fmt.Errorf("Open %w", ErrCorruptFile)
	}

	pager := &Pager{
		file:     f,
		filePath: path,
		log:      log,
		pageSize: PageSize,
		numPages: uint32(size / PageSize),
	}

	wal, wErr := OpenWAL(path, pager, log)
	if wErr != nil {
		return nil, wErr
	}

	pager.wal = wal

	if err := wal.Replay(); err != nil {
		return nil, err
	}

	return pager, nil
}

func checkSignature(f *os.File) error {
	if _, sErr := f.Seek(1, io.SeekStart); sErr != nil {
		return fmt.Errorf("Error seeking start of file: %s", sErr)
	}

	h := make([]byte, len(sig))
	if _, err := io.ReadFull(f, h); err != nil {
		return fmt.Errorf("Error reading magic bytes: %s", err)
	}

	if !bytes.Equal(h, sig) {
		return ErrInvalidFileSig
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
	if !pager.Replaying {
		if err := pager.wal.LogPage(page); err != nil {
			fmt.Printf("Error: %v", err)
		}
	}

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
		freePage, err := pager.ReadPage(head)
		if err != nil {
			pager.log.Warnf("AllocatePage: %v (page:%d)", ErrCorruptFreeList, head)
			meta.SetFreeHead(InvalidPage)
			_ = pager.WritePage(meta.Page)
			goto newPage
		}

		nextPage := binary.LittleEndian.Uint32(freePage.Data[1:5])
		if nextPage != InvalidPage && nextPage >= pager.numPages {
			pager.log.Warnf("AllocatePage: %v (next:%d)", ErrCorruptFreeList, nextPage)
			nextPage = InvalidPage
		}

		meta.SetFreeHead(nextPage)
		pager.WritePage(meta.Page)

		freePage.Data = make([]byte, PageSize)
		freePage.Type = PageTypeFree
		freePage.Data[0] = byte(PageTypeFree)
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

func (pager *Pager) Sync() error {
	return pager.file.Sync()
}

func (pager *Pager) Close() error {
	if err := pager.wal.Checkpoint(); err != nil {
		return err
	}
	pager.wal.file.Close()
	return os.Remove(pager.wal.filePath)
}
