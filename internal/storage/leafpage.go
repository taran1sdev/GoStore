package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type LeafPage struct {
	Page *Page
}

const (
	numCellsOffset int = 1
	startOffset    int = 3
	endOffset      int = 5
	dataStart      int = 7
)

func NewLeafPage(page *Page) *LeafPage {
	pType := byte(PageTypeLeaf)

	var nCells [2]byte
	binary.LittleEndian.PutUint16(nCells[:], uint16(0))

	var fStart [2]byte
	binary.LittleEndian.PutUint16(fStart[:], uint16(dataStart))

	var fEnd [2]byte
	binary.LittleEndian.PutUint16(fEnd[:], uint16(PageSize))

	page.Type = PageTypeLeaf

	page.Data[0] = pType
	copy(page.Data[numCellsOffset:], nCells[:])
	copy(page.Data[startOffset:], fStart[:])
	copy(page.Data[endOffset:], fEnd[:])
	return &LeafPage{
		Page: page,
	}
}

// GETTERS
func (lp *LeafPage) GetNumCells() int {
	raw := lp.Page.Data[numCellsOffset : numCellsOffset+2]
	nCells := int(binary.LittleEndian.Uint16(raw))
	return nCells
}

func (lp *LeafPage) GetFreeStart() int {
	raw := lp.Page.Data[startOffset : startOffset+2]
	fStart := int(binary.LittleEndian.Uint16(raw))
	return fStart
}

func (lp *LeafPage) GetFreeEnd() int {
	raw := lp.Page.Data[endOffset : endOffset+2]
	fEnd := int(binary.LittleEndian.Uint16(raw))
	return fEnd
}

func (lp *LeafPage) GetCellPointer(i int) uint16 {
	off := dataStart + (i * 2)
	raw := lp.Page.Data[off : off+2]

	return binary.LittleEndian.Uint16(raw)
}

// SETTERS
func (lp *LeafPage) SetNumCells(n int) {
	var nCells [2]byte
	binary.LittleEndian.PutUint16(nCells[:], uint16(n))

	copy(lp.Page.Data[numCellsOffset:], nCells)
}

func (lp *LeafPage) SetFreeStart(n int) {
	var fStart [2]byte
	binary.LittleEndian.PutUint16(fStart[:], uint16(n))

	copy(lp.Page.Data[startOffset:], fStart[:])
}

func (lp *LeafPage) SetFreeEnd(n int) {
	var fEnd [2]byte
	binary.LittleEndian.PutUint16(fEnd[:], uint16(n))

	copy(lp.Page.Data[endOffset:], fEnd[:])
}

func (lp *LeafPage) SetCellPointer(i int, ptr uint16) {
	var cPtr [2]byte
	binary.LittleEndian.PutUint16(cPtr[:], ptr)

	off := dataStart + (i * 2)
	copy(lp.Page.Data[off:off+2], cPtr[:])
}

func (lp *LeafPage) InsertCellPointer(i int, ptr uint16) {
	n := lp.GetNumCells()

	for j := n - 1; j >= i; j-- {
		ptrVal := lp.GetCellPointer(j)
		lp.SetCellPointer(j+1, ptrVal)
	}

	lp.SetCellPointer(i, ptr)
	lp.SetNumCells(n + 1)

	lp.SetFreeStart(dataStart + ((n + 1) * 2))
}

func (lp *LeafPage) FindInsertIndex(key []byte) int {
	n := lp.GetNumCells()

	low, high := 0, n

	for low < high {
		mid := (low + high) / 2
		midPtr := lp.GetCellPointer(mid)
		midKey := lp.ReadKey(midPtr)
		cmp := bytes.Compare(key, midKey)
		if cmp <= 0 {
			high = mid
		} else {
			low = mid + 1
		}
	}

	return low
}

func (lp *LeafPage) Insert(key, val []byte) error {
	idx := lp.FindInsertIndex(key)

	off, err := lp.WriteRecord(key, val)
	if err != nil {
		return err
	}

	lp.InsertCellPointer(idx, off)

	return nil
}

// RECORD READ / WRITE
func (lp *LeafPage) WriteRecord(key, val []byte) (uint16, error) {
	var keyLen [2]byte
	binary.LittleEndian.PutUint16(keyLen[:], uint16(len(key)))

	var valLen [2]byte
	binary.LittleEndian.PutUint16(valLen[:], uint16(len(val)))

	recordLen := len(keyLen) + len(valLen) + len(key) + len(val)
	off := lp.GetFreeEnd() - recordLen

	if off < lp.GetFreeStart() {
		return 0, fmt.Errorf("Not enough space to write record")
	}

	pos := off
	copy(lp.Page.Data[pos:pos+2], keyLen[:])
	pos += 2
	copy(lp.Page.Data[pos:pos+2], valLen[:])
	pos += 2
	copy(lp.Page.Data[pos:pos+len(key)], key[:])
	pos += len(key)
	copy(lp.Page.Data[pos:pos+len(val)], val[:])

	lp.SetFreeEnd(off)
	return uint16(off), nil
}

func (lp *LeafPage) ReadRecord(off uint16) (key, val []byte) {
	pos := int(off)

	keyLen := int(binary.LittleEndian.Uint16(lp.Page.Data[pos : pos+2]))
	valLen := int(binary.LittleEndian.Uint16(lp.Page.Data[pos+2 : pos+4]))

	keyStart := pos + 4
	valStart := pos + 4 + keyLen

	key = lp.Page.Data[keyStart : keyStart+keyLen]
	val = lp.Page.Data[valStart : valStart+valLen]
	return
}

func (lp *LeafPage) ReadKey(off uint16) (key []byte) {
	pos := int(off)

	keyLen := int(binary.LittleEndian.Uint16(lp.Page.Data[pos : pos+2]))

	keyStart := pos + 4

	key = lp.Page.Data[keyStart : keyStart+keyLen]
	return
}
