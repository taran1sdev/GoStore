package storage

import "encoding/binary"

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
		page: page,
	}
}
