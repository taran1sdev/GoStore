package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type InternalPage struct {
	Page *Page
}

const (
	numKeysOffset    int = 1
	rChildOffset     int = 7
	childStartOffset int = 11
	keyPointerOffset     = childStartOffset + (maxChildren * 4)
)

func NewInternalPage(page *Page) *InternalPage {
	pType := byte(pageTypeInternal)

	var nKeys [2]byte
	binary.LittleEndian.PutUint16(nKeys[:], uint16(0))

	var fStart [2]byte
	binary.LittleEndian.PutUint16(fStart[:], uint16(keyPointerOffset))

	var fEnd [2]byte
	binary.LittleEndian.PutUint16(fEnd[:], uint16(PageSize))

	var rChild [4]byte
	binary.LittleEndian.PutUint32(rChild[:], uint32(0))

	page.Type = pageTypeInternal

	page.Data[0] = pType
	copy(page.Data[numKeysOffset:numKeysOffset+2], nKeys[:])
	copy(page.Data[startOffset:startOffset+2], fStart[:])
	copy(page.Data[endOffset:endOffset+2], fEnd[:])
	copy(page.Data[rChildOffset:rChildOffset+4], rChild[:])

	return &InternalPage{
		Page: page,
	}
}

// GETTERS
func (ip *InternalPage) GetNumKeys() int {
	raw := ip.Page.Data[numKeysOffset : numKeysOffset+2]
	n := int(binary.LittleEndian.Uint16(raw))
	return n
}

func (ip *InternalPage) GetFreeStart() int {
	raw := ip.Page.Data[startOffset : startOffset+2]
	n := int(binary.LittleEndian.Uint16(raw))
	return n
}

func (ip *InternalPage) GetFreeEnd() int {
	raw := ip.Page.Data[endOffset : endOffset+2]
	n := int(binary.LittleEndian.Uint16(raw))
	return n
}

func (ip *InternalPage) GetRightChild() int {
	raw := ip.Page.Data[rChildOffset : rChildOffset+4]
	n := int(binary.LittleEndian.Uint32(raw))
	return n
}

func (ip *InternalPage) GetChild(i int) uint32 {
	off := childStartOffset + (i * 4)
	raw := ip.Page.Data[off : off+4]

	return binary.LittleEndian.Uint32(raw)
}

func (ip *InternalPage) GetKeyPointer(i int) uint16 {
	off := keyPointerOffset + (i * 2)
	raw := ip.Page.Data[off : off+2]
	return binary.LittleEndian.Uint16(raw)
}

// SETTERS
func (ip *InternalPage) SetNumKeys(n int) {
	var nKeys [2]byte
	binary.LittleEndian.PutUint16(nKeys[:], uint16(n))

	copy(ip.Page.Data[numKeysOffset:numKeysOffset+2], nKeys[:])
}

func (ip *InternalPage) SetFreeStart(n int) {
	var fStart [2]byte
	binary.LittleEndian.PutUint16(fStart[:], uint16(n))

	copy(ip.Page.Data[startOffset:startOffset+2], fStart[:])
}

func (ip *InternalPage) SetFreeEnd(n int) {
	var fEnd [2]byte
	binary.LittleEndian.PutUint16(fEnd[:], uint16(n))

	copy(ip.Page.Data[endOffset:endOffset+2], fEnd[:])
}

func (ip *InternalPage) SetRightChild(n int) {
	var rChild [4]byte
	binary.LittleEndian.PutUint32(rChild[:], uint32(n))

	copy(ip.Page.Data[rChildOffset:rChildOffset+4], rChild[:])
}

func (ip *InternalPage) SetChild(i int, ptr uint32) {
	var kPtr [4]byte
	binary.LittleEndian.PutUint32(kPtr[:], uint32(ptr))

	off := childStartOffset + (i * 4)
	copy(ip.Page.Data[off:off+4], kPtr[:])
}

func (ip *InternalPage) InsertChildPointer(i int, childPageID uint32) {
	n := ip.GetNumKeys()

	for j := n; j >= i; j-- {
		child := ip.GetChild(j)
		ip.SetChild(j+1, child)
	}

	ip.SetChild(i, childPageID)
}

func (ip *InternalPage) SetKeyPointer(i int, ptr uint16) {
	var kPtr [2]byte
	binary.LittleEndian.PutUint16(kPtr[:], ptr)

	off := keyPointerOffset + (i * 2)
	copy(ip.Page.Data[off:off+2], kPtr[:])
}

func (ip *InternalPage) InsertKeyPointer(i int, ptr uint16) {
	n := ip.GetNumKeys()

	for j := n - 1; j >= i; j-- {
		ptrVal := ip.GetKeyPointer(j)
		ip.SetKeyPointer(j+1, ptrVal)
	}

	ip.SetKeyPointer(i, ptr)

	ip.SetNumKeys(n + 1)
	ip.SetFreeStart(keyPointerOffset + ((n + 1) * 2))
}

func (ip *InternalPage) FindInsertIndex(key []byte) int {
	n := ip.GetNumKeys()

	low, high := 0, n

	for low < high {
		mid := (low + high) / 2
		midPtr := ip.GetKeyPointer(mid)
		midKey := ip.ReadKey(midPtr)
		cmp := bytes.Compare(key, midKey)
		if cmp <= 0 {
			high = mid
		} else {
			low = mid + 1
		}
	}

	return low
}

func (ip *InternalPage) InsertKey(key []byte) error {
	idx := ip.FindInsertIndex(key)

	off, err := ip.WriteKey(key)
	if err != nil {
		return err
	}

	ip.InsertKeyPointer(idx, off)
	return nil
}

func (ip *InternalPage) ReadKey(off uint16) []byte {
	pos := int(off)
	keyLen := int(binary.LittleEndian.Uint16(ip.Page.Data[pos : pos+2]))

	pos += 2

	return ip.Page.Data[pos : pos+keyLen]
}

func (ip *InternalPage) WriteKey(key []byte) (uint16, error) {
	var keyLen [2]byte
	binary.LittleEndian.PutUint16(keyLen[:], uint16(len(key)))

	recordLen := 2 + len(key)

	off := ip.GetFreeEnd() - recordLen

	if off < ip.GetFreeStart() {
		return 0, fmt.Errorf("Not enough space to write key")
	}

	copy(ip.Page.Data[off:off+2], keyLen[:])

	copy(ip.Page.Data[off+2:off+2+len(key)], key[:])

	ip.SetFreeEnd(off)
	return uint16(off), nil
}
