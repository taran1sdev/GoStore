package storage

import "encoding/binary"

type InternalPage struct {
	Page *Page
}

const (
	numKeysOffset    int = 1
	rChildOffset     int = 7
	childStartOffset int = 11
)

func NewInternalPage(page *Page) *InternalPage {
	pType := byte(pageTypeInternal)

	var nKeys [2]byte
	binary.LittleEndian.PutUint16(nKeys[:], uint16(0))

	var fStart [2]byte
	binary.LittleEndian.PutUint16(fStart[:], uint16(childStartOffset))

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

// READ / WRITE
func (ip *InternalPage) ReadKey(off uint16) []byte {
	pos := int(off)
	keyLen := int(binary.LittleEndian.Uint16(ip.Page.Data[pos : pos+2]))

	pos += 2

	return ip.Page.Data[pos : pos+keyLen]
}
