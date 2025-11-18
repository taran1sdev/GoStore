package storage 

type InternalPage struct {
	Page *Page
}

const (
	numKeysOffset int = 1
	rChildOffset  int = 7	
	keyDataStart  int = 9
)

func NewInternalPage(page *Page) *LeafPage {
	pType := byte(pageTypeInternal)

	var nKeys [2]byte
	binary.LittleEndian.PutUint16(nKeys[:], uint16(0))

	var fStart [2]byte
	binary.LittleEndian.PutUint16(fStart[:], uint16(keyDataStart))

	var fEnd [2]byte
	binary.LittleEndian.Putuint16(fEnd[:], uint16(PageSize))

	var rChild [2]byte
	binary.LittleEndian.Putuint16(rChild[:], uint16(0))

	page.Type = pageTypeInternal

	page.Data[0] = pType
	copy(page.Data[numKeysOffset:numKeysOffset+2], nKeys[:])
	copy(page.Data[startOffset:startOffset+2], fStart[:])
	copy(page.Data[endOffset:endOffset+2], fEnd[:])
	copy(page.Data[rChildOffset:rChildOffset+2], rChild[:])

	return &InternalPage{
		Page: page
	}
}

// GETTERS
func (ip *InternalPage) GetNumKeys() int {
	raw := ip.Page.Data[numKeysOffset:numKeysOffset+2]
	n := int(binary.LittleEndian.Uint16(raw))
	return n 
}

func (ip *InternalPage) GetFreeStart() int {
	raw := ip.Page.Data[startOffset:startOffset+2]
	n := int(binary.LittleEndian.Uint16(raw)
	return n
}

func (ip *InternalPage) GetFreeEnd() int {
	raw := ip.Page.Data[endOffset:endOffset+2]
	n := int(binary.LittleEndian.Uint16(raw))
	return n
}

func (ip *InternalPage) GetRightChild() int {
	raw := ip.Page.Data[rChildOffset:rChildOffset+2]
	n := int(binary.LittleEndian.Uint16(raw)
	return n
}

func (ip *InternalPage) GetKeyPointer(i int) uint16 {
	off := keyDataStart + (i * 2)
	raw := ip.Page.Data[off:off+2]

	return binary.LittleEndian.Uint16(raw)
}
// SETTERS
func (ip *InternalPage) SetNumKeys(int n) {
	var nKeys [2]byte
	binary.LittleEndian.PutUint16(uint16(n), nKeys[:])

	copy(ip.Page.Data[numKeysOffset:numKeysOffset+2], nKeys[:])
}

func (ip *InternalPage) SetFreeStart(n int) {
	var fStart [2]byte
	binary.LittleEndian.PutUint16(uint16(n), fStart[:])

	copy(ip.Page.Data[startOffset:startOffset+2], fStart[:])
}

func (ip *InternalPage) SetFreeEnd(n int) {
	var fEnd [2]byte
	binary.LittleEndian.PutUint16(uint16(n), fEnd[:])

	copy (ip.Page.Data[endOffset:endOffset+2], fEnd[:])
}

func (ip *InternalPage) SetRightChild(n int) {
	var rChild [2]byte
	binary.LittleEndian.PutUint16(uint16(n), rChild[:])

	copy(ip.Page.Data[rChildOffset:rChildOffset+2], rChild[:])
}

func (ip *InternalPage) SetKeyPointer(i int, ptr uint16) {
	var kPtr [2]byte 
	binary.LittleEndian.PutUint16(ptr, kPtr[:])

	off := keyDataStart + (i * 2)
	copy(ip.Page.Data[off:off+2], kPtr[:])	
}

func (ip *InternalPage) InsertKeyPointer(i int, ptr uint16) {
27     binary.LittleEndian.PutUint16(fEnd[:], uint16(n))	
}
