package storage

import "encoding/binary"

type MetaPage struct {
	Page *Page
}

var sig = []byte{'G', 'o', 'S', 't', 'o', 'r', 'e', '2', '5'}

const (
	sizeOffset         int = 9
	rootOffset         int = 15
	freePageHeadOffset int = 19
)

func NewMetaPage(page *Page) *MetaPage {
	var pSize [2]byte
	binary.LittleEndian.PutUint16(pSize[:], uint16(PageSize))

	var rootId [4]byte
	binary.LittleEndian.PutUint32(rootId[:], uint32(1))

	var freeHead [4]byte
	binary.LittleEndian.PutUint32(freeHead[:], InvalidPage)

	copy(page.Data[0:], sig)
	copy(page.Data[sizeOffset:], pSize[:])
	copy(page.Data[rootOffset:], rootId[:])
	copy(page.Data[freePageHeadOffset:], freeHead[:])

	page.Type = PageTypeMeta

	return &MetaPage{
		Page: page,
	}
}

func WrapMetaPage(page *Page) *MetaPage {
	return &MetaPage{
		Page: page,
	}
}

func (mp *MetaPage) GetRootID() uint32 {
	return binary.LittleEndian.Uint32(mp.Page.Data[rootOffset : rootOffset+4])
}

func (mp *MetaPage) SetRootID(id uint32) {
	var rootId [4]byte
	binary.LittleEndian.PutUint32(rootId[:], id)

	copy(mp.Page.Data[rootOffset:rootOffset+4], rootId[:])
}

func (mp *MetaPage) GetFreeHead() uint32 {
	return binary.LittleEndian.Uint32(mp.Page.Data[freePageHeadOffset : freePageHeadOffset+4])
}

func (mp *MetaPage) SetFreeHead(id uint32) {
	var freeHead [4]byte
	binary.LittleEndian.PutUint32(freeHead[:], id)

	copy(mp.Page.Data[freePageHeadOffset:freePageHeadOffset+4], freeHead[:])
}
