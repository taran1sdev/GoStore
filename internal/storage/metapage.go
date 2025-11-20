package storage

import "encoding/binary"

type MetaPage struct {
	Page *Page
}

var sig = []byte{'G', 'o', 'S', 't', 'o', 'r', 'e', '2', '5'}

const (
	sizeOffset int = 8
	rootOffset int = 12
)

func NewMetaPage(page *Page) *MetaPage {
	var pSize [2]byte
	binary.LittleEndian.PutUint16(pSize[:], uint16(PageSize))

	var rootId [4]byte
	binary.LittleEndian.PutUint32(rootId[:], uint32(1))

	copy(page.Data[0:], sig)
	copy(page.Data[sizeOffset:], pSize[:])
	copy(page.Data[rootOffset:], rootId[:])

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

func (metaPage *MetaPage) GetRootID() uint32 {
	return binary.LittleEndian.Uint32(metaPage.Page.Data[rootOffset : rootOffset+4])
}

func (metaPage *MetaPage) SetRootID(id uint32) {
	var rootId [4]byte
	binary.LittleEndian.PutUint32(rootId[:], id)

	copy(metaPage.Page.Data[rootOffset:rootOffset+4], rootId[:])
}
