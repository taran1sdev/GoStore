package storage

const (
	InvalidPage uint32 = 0xFFFFFFFF
	PageSize           = 4096
	maxChildren int    = 128
)

type PageType uint8

const (
	PageTypeMeta PageType = iota
	PageTypeLeaf
	PageTypeInternal
	PageTypeOverflow
	PageTypeFree
)

type Page struct {
	ID   uint32
	Type PageType
	Data []byte
}

func NewPage() *Page {
	return &Page{
		Data: make([]byte, PageSize),
	}
}
