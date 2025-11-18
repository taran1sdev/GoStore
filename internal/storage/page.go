package storage

const PageSize = 4096

type PageType uint8

const (
	PageTypeMeta PageType = iota
	PageTypeLeaf
	PageTypeInternal
	PageTypeOverflow
	PageTypeFree
)

const (
	maxChildren = 128
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
