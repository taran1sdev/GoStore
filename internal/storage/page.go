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

type Page struct {
	ID   uint32
	Type PageType
	Data []byte
}
