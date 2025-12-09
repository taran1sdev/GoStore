package storage

import "errors"

var (
	// btree
	ErrSamePage     = errors.New("operation attempted on the same page")
	ErrCorruptTree  = errors.New("btree is corrupt")
	ErrSiblingEmpty = errors.New("sibling empty")
	ErrPageOverflow = errors.New("operation cause page overflow")
	// pager
	ErrCorruptFile       = errors.New("file is corrupt")
	ErrCorruptFreeList   = errors.New("free list is corrupt")
	ErrInvalidPointer    = errors.New("invalid page pointer")
	ErrInvalidFileSig    = errors.New("invalid file signature")
	ErrWriteSizeMismatch = errors.New("data written does not match page size")
	// pages
	ErrKeyExists = errors.New("key already exists")
	ErrPageFull  = errors.New("not enough space to write record")
	// wal
	ErrChecksumMismatch = errors.New("checksum does not match")
)
