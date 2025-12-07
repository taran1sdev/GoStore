package storage

import (
	"bytes"
)

type BTree struct {
	pager     *Pager
	root      uint32
	meta      *MetaPage
	metaDirty bool
}

// This type stores records when splitting / merging
type rec struct {
	key []byte
	val []byte
}

func NewBTree(pager *Pager) (*BTree, error) {
	m, err := pager.ReadPage(0)
	if err != nil {
		return nil, err
	}

	metaPage := WrapMetaPage(m)
	rootID := metaPage.GetRootID()
	return &BTree{
		pager:     pager,
		root:      rootID,
		meta:      metaPage,
		metaDirty: false,
	}, nil
}

func (bt *BTree) Search(key []byte) ([]byte, bool, error) {
	leaf, _, err := bt.descend(key)
	if err != nil {
		return nil, false, err
	}

	idx := leaf.FindInsertIndex(key)
	if idx >= leaf.GetNumCells() {
		return nil, false, nil
	}

	ptr := leaf.GetCellPointer(idx)

	if bytes.Equal(leaf.ReadKey(ptr), key) {
		_, val := leaf.ReadRecord(ptr)
		return val, true, nil
	} else {
		return nil, false, nil
	}
}
