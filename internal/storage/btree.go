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
	curr := bt.root
	for {
		page, err := bt.pager.ReadPage(curr)
		if err != nil {
			return nil, false, err
		}

		if page.Type == PageTypeLeaf {
			leafPage := WrapLeafPage(page)
			idx := leafPage.FindInsertIndex(key)
			if idx >= leafPage.GetNumCells() {
				return nil, false, nil
			}

			ptr := leafPage.GetCellPointer(idx)

			if bytes.Equal(leafPage.ReadKey(ptr), key) {
				_, val := leafPage.ReadRecord(ptr)
				return val, true, nil
			} else {
				return nil, false, nil
			}
		}

		if page.Type == PageTypeInternal {
			internalPage := WrapInternalPage(page)
			idx := internalPage.FindInsertIndex(key)
			if idx < internalPage.GetNumKeys() {
				curr = internalPage.GetChild(idx)
			} else {
				curr = internalPage.GetRightChild()
			}

		}
	}
}
