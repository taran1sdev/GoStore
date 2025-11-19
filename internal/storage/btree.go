package storage

import (
	"bytes"
)

// B+Tree - each page is a node

type BTree struct {
	pager *Pager
	root  uint32
}

func (bt *BTree) getPage(pid uint32) (*Page, error) {
	if page, err := bt.pager.ReadPage(pid); err != nil {
		return nil, err
	}
	return page, nil
}

func (bt *BTree) Search(key []byte) ([]byte, bool, error) {
	curr := bt.root
	for {
		page, err := pager.ReadPage(curr)
		if err != nil {
			return nil, false, err
		}

		if page.Type == PageTypeLeaf {
			leafPage := NewLeafPage(page)
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
			internalPage := NewInternalPage(page)
			idx := internalPage.FindInsertIndex(key)
			if idx == internalPage.GetNumKeys() {
				curr = internalPage.GetRightChild()
			} else {
				curr = internalPage.GetChild(idx)
			}

		}
	}
}
