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
		page, err := bt.pager.ReadPage(curr)
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

// This type allows us to keep track our last parent when traversing the tree
type Parent struct {
	pageID uint32
	index  int
}

type ParentStack struct {
	items []Parent
}

func (s *ParentStack) IsEmpty() bool {
	return len(s.items) == 0
}

func (s *ParentStack) Push(p Parent) {
	s.items = append(s.items, p)
}

func (s *Stack) Pop() (ParentInfo, bool) {
	if !s.IsEmpty() {
		parent := s.items[:len(s.items)-1]
		s.items = s.items[:len(s.items)-1]
		return parent, true
	}
	return Parent{}, false
}

func (bt *BTree) splitLeaf(left *LeafPage, key []byte, val []byte) ([]byte, uint32) {
	p := bt.Pager.AllocatePage()
	right := NewLeafPage(p)

	// First we need to find the split point
	numCells := left.GetNumCells()
	mid := numCells / 2

	for i := mid; i < numCells; i++ {
		ptr := left.GetCellPointer(i)
		key, val := left.ReadRecord(ptr)
		// Shouldn't need to handle this error with empty page
		_ := right.Insert(key, val)
	}

	left.SetNumCells(mid)
	left.SetFreeStart(dataStart + mid*2)
	lastPtr := left.GetCellPointer(mid - 1)
	left.SetFreeEnd(int(lastPtr))

	sepPtr := right.GetCellPointer(0)
	sepKey := right.ReadKey(sepPtr)
	return sepKey, right.ID
}

func (bt *BTree) Insert(key, val []byte) error {
	curr := bt.root
	stack := &ParentStack{}
	split := false

	for {
		page, err := bt.pager.ReadPage(curr)
		if err != nil {
			return err
		}

		if page.Type == PageTypeLeaf {
			leafPage := NewLeafPage(page)
			if fErr := leafPage.Insert(key, val); fErr == nil {
				return nil
			}
			split = true
			sepKey, rightPageID := bt.splitLeaf(leafPage, key, val)
		}

		// We are propogating a split
		if page.Type == PageTypeInternal && split {

		}

		// We are traversing the tree
		if page.Type == PageTypeInternal && !split {
			internalPage := NewInternalPage(page)
			idx := internalPage.FindInsertIndex(key)
			stack.Push(&ParentInfo{
				pageID: curr,
				index:  idx,
			})
			if idx == internalPage.GetNumKeys() {
				curr = internalPage.GetRightChild()
			} else {
				curr = internalPage.GetChild(idx)
			}
		}
	}
}
