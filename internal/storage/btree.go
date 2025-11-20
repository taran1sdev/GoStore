package storage

import (
	"bytes"
)

// B+Tree - each page is a node

type BTree struct {
	pager *Pager
	root  uint32
}

func NewBTree(pager *Pager) (*BTree, error) {
	m, err := pager.ReadPage(0)
	if err != nil {
		return nil, err
	}

	metaPage := WrapMetaPage(m)
	rootID := metaPage.GetRootID()
	return &BTree{
		pager: pager,
		root:  rootID,
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

func (s *ParentStack) Pop() (Parent, bool) {
	if !s.IsEmpty() {
		parent := s.items[len(s.items)-1]
		s.items = s.items[:len(s.items)-1]
		return parent, true
	}
	return Parent{}, false
}

func (bt *BTree) splitLeaf(left *LeafPage) ([]byte, uint32) {
	p := bt.pager.AllocatePage()
	right := NewLeafPage(p)

	// First we need to find the split point
	numCells := left.GetNumCells()
	mid := numCells / 2

	for i := mid; i < numCells; i++ {
		ptr := left.GetCellPointer(i)
		lKey, lVal := left.ReadRecord(ptr)
		// Shouldn't need to handle this error with empty page
		_ = right.Insert(lKey, lVal)
	}

	left.SetNumCells(mid)
	left.SetFreeStart(dataStart + mid*2)
	lastPtr := left.GetCellPointer(mid - 1)
	left.SetFreeEnd(int(lastPtr))

	sepPtr := right.GetCellPointer(0)
	sepKey := right.ReadKey(sepPtr)

	bt.pager.WritePage(left.Page)
	bt.pager.WritePage(right.Page)

	return sepKey, right.Page.ID
}

func (bt *BTree) splitInternal(left *InternalPage) ([]byte, uint32) {
	p := bt.pager.AllocatePage()
	right := NewInternalPage(p)

	// Find the split point
	numKeys := left.GetNumKeys()
	mid := numKeys / 2

	sepPtr := left.GetKeyPointer(mid)
	sepKey := left.ReadKey(sepPtr)

	rightChild := left.GetChild(mid)
	oldRightChild := left.GetRightChild()

	right.SetChild(0, left.GetChild(mid+1))

	pos := 0
	for i := mid + 1; i < numKeys; i++ {
		ptr := left.GetKeyPointer(i)
		lKey := left.ReadKey(ptr)

		off, _ := right.WriteKey(lKey)
		right.InsertKeyPointer(pos, off)

		pos++

		right.SetChild(pos, left.GetChild(i+1))
	}
	right.SetRightChild(oldRightChild)

	left.SetNumKeys(mid)
	left.SetFreeStart(keyPointerOffset + (mid * 2))
	left.SetRightChild(rightChild)

	// Unlikely edge case if node only has one key
	// But we should still check to avoid a panic
	if mid > 0 {
		lastPtr := left.GetKeyPointer(mid - 1)
		left.SetFreeEnd(int(lastPtr))
	} else {
		left.SetFreeEnd(PageSize)
	}

	bt.pager.WritePage(left.Page)
	bt.pager.WritePage(right.Page)

	return sepKey, right.Page.ID
}

func (bt *BTree) growRoot(sepKey []byte, leftID, rightID uint32) (bool, error) {
	p := bt.pager.AllocatePage()
	root := NewInternalPage(p)

	root.SetChild(0, leftID)
	root.SetRightChild(rightID)

	off, err := root.WriteKey(sepKey)
	if err != nil {
		return false, err
	}

	root.InsertKeyPointer(0, off)

	if err := bt.pager.WritePage(root.Page); err != nil {
		return false, err
	}

	bt.root = root.Page.ID

	m, _ := bt.pager.ReadPage(0)
	metaPage := WrapMetaPage(m)
	metaPage.SetRootID(bt.root)
	bt.pager.WritePage(metaPage.Page)
	return true, nil
}

func (bt *BTree) Insert(key, val []byte) (bool, error) {
	curr := bt.root
	stack := &ParentStack{}

	var sepKey []byte
	var rightPageID uint32
	propagating := false

	for {
		// Read the current page
		page, err := bt.pager.ReadPage(curr)
		if err != nil {
			return false, err
		}

		if !propagating {
			// Once we hit a leaf node try and insert the record
			if page.Type == PageTypeLeaf {
				leafPage := WrapLeafPage(page)
				if fErr := leafPage.Insert(key, val); fErr == nil {
					return true, bt.pager.WritePage(leafPage.Page)
				}
				// If the page is full trigger a split
				sepKey, rightPageID = bt.splitLeaf(leafPage)

				// Compare the key to be inserted with the seperator key
				// and insert into the correct leaf page
				if bytes.Compare(key, sepKey) <= 0 {
					_ = leafPage.Insert(key, val)
					bt.pager.WritePage(leafPage.Page)
				} else {
					right, _ := bt.pager.ReadPage(rightPageID)
					rightLeaf := WrapLeafPage(right)
					_ = rightLeaf.Insert(key, val)
					bt.pager.WritePage(rightLeaf.Page)
				}

				// No parent means we need to create a new root node
				parent, ok := stack.Pop()
				if !ok {
					return bt.growRoot(sepKey, leafPage.Page.ID, rightPageID)
				}

				// Start propagating up the tree
				curr = parent.pageID
				propagating = true
				continue

				// Traverse the internal pages
			} else if page.Type == PageTypeInternal {
				internalPage := WrapInternalPage(page)
				idx := internalPage.FindInsertIndex(key)
				stack.Push(Parent{
					pageID: curr,
					index:  idx,
				})
				if idx == internalPage.GetNumKeys() {
					curr = internalPage.GetRightChild()
				} else {
					curr = internalPage.GetChild(idx)
				}
			}

			// Propagation after a split
		} else {
			internalPage := WrapInternalPage(page)
			// We successfully updated internal pages so we can return
			if !internalPage.InsertSeparator(sepKey, rightPageID) {
				return true, bt.pager.WritePage(internalPage.Page)
			}

			// No space for key means we need to split the internal page
			sepKey, rightPageID = bt.splitInternal(internalPage)

			parent, ok := stack.Pop()
			if !ok {
				return bt.growRoot(sepKey, internalPage.Page.ID, rightPageID)
			}

			curr = parent.pageID
		}
	}
}
