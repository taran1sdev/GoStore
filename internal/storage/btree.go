package storage

import (
	"bytes"
	"errors"
	"fmt"
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
			if idx < internalPage.GetNumKeys() {
				curr = internalPage.GetChild(idx)
			} else {
				curr = internalPage.GetRightChild()
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
	fmt.Println("Splitting leaf page")
	p := bt.pager.AllocatePage()
	right := NewLeafPage(p)

	// First we need to find the split point
	numCells := left.GetNumCells()
	mid := numCells / 2

	type rec struct {
		key []byte
		val []byte
	}
	var recs []rec

	for i := 0; i < numCells; i++ {
		ptr := left.GetCellPointer(i)
		k, v := left.ReadRecord(ptr)
		recs = append(recs, rec{key: k, val: v})
	}

	left.SetNumCells(0)
	left.SetFreeStart(dataStart)
	left.SetFreeEnd(PageSize)

	for i := 0; i < mid; i++ {
		if err := left.Insert(recs[i].key, recs[i].val); err != nil {
			panic(err)
		}
	}

	for i := mid; i < numCells; i++ {
		if err := right.Insert(recs[i].key, recs[i].val); err != nil {
			panic(err)
		}
	}

	sepPtr := right.GetCellPointer(0)
	sepKey := right.ReadKey(sepPtr)

	bt.pager.WritePage(left.Page)
	bt.pager.WritePage(right.Page)

	return sepKey, right.Page.ID
}

func (bt *BTree) splitInternal(left *InternalPage) ([]byte, uint32) {
	fmt.Println("Splitting Internal Page")
	p := bt.pager.AllocatePage()
	right := NewInternalPage(p)

	// Find the split point
	numKeys := left.GetNumKeys()
	mid := numKeys / 2

	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		ptr := left.GetKeyPointer(i)
		keys[i] = left.ReadKey(ptr)
	}

	children := make([]uint32, numKeys+1)
	for i := 0; i < numKeys; i++ {
		children[i] = left.GetChild(i)
	}
	children[numKeys] = left.GetRightChild()

	left.SetNumKeys(0)
	left.SetFreeStart(keyPointerOffset)
	left.SetFreeEnd(PageSize)

	for i := 0; i < mid; i++ {
		off, err := left.WriteKey(keys[i])
		if err != nil {
			panic(err)
		}
		left.SetKeyPointer(i, off)
		left.SetChild(i, children[i])
		left.SetNumKeys(i + 1)
	}

	left.SetRightChild(children[mid])

	rightIdx := 0
	for i := mid; i < numKeys; i++ {
		off, err := right.WriteKey(keys[i])
		if err != nil {
			panic(err)
		}
		right.SetKeyPointer(rightIdx, off)
		right.SetChild(rightIdx, children[i])
		right.SetNumKeys(rightIdx + 1)
		rightIdx++
	}

	right.SetRightChild(children[numKeys])

	firstPtr := right.GetKeyPointer(0)
	sepKey := right.ReadKey(firstPtr)

	bt.pager.WritePage(left.Page)
	bt.pager.WritePage(right.Page)

	return sepKey, right.Page.ID
}

func (bt *BTree) growRoot(sepKey []byte, leftID, rightID uint32) (bool, error) {
	p := bt.pager.AllocatePage()
	root := NewInternalPage(p)

	off, err := root.WriteKey(sepKey)
	if err != nil {
		return false, err
	}

	root.InsertKeyPointer(0, off)

	root.SetChild(0, leftID)
	root.SetRightChild(rightID)

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
				} else if errors.Is(fErr, ErrKeyExists) {
					return false, fErr
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
				if idx < internalPage.GetNumKeys() {
					curr = internalPage.GetChild(idx)
				} else {
					curr = internalPage.GetRightChild()
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

func (bt *BTree) rebalanceLeaf(leaf *LeafPage, parent *InternalPage, idx int) (bool, error) {
	fmt.Println("Rebalancing leaf")
	var left, right *LeafPage
	var leftID, rightID uint32
	n := parent.GetNumKeys()

	if idx > 0 {
		leftID = parent.GetChild(idx - 1)
	} else {
		leftID = 0
	}

	if leftID != 0 {
		lp, _ := bt.pager.ReadPage(leftID)
		left = WrapLeafPage(lp)
	}

	if idx < n-1 {
		rightID = parent.GetChild(idx + 1)
	} else if idx == n-1 {
		rightID = parent.GetRightChild()
	} else {
		rightID = 0
	}

	if rightID != 0 {
		rp, _ := bt.pager.ReadPage(rightID)
		right = WrapLeafPage(rp)
	}

	if left != nil && bt.canBorrowLeaf(left, leaf, false) {
		err := bt.borrowLeaf(left, leaf, parent, idx-1, false)
		return false, err
	}

	if right != nil && bt.canBorrowLeaf(right, leaf, true) {
		err := bt.borrowLeaf(right, leaf, parent, idx, true)
		return false, err
	}

	if left != nil && bt.canMergeLeaf(left, leaf) {
		err := bt.mergeLeaf(left, leaf, parent, idx-1, false)
		return true, err
	}

	if right != nil && bt.canMergeLeaf(right, leaf) {
		err := bt.mergeLeaf(right, leaf, parent, idx, true)
		return true, err
	}

	return false, fmt.Errorf("rebalanceLeaf: no valid sibling found to borrow or merge")
}

func (bt *BTree) rebalanceInternal(page, parent *InternalPage, idx int, pageID uint32) (bool, error) {
	fmt.Println("Rebalancing internal")
	if pageID == bt.root {
		if page.GetNumKeys() == 0 {
			onlyChild := page.GetChild(0)
			bt.root = onlyChild

			m, _ := bt.pager.ReadPage(0)
			mp := WrapMetaPage(m)
			mp.SetRootID(bt.root)
			bt.pager.WritePage(mp.Page)
			return false, nil
		}
		return false, nil
	}

	var left, right *InternalPage
	var leftID, rightID uint32
	n := parent.GetNumKeys()

	if idx > 0 {
		leftID = parent.GetChild(idx - 1)
	}

	if leftID != 0 {
		lp, _ := bt.pager.ReadPage(leftID)
		left = WrapInternalPage(lp)
	}

	if idx < n-1 {
		rightID = parent.GetChild(idx + 1)
	} else if idx == n-1 {
		rightID = parent.GetRightChild()
	} else {
		rightID = 0
	}

	if rightID != 0 {
		rp, _ := bt.pager.ReadPage(rightID)
		right = WrapInternalPage(rp)
	}

	if left != nil && bt.canBorrowInternal(left, page, false) {
		err := bt.borrowInternal(left, page, parent, idx-1, false)
		return false, err
	}

	if right != nil && bt.canBorrowInternal(right, page, true) {
		err := bt.borrowInternal(right, page, parent, idx, true)
		return false, err
	}

	if left != nil && bt.canMergeInternal(left, page) {
		err := bt.mergeInternal(left, page, parent, idx-1, false)
		return true, err
	}

	if right != nil && bt.canMergeInternal(right, page) {
		err := bt.mergeInternal(right, page, parent, idx, true)
		return true, err
	}

	return false, fmt.Errorf("rebalanceInternal: no valid sibling found to borrow or merge")
}

func (bt *BTree) canBorrowLeaf(sib, leaf *LeafPage, right bool) bool {
	if sib.GetNumCells() == 0 {
		return false
	}

	var borrowSize int
	if right {
		borrowSize = sib.GetFirstRecordSize()
	} else {
		borrowSize = sib.GetLastRecordSize()
	}

	newUsed := sib.GetSpaceUsed() - borrowSize
	return newUsed >= PageSize/2
}

func (bt *BTree) canBorrowInternal(sib, page *InternalPage, right bool) bool {
	if sib.GetNumKeys() == 0 {
		return false
	}

	var ptr uint16

	if right {
		ptr = sib.GetKeyPointer(0)
	} else {
		ptr = sib.GetKeyPointer(sib.GetNumKeys() - 1)
	}

	key := sib.ReadKey(ptr)
	borrowSize := 6 + len(key)

	newUsed := sib.GetSpaceUsed() - borrowSize
	return newUsed >= PageSize/2
}

func (bt *BTree) canMergeLeaf(sib, leaf *LeafPage) bool {
	return sib.GetSpaceUsed()+leaf.GetSpaceUsed() < PageSize
}

func (bt *BTree) canMergeInternal(sib, page *InternalPage) bool {
	return sib.GetSpaceUsed()+page.GetSpaceUsed() < PageSize
}

func (bt *BTree) borrowLeaf(sib, leaf *LeafPage, parent *InternalPage, sepIdx int, right bool) error {
	var msg string

	if right {
		msg = "right sibling"
	} else {
		msg = "left sibling"
	}

	fmt.Printf("Leaf: Borrowing %s\n", msg)

	var key, val []byte
	var ptr uint16

	if right {
		ptr = sib.GetCellPointer(0)
		key, val = sib.ReadRecord(ptr)

		sib.DeleteCellPointer(0)
		if err := sib.Compact(); err != nil {
			return err
		}
	} else {
		idx := sib.GetNumCells() - 1
		ptr = sib.GetCellPointer(sib.GetNumCells() - 1)
		key, val = sib.ReadRecord(ptr)

		sib.DeleteCellPointer(idx)
		if err := sib.Compact(); err != nil {
			return err
		}
	}

	if err := leaf.Insert(key, val); err != nil {
		return err
	}
	if err := leaf.Compact(); err != nil {
		return err
	}

	var newMin []byte

	if right {
		if sib.GetNumCells() == 0 {
			return fmt.Errorf("borrowLeaf: right sibling empty")
		}
		off := sib.GetCellPointer(0)
		newMin = sib.ReadKey(off)
	} else {
		if leaf.GetNumCells() == 0 {
			return fmt.Errorf("borrowLeaf: leaf empty")
		}
		off := leaf.GetCellPointer(0)
		newMin = leaf.ReadKey(off)
	}

	if err := parent.ReplaceKey(sepIdx, newMin); err != nil {
		return err
	}

	// Closure here makes these calls a bit cleaner
	var err error
	writePage := func(page *Page) {
		if err == nil {
			err = bt.pager.WritePage(page)
		}
	}

	writePage(sib.Page)
	writePage(leaf.Page)
	writePage(parent.Page)
	return err
}

func (bt *BTree) borrowInternal(sib, page, parent *InternalPage, sepIdx int, right bool) error {
	var msg string

	if right {
		msg = "right sibling"
	} else {
		msg = "left sibling"
	}

	fmt.Printf("Internal: Borrowing %s\n", msg)

	var borrowKey []byte
	var borrowChild uint32
	var ptr uint16

	if right {
		if sib.GetNumKeys() == 0 {
			return fmt.Errorf("borrowInternal: right sibling empty")
		}

		ptr = sib.GetKeyPointer(0)
		borrowKey = sib.ReadKey(ptr)

		borrowChild = sib.GetChild(0)

		if err := sib.DeleteChild(0); err != nil {
			return err
		}

		if err := sib.DeleteKey(0); err != nil {
			return err
		}
	} else {
		n := sib.GetNumKeys()
		if n == 0 {
			return fmt.Errorf("borrowInternal: left sibling empty")
		}

		ptr = sib.GetKeyPointer(n - 1)
		borrowKey = sib.ReadKey(ptr)

		borrowChild = sib.GetRightChild()

		if err := sib.DeleteKey(n - 1); err != nil {
			return err
		}

		if err := sib.DeleteChild(n - 1); err != nil {
			return err
		}
	}

	parentPtr := parent.GetKeyPointer(sepIdx)
	parentKey := parent.ReadKey(parentPtr)

	if err := page.InsertKey(parentKey); err != nil {
		return err
	}

	newKeyCount := page.GetNumKeys()

	if right {
		page.InsertChildPointer(newKeyCount, borrowChild)
	} else {
		page.InsertChildPointer(0, borrowChild)
	}

	if err := parent.ReplaceKey(sepIdx, borrowKey); err != nil {
		return err
	}

	var err error
	writePage := func(page *Page) {
		if err == nil {
			err = bt.pager.WritePage(page)
		}
	}

	writePage(sib.Page)
	writePage(page.Page)
	writePage(parent.Page)

	return err
}

func (bt *BTree) mergeLeaf(sib, leaf *LeafPage, parent *InternalPage, sepIdx int, right bool) error {
	var msg string

	if right {
		msg = "right sibling"
	} else {
		msg = "left sibling"
	}
	fmt.Printf("Leaf: Merging %s\n", msg)

	type rec struct {
		key []byte
		val []byte
	}

	var dest *LeafPage
	var records []rec

	var lNum, sNum int = 0, 0
	var ptr uint16

	if right {
		dest = leaf
		lNum = leaf.GetNumCells()
		sNum = sib.GetNumCells()
		records = make([]rec, 0, lNum+sNum)

		for i := 0; i < lNum; i++ {
			ptr = leaf.GetCellPointer(i)
			k, v := leaf.ReadRecord(ptr)
			records = append(records, rec{key: k, val: v})
		}

		for i := 0; i < sNum; i++ {
			ptr = sib.GetCellPointer(i)
			k, v := sib.ReadRecord(ptr)
			records = append(records, rec{key: k, val: v})
		}
	} else {
		dest = sib
		lNum = leaf.GetNumCells()
		sNum = sib.GetNumCells()
		records = make([]rec, 0, lNum+sNum)

		for i := 0; i < sNum; i++ {
			ptr = sib.GetCellPointer(i)
			k, v := sib.ReadRecord(ptr)
			records = append(records, rec{key: k, val: v})
		}

		for i := 0; i < lNum; i++ {
			ptr = leaf.GetCellPointer(i)
			k, v := leaf.ReadRecord(ptr)
			records = append(records, rec{key: k, val: v})
		}
	}

	nTotal := len(records)
	dest.SetNumCells(0)
	dest.SetFreeStart(dataStart)
	dest.SetFreeEnd(PageSize)

	for i := 0; i < nTotal; i++ {
		if err := dest.Insert(records[i].key, records[i].val); err != nil {
			return err
		}
	}

	if err := parent.DeleteChild(sepIdx + 1); err != nil {
		return err
	}

	if err := parent.DeleteKey(sepIdx); err != nil {
		return err
	}

	var err error
	writePage := func(page *Page) {
		if err == nil {
			err = bt.pager.WritePage(page)
		}
	}

	writePage(dest.Page)
	writePage(parent.Page)

	return err
}

func (bt *BTree) mergeInternal(sib, page, parent *InternalPage, sepIdx int, right bool) error {
	var msg string

	if right {
		msg = "right sibling"
	} else {
		msg = "left sibling"
	}

	fmt.Printf("Internal: Merging %s\n", msg)
	var leftNode, rightNode *InternalPage

	if right {
		leftNode = page
		rightNode = sib
	} else {
		leftNode = sib
		rightNode = page
	}

	lNum := leftNode.GetNumKeys()
	rNum := rightNode.GetNumKeys()

	sepPtr := parent.GetKeyPointer(sepIdx)
	sepKey := parent.ReadKey(sepPtr)

	keys := make([][]byte, 0, lNum+1+rNum)
	children := make([]uint32, 0, lNum+rNum+2)

	for i := 0; i < lNum; i++ {
		children = append(children, leftNode.GetChild(i))
		ptr := leftNode.GetKeyPointer(i)
		keys = append(keys, leftNode.ReadKey(ptr))
	}
	children = append(children, leftNode.GetRightChild())

	keys = append(keys, sepKey)

	for i := 0; i < rNum; i++ {
		children = append(children, rightNode.GetChild(i))
		ptr := rightNode.GetKeyPointer(i)
		keys = append(keys, rightNode.ReadKey(ptr))
	}
	children = append(children, rightNode.GetRightChild())

	leftNode.SetNumKeys(0)
	leftNode.SetFreeStart(keyPointerOffset)
	leftNode.SetFreeEnd(PageSize)

	for _, k := range keys {
		leftNode.InsertKey(k)
	}

	for i := 0; i < len(children)-1; i++ {
		leftNode.SetChild(i, children[i])
	}
	leftNode.SetRightChild(children[len(children)-1])

	if err := parent.DeleteChild(sepIdx + 1); err != nil {
		return err
	}

	if err := parent.DeleteKey(sepIdx); err != nil {
		return err
	}

	var err error
	writePage := func(page *Page) {
		if err == nil {
			err = bt.pager.WritePage(page)
		}
	}

	writePage(leftNode.Page)
	writePage(parent.Page)

	return err
}

func (bt *BTree) Delete(key []byte) error {
	curr := bt.root
	stack := &ParentStack{}

	propagating := false

	for {
		page, err := bt.pager.ReadPage(curr)
		if err != nil {
			return err
		}

		if !propagating {
			switch page.Type {
			case PageTypeLeaf:
				leaf := WrapLeafPage(page)

				if err := leaf.Delete(key); err != nil {
					return err
				}

				if leaf.GetSpaceUsed() >= PageSize/2 || curr == bt.root {
					return bt.pager.WritePage(leaf.Page)
				}

				parentF, ok := stack.Pop()
				if !ok {
					return bt.pager.WritePage(leaf.Page)
				}

				parentP, err := bt.pager.ReadPage(parentF.pageID)
				if err != nil {
					return err
				}

				parent := WrapInternalPage(parentP)

				prop, err := bt.rebalanceLeaf(leaf, parent, parentF.index)
				if err != nil {
					return err
				}
				if !prop {
					return nil
				}

				curr = parent.Page.ID
				propagating = true

			case PageTypeInternal:
				iPage := WrapInternalPage(page)
				idx := iPage.FindInsertIndex(key)

				stack.Push(Parent{
					pageID: curr,
					index:  idx,
				})

				if idx < iPage.GetNumKeys() {
					curr = iPage.GetChild(idx)
				} else {
					curr = iPage.GetRightChild()
				}
			}
		} else {
			iPage := WrapInternalPage(page)

			if curr == bt.root {
				if iPage.GetNumKeys() == 0 {
					childID := iPage.GetChild(0)
					bt.root = childID

					m, err := bt.pager.ReadPage(0)
					if err != nil {
						return err
					}

					meta := WrapMetaPage(m)
					meta.SetRootID(bt.root)
					if err := bt.pager.WritePage(meta.Page); err != nil {
						return err
					}
				}

				return nil
			}

			parentF, ok := stack.Pop()
			if !ok {
				return bt.pager.WritePage(iPage.Page)
			}

			parentP, err := bt.pager.ReadPage(parentF.pageID)
			if err != nil {
				return err
			}

			parent := WrapInternalPage(parentP)

			if iPage.GetSpaceUsed() >= PageSize/2 {
				return bt.pager.WritePage(iPage.Page)
			}

			prop, err := bt.rebalanceInternal(iPage, parent, parentF.index, curr)
			if err != nil {
				return err
			}

			if !prop {
				return nil
			}

			curr = parent.Page.ID
		}
	}
}
