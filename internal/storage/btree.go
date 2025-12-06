package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// B+Tree - each page is a node

const InvalidPage uint32 = 0xFFFFFFFF

type BTree struct {
	pager     *Pager
	root      uint32
	meta      *MetaPage
	metaDirty bool
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

// This type allows us to keep track our last parent when traversing the tree
type Parent struct {
	pageID uint32
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

func (bt *BTree) findChildIndex(parent *InternalPage, childID uint32) int {
	n := parent.GetNumKeys()

	if parent.GetRightChild() == childID {
		return n
	}

	for i := 0; i < n; i++ {
		if parent.GetChild(i) == childID {
			return i
		}
	}

	return -1
}

func (bt *BTree) splitLeaf(left *LeafPage) ([]byte, uint32) {
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
		off, err := left.WriteRecord(recs[i].key, recs[i].val)
		if err != nil {
			panic(err)
		}
		left.SetCellPointer(i, off)
		left.SetNumCells(left.GetNumCells() + 1)
	}

	rightIdx := 0
	for i := mid; i < numCells; i++ {
		off, err := right.WriteRecord(recs[i].key, recs[i].val)
		if err != nil {
			panic(err)
		}
		right.SetCellPointer(rightIdx, off)
		right.SetNumCells(right.GetNumCells() + 1)
		rightIdx++
	}

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

	// Initially we set the right child to the first key
	// value as insert separator will shift keys
	left.SetRightChild(children[0])

	for i := 0; i < mid; i++ {
		if left.InsertSeparator(keys[i], children[i+1]) {
			panic("left page split during splitInternal..")
		}
	}

	right.SetNumKeys(0)
	right.SetFreeStart(keyPointerOffset)
	right.SetFreeEnd(PageSize)

	right.SetRightChild(children[mid+1])

	for i := mid + 1; i < numKeys; i++ {
		if right.InsertSeparator(keys[i], children[i+1]) {
			panic("right page split during splitInternal")
		}
	}

	sepKey := keys[mid]

	bt.pager.WritePage(left.Page)
	bt.pager.WritePage(right.Page)

	return sepKey, right.Page.ID
}

func (bt *BTree) growRoot(sepKey []byte, leftID, rightID uint32) (bool, error) {
	p := bt.pager.AllocatePage()
	root := NewInternalPage(p)

	root.SetRightChild(leftID)

	if root.InsertSeparator(sepKey, rightID) {
		panic("split during growRoot")
	}

	if err := bt.pager.WritePage(root.Page); err != nil {
		return false, err
	}

	bt.root = root.Page.ID

	bt.meta.SetRootID(bt.root)
	bt.pager.WritePage(bt.meta.Page)
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

			origKey, origChild := sepKey, rightPageID

			// No space for key means we need to split the internal page
			sepKey, rightPageID = bt.splitInternal(internalPage)

			left := internalPage
			r, err := bt.pager.ReadPage(rightPageID)
			if err != nil {
				return false, err
			}

			right := WrapInternalPage(r)

			if bytes.Compare(origKey, sepKey) < 0 {
				if !left.InsertSeparator(origKey, origChild) {
					if err := bt.pager.WritePage(left.Page); err != nil {
						return false, err
					}
				}
			} else {
				if !right.InsertSeparator(origKey, origChild) {
					if err := bt.pager.WritePage(right.Page); err != nil {
						return false, err
					}
				}
			}

			parent, ok := stack.Pop()
			if !ok {
				return bt.growRoot(sepKey, internalPage.Page.ID, rightPageID)
			}

			curr = parent.pageID
		}
	}
}

func (bt *BTree) rebalanceLeaf(leaf *LeafPage, parent *InternalPage, idx int) (bool, error) {
	var left, right *LeafPage
	var leftID, rightID uint32
	n := parent.GetNumKeys()

	if idx > 0 {
		leftID = parent.GetChild(idx - 1)
	} else {
		leftID = InvalidPage
	}

	if leftID != InvalidPage {
		lp, _ := bt.pager.ReadPage(leftID)
		left = WrapLeafPage(lp)
	}

	if idx < n-1 {
		rightID = parent.GetChild(idx + 1)
	} else if idx == n-1 {
		rightID = parent.GetRightChild()
	} else {
		rightID = InvalidPage
	}

	if rightID != InvalidPage {
		rp, _ := bt.pager.ReadPage(rightID)
		right = WrapLeafPage(rp)
	}

	if leftID != InvalidPage && bt.canBorrowLeaf(left, leaf, false) {
		err := bt.borrowLeaf(left, leaf, parent, idx-1, false)
		return false, err
	}

	if rightID != InvalidPage && bt.canBorrowLeaf(right, leaf, true) {
		err := bt.borrowLeaf(right, leaf, parent, idx, true)
		return false, err
	}

	if leftID != InvalidPage && bt.canMergeLeaf(left, leaf) {
		err := bt.mergeLeaf(left, leaf, parent, idx-1, false)
		return true, err
	}

	if rightID != InvalidPage && bt.canMergeLeaf(right, leaf) {
		err := bt.mergeLeaf(right, leaf, parent, idx, true)
		return true, err
	}

	return false, nil
}

func (bt *BTree) rebalanceInternal(page, parent *InternalPage, idx int, pageID uint32) (bool, error) {
	if pageID == bt.root {
		if page.GetNumKeys() == 0 {
			onlyChild := page.GetChild(0)
			bt.root = onlyChild

			bt.meta.SetRootID(bt.root)
			bt.FreePage(pageID)
			return false, nil
		}
		return false, nil
	}

	var left, right *InternalPage
	var leftID, rightID uint32
	n := parent.GetNumKeys()

	if idx > 0 {
		leftID = parent.GetChild(idx - 1)
	} else {
		leftID = InvalidPage
	}

	if leftID != InvalidPage {
		lp, _ := bt.pager.ReadPage(leftID)
		left = WrapInternalPage(lp)
	}

	if idx < n-1 {
		rightID = parent.GetChild(idx + 1)
	} else if idx == n-1 {
		rightID = parent.GetRightChild()
	} else {
		rightID = InvalidPage
	}

	if rightID != InvalidPage {
		rp, _ := bt.pager.ReadPage(rightID)
		right = WrapInternalPage(rp)
	}

	if leftID != InvalidPage && bt.canBorrowInternal(left, page, false) {
		err := bt.borrowInternal(left, page, parent, idx-1, false)
		return false, err
	}

	if rightID != InvalidPage && bt.canBorrowInternal(right, page, true) {
		err := bt.borrowInternal(right, page, parent, idx, true)
		return false, err
	}

	if leftID != InvalidPage && bt.canMergeInternal(left, page) {
		err := bt.mergeInternal(left, page, parent, idx-1, false)
		return true, err
	}

	if rightID != InvalidPage && bt.canMergeInternal(right, page) {
		err := bt.mergeInternal(right, page, parent, idx, true)
		return true, err
	}

	return false, nil
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
	sibKeys := sib.GetNumKeys()
	pageKeys := page.GetNumKeys()

	if sibKeys <= 1 {
		return false
	}

	if pageKeys+1 > maxChildren-1 {
		return false
	}

	var ptr uint16

	if right {
		ptr = sib.GetKeyPointer(0)
	} else {
		ptr = sib.GetKeyPointer(sib.GetNumKeys() - 1)
	}

	key := sib.ReadKey(ptr)
	borrowSize := 4 + len(key)

	newUsed := sib.GetSpaceUsed() - borrowSize
	return newUsed >= PageSize/2
}

func (bt *BTree) canMergeLeaf(sib, leaf *LeafPage) bool {
	return sib.GetSpaceUsed()+leaf.GetSpaceUsed() < PageSize
}

func (bt *BTree) canMergeInternal(sib, page *InternalPage) bool {
	sibKeys := sib.GetNumKeys()
	pageKeys := page.GetNumKeys()

	if sibKeys+pageKeys > maxChildren-1 {
		return false
	}

	if sib.GetSpaceUsed()+page.GetSpaceUsed() > PageSize {
		return false
	}

	return true
}

func (bt *BTree) borrowLeaf(sib, leaf *LeafPage, parent *InternalPage, sepIdx int, right bool) error {
	if sib.Page.ID == leaf.Page.ID {
		panic(fmt.Sprintf("borrowLeaf: sibling and leaf are the same page (%d)", sib.Page.ID))
	}
	var key, val []byte
	var ptr uint16

	if right {
		ptr = sib.GetCellPointer(0)
		k, v := sib.ReadRecord(ptr)

		key = append([]byte(nil), k...)
		val = append([]byte(nil), v...)
		sib.Delete(key)
	} else {
		idx := sib.GetNumCells() - 1
		ptr = sib.GetCellPointer(idx)
		k, v := sib.ReadRecord(ptr)

		key = append([]byte(nil), k...)
		val = append([]byte(nil), v...)
		sib.Delete(key)
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
			return fmt.Errorf("borrowLeaf: left sibling empty")
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
	if sib.Page.ID == page.Page.ID {
		panic(fmt.Sprintf("borrowInternal: sibling and page have the same ID (%d)", sib.Page.ID))
	}

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

		if err := sib.DeleteChild(n); err != nil {
			return err
		}

		if err := sib.DeleteKey(n - 1); err != nil {
			return err
		}
	}

	oldKeyCount := page.GetNumKeys()
	parentPtr := parent.GetKeyPointer(sepIdx)
	parentKey := parent.ReadKey(parentPtr)

	if right {
		page.InsertChildPointer(oldKeyCount+1, borrowChild)
	} else {
		page.InsertChildPointer(0, borrowChild)
	}

	if err := page.InsertKey(parentKey); err != nil {
		return err
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
	if sib.Page.ID == leaf.Page.ID {
		panic(fmt.Sprintf("mergeLeaf: sibling and leaf are the same page (%d)", sib.Page.ID))
	}

	type rec struct {
		key []byte
		val []byte
	}

	var leftLeaf, rightLeaf, dest, orphan *LeafPage

	if right {
		leftLeaf = leaf
		rightLeaf = sib
		dest = leaf
		orphan = sib
	} else {
		leftLeaf = sib
		rightLeaf = leaf
		dest = sib
		orphan = leaf
	}

	lNum := leftLeaf.GetNumCells()
	rNum := rightLeaf.GetNumCells()

	records := make([]rec, 0, lNum+rNum)

	for i := 0; i < lNum; i++ {
		ptr := leftLeaf.GetCellPointer(i)
		k, v := leftLeaf.ReadRecord(ptr)

		kCopy := append([]byte(nil), k...)
		vCopy := append([]byte(nil), v...)
		records = append(records, rec{key: kCopy, val: vCopy})
	}

	for i := 0; i < rNum; i++ {
		ptr := rightLeaf.GetCellPointer(i)
		k, v := rightLeaf.ReadRecord(ptr)

		kCopy := append([]byte(nil), k...)
		vCopy := append([]byte(nil), v...)
		records = append(records, rec{key: kCopy, val: vCopy})
	}

	dest.SetNumCells(0)
	dest.SetFreeStart(dataStart)
	dest.SetFreeEnd(PageSize)

	for i := 0; i < len(records); i++ {
		off, err := dest.WriteRecord(records[i].key, records[i].val)
		if err != nil {
			return err
		}
		dest.SetCellPointer(i, off)
	}

	dest.SetNumCells(len(records))
	dest.SetFreeStart(dataStart + len(records)*2)

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

	bt.FreePage(orphan.Page.ID)
	return err
}

func (bt *BTree) mergeInternal(sib, page, parent *InternalPage, sepIdx int, right bool) error {
	if sib.Page.ID == page.Page.ID {
		panic(fmt.Sprintf("mergeInternal: sibling and leaf are the same page (%d)", sib.Page.ID))
	}

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

	for i, key := range keys {
		off, err := leftNode.WriteKey(key)
		if err != nil {
			return err
		}
		leftNode.SetKeyPointer(i, off)
	}

	leftNode.SetNumKeys(len(keys))
	leftNode.SetFreeStart(keyPointerOffset + len(keys)*2)

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

	bt.FreePage(leftNode.Page.ID)
	return err
}

func (bt *BTree) checkMeta() {
	if bt.metaDirty {
		bt.pager.WritePage(bt.meta.Page)
		bt.metaDirty = false
	}
}

func (bt *BTree) Delete(key []byte) error {
	curr := bt.root
	stack := &ParentStack{}

	propagating := false

	defer bt.checkMeta()

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
					fmt.Printf("Delete failed for key %s in leaf %d: %v\n",
						string(key), leaf.Page.ID, err)
					fmt.Printf("Leaf %d keys: %v\n", leaf.Page.ID, leaf.DebugKeys())
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

				idx := bt.findChildIndex(parent, curr)

				prop, err := bt.rebalanceLeaf(leaf, parent, idx)
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
				sepIdx := iPage.FindInsertIndex(key)

				stack.Push(Parent{
					pageID: curr,
				})

				if sepIdx < iPage.GetNumKeys() {
					curr = iPage.GetChild(sepIdx)
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

					bt.meta.SetRootID(bt.root)
					bt.FreePage(iPage.Page.ID)
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

			idx := bt.findChildIndex(parent, curr)
			prop, err := bt.rebalanceInternal(iPage, parent, idx, curr)
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

func (bt *BTree) FreePage(id uint32) {
	if id == bt.root || id == 0 {
		return
	}

	p, _ := bt.pager.ReadPage(id)

	p.Type = PageTypeFree
	p.Data = make([]byte, PageSize)

	prevHead := bt.meta.GetFreeHead()
	binary.LittleEndian.PutUint32(p.Data[1:5], prevHead)

	bt.meta.SetFreeHead(id)
	bt.metaDirty = true

	bt.pager.WritePage(p)
}
