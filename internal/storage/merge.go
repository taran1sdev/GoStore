package storage

import "fmt"

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

func (bt *BTree) mergeLeaf(sib, leaf *LeafPage, parent *InternalPage, sepIdx int, right bool) error {
	if sib.Page.ID == leaf.Page.ID {
		bt.log.Errorf("mergeLeaf: %v", ErrSamePage)
		return fmt.Errorf("mergeLeaf: %w", ErrSamePage)
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
			err = bt.writePage(page)
		}
	}

	writePage(dest.Page)
	writePage(parent.Page)

	bt.FreePage(orphan.Page.ID)
	return err
}

func (bt *BTree) mergeInternal(sib, page, parent *InternalPage, sepIdx int, right bool) error {
	if sib.Page.ID == page.Page.ID {
		bt.log.Errorf("mergeInternal: %v", ErrSamePage)
		return fmt.Errorf("mergeInternal: %w", ErrSamePage)
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
			err = bt.writePage(page)
		}
	}
	writePage(leftNode.Page)
	writePage(parent.Page)

	bt.FreePage(rightNode.Page.ID)
	return err
}
