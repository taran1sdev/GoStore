package storage

import "fmt"

func (bt *BTree) splitLeaf(left *LeafPage) ([]byte, uint32) {
	p := bt.pager.AllocatePage()
	right := NewLeafPage(p)

	// First we need to find the split point
	numCells := left.GetNumCells()
	mid := numCells / 2

	var recs []rec

	for i := 0; i < numCells; i++ {
		ptr := left.GetCellPointer(i)
		k, v := left.ReadRecord(ptr)

		// Deep copy to ensure our data is consistent
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

	bt.writePage(left.Page)
	bt.writePage(right.Page)

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
		k := left.ReadKey(ptr)

		keys[i] = append([]byte(nil), k...)
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
			bt.log.Errorf("splitInternal: unexpected left page split")
			panic(fmt.Errorf("splitInternal: %w", ErrPageOverflow))
		}
	}

	right.SetNumKeys(0)
	right.SetFreeStart(keyPointerOffset)
	right.SetFreeEnd(PageSize)

	right.SetRightChild(children[mid+1])
	for i := mid + 1; i < numKeys; i++ {
		if right.InsertSeparator(keys[i], children[i+1]) {
			bt.log.Errorf("splitInternal: unexpected right page split")
			panic(fmt.Errorf("splitInternal: %w", ErrPageOverflow))
		}
	}

	sepKey := keys[mid]

	bt.writePage(left.Page)
	bt.writePage(right.Page)

	return sepKey, right.Page.ID
}

func (bt *BTree) growRoot(sepKey []byte, leftID, rightID uint32) (bool, error) {
	p := bt.pager.AllocatePage()
	root := NewInternalPage(p)

	root.SetRightChild(leftID)

	if root.InsertSeparator(sepKey, rightID) {
		bt.log.Errorf("growRoot: unexpected split during growRoot")
		return false, fmt.Errorf("growRoot: %w", ErrPageOverflow)
	}

	if err := bt.writePage(root.Page); err != nil {
		return false, err
	}

	bt.root = root.Page.ID

	bt.meta.SetRootID(bt.root)
	bt.writePage(bt.meta.Page)
	return true, nil
}
