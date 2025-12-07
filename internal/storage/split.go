package storage

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
