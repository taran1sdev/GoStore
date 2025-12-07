package storage

import "fmt"

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
