package storage

import "fmt"

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
