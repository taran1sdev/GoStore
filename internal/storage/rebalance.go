package storage

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
