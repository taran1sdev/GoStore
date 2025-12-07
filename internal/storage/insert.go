package storage

import (
	"bytes"
	"errors"
)

func (bt *BTree) descendForInsert(key []byte) (*LeafPage, *ParentStack, error) {
	curr := bt.root
	stack := &ParentStack{}

	for {
		page, err := bt.pager.ReadPage(curr)
		if err != nil {
			return nil, nil, err
		}

		switch page.Type {
		case PageTypeLeaf:
			return WrapLeafPage(page), stack, nil

		case PageTypeInternal:
			internal := WrapInternalPage(page)
			idx := internal.FindInsertIndex(key)

			stack.Push(Parent{pageID: curr})

			if idx < internal.GetNumKeys() {
				curr = internal.GetChild(idx)
			} else {
				curr = internal.GetRightChild()
			}
		}
	}
}

func (bt *BTree) insertIntoLeaf(leaf *LeafPage, key, val []byte) (bool, []byte, uint32, error) {
	// First try and insert the key, val into the leafpage
	if err := leaf.Insert(key, val); err == nil {
		return true, nil, 0, bt.pager.WritePage(leaf.Page)
	} else if errors.Is(err, ErrKeyExists) {
		return false, nil, 0, err
	}

	// If we get any other error it means the page is full and we have to split
	sepKey, rightPageID := bt.splitLeaf(leaf)

	// Now decide which leaf to insert the value into after the split
	var err error
	if bytes.Compare(key, sepKey) <= 0 {
		// There is always space after a split
		_ = leaf.Insert(key, val)
		err = bt.pager.WritePage(leaf.Page)
	} else {
		right, _ := bt.pager.ReadPage(rightPageID)
		rleaf := WrapLeafPage(right)
		_ = rleaf.Insert(key, val)
		err = bt.pager.WritePage(rleaf.Page)
	}

	return false, sepKey, rightPageID, err
}

func (bt *BTree) propogateSplit(
	parentStack *ParentStack,
	sepKey []byte,
	leftID,
	rightID uint32,
) (bool, error) {

	for {
		// Try pop our previously visited node
		parent, ok := parentStack.Pop()

		// if no parent then we need to create a new root node
		if !ok {
			return bt.growRoot(sepKey, leftID, rightID)
		}

		page, err := bt.pager.ReadPage(parent.pageID)
		if err != nil {
			return false, err
		}

		internal := WrapInternalPage(page)

		// Try insert the new separator into the parent (false means no split required)
		if !internal.InsertSeparator(sepKey, rightID) {
			return true, bt.pager.WritePage(internal.Page)
		}

		// If we need an internal split - keep track of the original separator to insert
		origKey, origChild := sepKey, rightID

		sepKey, rightID = bt.splitInternal(internal)

		leftNode := internal
		rightPage, _ := bt.pager.ReadPage(rightID)
		rightNode := WrapInternalPage(rightPage)

		if bytes.Compare(origKey, sepKey) < 0 {
			_ = leftNode.InsertSeparator(origKey, origChild)
			bt.pager.WritePage(leftNode.Page)
		} else {
			_ = rightNode.InsertSeparator(origKey, origChild)
			bt.pager.WritePage(rightNode.Page)
		}

		leftID = leftNode.Page.ID
	}
}

// Entry point into insertion logic
func (bt *BTree) Insert(key, val []byte) (bool, error) {
	leaf, parentStack, err := bt.descendForInsert(key)
	if err != nil {
		return false, err
	}

	inserted, sepKey, rightPageID, err := bt.insertIntoLeaf(leaf, key, val)
	if err != nil {
		return false, err
	}

	if inserted {
		return true, nil
	}

	return bt.propogateSplit(parentStack, sepKey, leaf.Page.ID, rightPageID)
}
