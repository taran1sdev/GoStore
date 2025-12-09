package storage

import (
	"bytes"
	"errors"
)

func (bt *BTree) insertIntoLeaf(leaf *LeafPage, key, val []byte) (bool, []byte, uint32, error) {
	// First try and insert the key, val into the leafpage
	if err := leaf.Insert(key, val); err == nil {
		return true, nil, 0, bt.writePage(leaf.Page)
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
		err = bt.writePage(leaf.Page)
	} else {
		right, _ := bt.pager.ReadPage(rightPageID)
		rleaf := WrapLeafPage(right)
		_ = rleaf.Insert(key, val)
		err = bt.writePage(rleaf.Page)
	}

	return false, sepKey, rightPageID, err
}

func (bt *BTree) propogateSplit(
	stack *ParentStack,
	sepKey []byte,
	leftID,
	rightID uint32,
) (bool, error) {

	for {
		// Try pop our previously visited node
		parent, ok := stack.Pop()

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
			return true, bt.writePage(internal.Page)
		}

		// If we need an internal split - keep track of the original separator to insert
		origKey, origChild := sepKey, rightID

		sepKey, rightID = bt.splitInternal(internal)

		leftNode := internal
		rightPage, _ := bt.pager.ReadPage(rightID)
		rightNode := WrapInternalPage(rightPage)

		if bytes.Compare(origKey, sepKey) < 0 {
			_ = leftNode.InsertSeparator(origKey, origChild)
			bt.writePage(leftNode.Page)
		} else {
			_ = rightNode.InsertSeparator(origKey, origChild)
			bt.writePage(rightNode.Page)
		}

		leftID = leftNode.Page.ID
	}
}

// Entry point into insertion logic
func (bt *BTree) Insert(key, val []byte) (bool, error) {

	bt.pager.write.Lock()
	defer bt.pager.write.Unlock()

	leaf, parentStack, err := bt.descend(key)
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
