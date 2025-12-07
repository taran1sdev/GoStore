package storage

import (
	"bytes"
	"errors"
)

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
