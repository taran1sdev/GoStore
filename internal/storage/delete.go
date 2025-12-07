package storage

func (bt *BTree) deleteFromLeaf(leaf *LeafPage, key []byte) (bool, error) {
	if err := leaf.Delete(key); err != nil {
		return false, err
	}

	if err := bt.writePage(leaf.Page); err != nil {
		return false, err
	}

	if leaf.Page.ID == bt.root || leaf.GetSpaceUsed() >= PageSize/2 {
		return false, nil
	}

	return true, nil
}

func (bt *BTree) shrinkRoot(root *InternalPage) error {
	if root.GetNumKeys() == 0 {
		onlyChild := root.GetChild(0)
		bt.root = onlyChild
		bt.meta.SetRootID(onlyChild)
		bt.FreePage(root.Page.ID)
	}
	return nil
}

func (bt *BTree) propogateDelete(stack *ParentStack, childID uint32) error {
	for {

		parentInfo, ok := stack.Pop()
		if !ok {
			return nil
		}

		parentPage, err := bt.pager.ReadPage(parentInfo.pageID)
		if err != nil {
			return err
		}
		parent := WrapInternalPage(parentPage)

		childPage, err := bt.pager.ReadPage(childID)
		if err != nil {
			return err
		}

		// Check first if the child is a leaf / internal page
		if childPage.Type == PageTypeLeaf {
			leaf := WrapLeafPage(childPage)

			idx := bt.findChildIndex(parent, childID)

			merged, err := bt.rebalanceLeaf(leaf, parent, idx)
			if err != nil {
				return err
			}

			if !merged {
				return nil
			}

			childID = parent.Page.ID
			continue
		}

		internal := WrapInternalPage(childPage)
		idx := bt.findChildIndex(parent, childID)

		merged, err := bt.rebalanceInternal(internal, parent, idx, childID)
		if err != nil {
			return err
		}

		if !merged {
			return nil
		}

		childID = parent.Page.ID

		if childID == bt.root {
			return bt.shrinkRoot(parent)
		}
	}
}

func (bt *BTree) Delete(key []byte) error {
	leaf, stack, err := bt.descend(key)
	if err != nil {
		return err
	}

	prop, err := bt.deleteFromLeaf(leaf, key)
	if err != nil {
		return err
	}

	if !prop {
		return nil
	}

	return bt.propogateDelete(stack, leaf.Page.ID)
}
