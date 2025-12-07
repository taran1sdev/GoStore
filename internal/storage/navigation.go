package storage

// Create a type for parents in case future features require more properties
type Parent struct {
	pageID uint32
}

// Stack to push parents as we descend allowing us to propogate up the tree
type ParentStack struct {
	items []Parent
}

func (s *ParentStack) IsEmpty() bool {
	return len(s.items) == 0
}

func (s *ParentStack) Push(p Parent) {
	s.items = append(s.items, p)
}

func (s *ParentStack) Pop() (Parent, bool) {
	if !s.IsEmpty() {
		parent := s.items[len(s.items)-1]
		s.items = s.items[:len(s.items)-1]
		return parent, true
	}
	return Parent{}, false
}

// Single function to traverse the tree and return the correct leaf page / stack with visited parents
func (bt *BTree) descend(key []byte) (*LeafPage, *ParentStack, error) {
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

// Originally Parent held the child index but this resulted in errors after merges
// this helper ensures we always find the correct index in the parent node for the
// current child
func (bt *BTree) findChildIndex(parent *InternalPage, childID uint32) int {
	n := parent.GetNumKeys()

	if parent.GetRightChild() == childID {
		return n
	}

	for i := 0; i < n; i++ {
		if parent.GetChild(i) == childID {
			return i
		}
	}

	return -1
}
