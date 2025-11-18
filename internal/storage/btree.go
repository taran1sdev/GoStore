package storage 

type BTree struct {
	pager *Pager
	root  uint32
}

func (bt *BTree) getPage(pid uint32) (*Page, error) {
	if page, err := bt.pager.ReadPage(pid); err != nil {
		return nil, err
	}
	return page, nil
}
