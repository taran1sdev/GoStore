package storage

import "encoding/binary"

// Avoid calling pager.WritePage directly and log all changes before they are made
func (bt *BTree) writePage(page *Page) error {
	return bt.pager.WritePage(page)
}

// Helper to check if we need to write the meta page in memory to disk
func (bt *BTree) checkMeta() {
	if bt.metaDirty {
		bt.writePage(bt.meta.Page)
		bt.metaDirty = false
	}
}

// Free any orphaned pages
func (bt *BTree) FreePage(id uint32) {
	if id == bt.root || id == 0 {
		return
	}

	p, _ := bt.pager.ReadPage(id)

	p.Type = PageTypeFree
	p.Data = make([]byte, PageSize)
	p.Data[0] = byte(PageTypeFree)

	prevHead := bt.meta.GetFreeHead()
	binary.LittleEndian.PutUint32(p.Data[1:5], prevHead)

	bt.meta.SetFreeHead(id)
	bt.metaDirty = true

	err := bt.writePage(p)
	if err != nil {
		bt.log.Errorf("FreePage: %v", err)
	}
}

func (bt *BTree) Close() error {
	return bt.pager.Close()
}
