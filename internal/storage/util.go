package storage

import "encoding/binary"

// Helper to check if we need to write the meta page in memory to disk
func (bt *BTree) checkMeta() {
	if bt.metaDirty {
		bt.pager.WritePage(bt.meta.Page)
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

	prevHead := bt.meta.GetFreeHead()
	binary.LittleEndian.PutUint32(p.Data[1:5], prevHead)

	bt.meta.SetFreeHead(id)
	bt.metaDirty = true

	bt.pager.WritePage(p)
}
