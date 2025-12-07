package engine

import (
	"go.store/internal/storage"
)

type Database struct {
	engine *Engine
	sync   bool
}

func Open(path string) (*Database, error) {
	pager, err := storage.Open(path)
	if err != nil {
		return nil, err
	}

	bt, bErr := storage.NewBTree(pager)
	if bErr != nil {
		return nil, bErr
	}

	eng := NewEngine(bt)

	return &Database{
		engine: eng,
		sync:   true,
	}, nil
}

func (db *Database) Set(key string, val []byte) error {
	return db.engine.Set(key, val)
}

func (db *Database) Delete(key string) error {
	return db.engine.Delete(key)
}

func (db *Database) Get(key string) ([]byte, error) {
	return db.engine.Get(key)
}

func (db *Database) Close() error {
	return db.engine.Close()
}
