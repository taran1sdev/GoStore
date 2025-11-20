package engine

import (
	"fmt"

	"go.store/internal/storage"
)

type Engine struct {
	tree *storage.BTree
}

func NewEngine(tree *storage.BTree) *Engine {
	return &Engine{
		tree: tree,
	}
}

func (e *Engine) Set(key string, value []byte) error {
	_, err := e.tree.Insert([]byte(key), value)
	return err
}

func (e *Engine) Get(key string) ([]byte, error) {
	val, ok, err := e.tree.Search([]byte(key))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("Key not found")
	}
	return val, nil
}
