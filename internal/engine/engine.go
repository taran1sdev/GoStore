package engine

import (
	"fmt"

	"go.store/internal/logger"
	"go.store/internal/storage"
)

type Engine struct {
	tree *storage.BTree
	log  *logger.Logger
}

func NewEngine(tree *storage.BTree, log *logger.Logger) *Engine {
	return &Engine{
		tree: tree,
		log:  log,
	}
}

func (e *Engine) Set(key string, value []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			e.log.Errorf("fatal storage error during set: %v", r)
			err = fmt.Errorf("fatal internal error: %v", r)
		}
	}()
	_, err = e.tree.Insert([]byte(key), value)
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

func (e *Engine) Delete(key string) error {
	return e.tree.Delete([]byte(key))
}

func (e *Engine) Close() error {
	return e.tree.Close()
}
