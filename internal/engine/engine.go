package engine

import "fmt"

type Engine struct {
	data map[string][]byte
}

func NewEngine() *Engine {
	return &Engine{
		data: make(map[string][]byte),
	}
}

func (e *Engine) Set(key string, value []byte) {
	e.data[key] = value
}

func (e *Engine) Get(key string) ([]byte, error) {
	value, ok := e.data[key]
	if !ok {
		return nil, fmt.Errorf("Key does not exist")
	}
	return value, nil
}

func (e *Engine) Delete(key string) {
	delete(e.data, key)
}
