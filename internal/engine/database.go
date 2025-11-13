package engine

import "go.store/internal/storage"

type Database struct {
	storage *storage.Storage
	engine  *Engine
}

func Open(path string) (*Database, error) {
	s, storageErr := storage.Open(path)
	if storageErr != nil {
		return nil, storageErr
	}

	records, recordsErr := s.Replay()
	if recordsErr != nil {
		return nil, recordsErr
	}

	eng := NewEngine()

	for _, r := range records {
		if r.Flag == storage.FlagSet {
			eng.Set(string(r.Key), r.Value)
		} else if r.Flag == storage.FlagDel {
			eng.Delete(string(r.Key))
		}
	}

	return &Database{
		storage: s,
		engine:  eng,
	}, nil
}
