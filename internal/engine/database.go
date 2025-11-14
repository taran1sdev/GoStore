package engine

import "go.store/internal/storage"

type Database struct {
	storage *storage.Storage
	engine  *Engine
	sync    bool
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
		sync:    true,
	}, nil
}

func (db *Database) Set(key string, value []byte) error {
	// First set the value in memory
	db.engine.Set(key, value)

	// Then append a record to storage
	if err := db.storage.AppendSet([]byte(key), value); err != nil {
		return err
	}
	return nil
}

func (db *Database) Delete(key string) error {
	// First remove the value from memory
	db.engine.Delete(key)

	// Then append a record to storage
	if err := db.storage.AppendDelete([]byte(key)); err != nil {
		return err
	}
	return nil
}

func (db *Database) Get(key string) ([]byte, error) {
	return engine.Get(key)
}
