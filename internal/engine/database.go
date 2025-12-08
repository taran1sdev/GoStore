package engine

type Database struct {
	engine *Engine
	sync   bool
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
