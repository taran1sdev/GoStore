package engine

type Database struct {
	storage *Storage
	engine  *Engine
	closed  bool
	path    string
	sync    bool
}

func (db *Database) Open(path string) (*Database, error) {

}
