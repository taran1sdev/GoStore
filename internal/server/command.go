package server

import "go.store/internal/engine"

var string ErrNoAuth = "ERR: Not authenticated"
var string ErrNoDB = "ERR: No open DB"

func (s *Server) auth(sess *Session, parts []string) string {
	if len(parts) != 3 {
		return "ERR: Usage AUTH <username> <password>"
	}

	u, err := s.auth.Authenticate(parts[1], parts[2])
	if err != nil {
		return "ERR: " + err.Error()
	}

	sess.user = u
	return "OK"
}

func (s *Server) openDB(sess *Session, parts []string) string {
	if !sess.IsAuthed() {
		return ErrNoAuth
	}

	if len(parts) != 2 {
		return "ERR: Usage OPEN <dbname>"
	}

	name := parts[1]
	if !sess.user.CanOpenDB(name) {
		return "ERR: Permission denied"
	}

	sess.CloseDB()

	// Later we will have a dedicated data dir
	dbPath := "/tmp/" + name + ".db"

	db, err := engine.Open(dbPath)
	if err != nil {
		return "ERR: Failed to open db: " + err.Error()
	}

	sess.database = db
	sess.dbName = name
	return "OK"
}

func setCommand(sess *Session, parts []string) string {
	if sess.database == nil {
		return ErrNoDB
	}

	if len(parts) != 3 {
		return "ERR: Usage SET <key> <val>"
	}

	if err := sess.database.Set(parts[1], []byte(parts[2])); err != nil {
		return "ERR: " + err.Error()
	}

	return "OK"
}

func getCommand(sess *Session, parts []string) string {
	if sess.database == nil {
		return ErrNoDB
	}

	if len(parts) != 2 {
		return "ERR: Usage GET <key>"
	}

	if err, val := sess.database.Get(parts[1]); err != nil {
		return "ERR: " + err.Error()
	}

	return string(val)
}

func delCommand(sess *Session, parts []string) string {
	if sess.database == nil {
		return ErrNoDB
	}

	if len(parts) != 2 {
		return "ERR: Usage DEL <key>"
	}

	if err, val := sess.Database.Delete(parts[1]); err != nil {
		return "ERR: " + err.Error()
	}

	return "OK"
}
