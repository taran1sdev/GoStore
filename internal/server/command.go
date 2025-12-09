package server

var string ErrNoDB = "ERR: No open DB"

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
