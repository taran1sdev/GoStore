package server

import (
	"fmt"

	"go.store/internal/engine"
)

type Msg string

type Response struct {
	Msg   Msg
	Close bool
}

const (
	Prompt Msg = "gostore> "

	OK Msg = "OK"

	NoAuth     Msg = "Not authenticated"
	NoPerm     Msg = "Permission denied"
	NoDB       Msg = "No DB currently open"
	OpenFailed Msg = "Failed to open Database"
)

func Usage(expected string) Response {
	return Response{Msg: Msg("ERR Usage: " + expected), Close: false}
}

func Fatal(errMsg Msg) Response {
	return Response{Msg: Msg("ERR: " + errMsg), Close: true}
}

func Err(errMsg Msg) Response {
	return Response{Msg: Msg("ERR: " + errMsg), Close: false}
}

func Respond(msg Msg) Response {
	return Response{Msg: msg, Close: false}
}

func (s *Server) authCommand(sess *Session, parts []string) Response {
	if len(parts) != 3 {
		return Usage("AUTH <username> <password>")
	}

	u, err := s.auth.Authenticate(parts[1], parts[2])
	if err != nil {
		return Err(Msg(err.Error()))
	}

	sess.user = u
	return Respond(OK)
}

func (s *Server) openDBCommand(sess *Session, parts []string) Response {
	if !sess.IsAuth() {
		return Err(NoAuth)
	}

	if len(parts) != 2 {
		return Usage("OPEN <dbname>")
	}

	dbname := parts[1]
	if !sess.user.CanOpenDB(dbname) {
		return Err(NoPerm)
	}

	sess.CloseDB()

	// Later we will have a dedicated data dir
	db, err := engine.Open(dbname, s.cfg)
	if err != nil {
		return Err(OpenFailed)
	}

	sess.database = db
	sess.dbName = dbname
	return Respond(OK)
}

func exitCommand(sess *Session, parts []string) Response {
	if sess.database != nil {
		sess.CloseDB()
	}

	return Response{Msg: OK, Close: true}
}

func setCommand(sess *Session, parts []string) Response {
	if sess.database == nil {
		return Err(NoDB)
	}

	if len(parts) != 3 {
		return Usage("SET <key> <val>")
	}

	if sess.user.IsGuest() {
		return Err(NoPerm)
	}

	if err := sess.database.Set(parts[1], []byte(parts[2])); err != nil {
		return Err(Msg(err.Error()))
	}

	return Respond(OK)
}

func getCommand(sess *Session, parts []string) Response {
	if sess.database == nil {
		return Err(NoDB)
	}

	if len(parts) != 2 {
		return Usage("GET <key>")
	}

	val, err := sess.database.Get(parts[1])
	if err != nil {
		return Err(Msg(err.Error()))
	}

	return Respond(Msg(fmt.Sprintf("%s: %s", parts[1], val)))
}

func delCommand(sess *Session, parts []string) Response {
	if sess.database == nil {
		return Err(NoDB)
	}

	if len(parts) != 2 {
		return Usage("DEL <key>")
	}

	if err := sess.database.Delete(parts[1]); err != nil {
		return Err(Msg(err.Error()))
	}

	return Respond(OK)
}
