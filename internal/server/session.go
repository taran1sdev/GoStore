package server

import (
	"go.store/internal/auth"
	"go.store/internal/engine"
)

type Session struct {
	user     *auth.User
	database *engine.Database
	dbName   string
}

func (s *Session) IsAuth() bool {
	return s.user != nil
}

func (s *Session) CloseDB() {
	if s.database != nil {
		_ = s.database.Close()
		s.database = nil
		s.dbName = ""
	}
}
