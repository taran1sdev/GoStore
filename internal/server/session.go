package server

import "go.store/internal/engine"

type Session struct {
	user     *User
	database *engine.Database
}
