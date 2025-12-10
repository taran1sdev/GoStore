package server

import (
	"errors"
	"os"
	"slices"

	"go.store/internal/auth"
)

func (s *Server) createUserCommand(sess *Session, parts []string) Response {
	if !sess.IsAuth() {
		return Err(NoAuth)
	}

	if !sess.user.IsSuperuser() {
		return Err(NoPerm)
	}

	if len(parts) != 4 {
		return Usage("CREATEUSER <username> <password> <role>")
	}

	username := parts[1]

	if user, _ := s.auth.Store().GetUser(username); user != nil {
		return Err(Msg("User already exists"))
	}

	password := parts[2]
	role := auth.Role(parts[3])

	switch role {
	case auth.RoleSuperuser:
		break
	case auth.RoleUser:
		break
	case auth.RoleGuest:
		break
	default:
		return Err(Msg("Invalid Role"))
	}

	// Later we should implement minimum length / complexity
	hash, err := auth.HashPassword(password)
	if err != nil {
		return Err(Msg("Failed to hash password"))
	}

	u := &auth.User{
		Username: username,
		Password: string(hash),
		Role:     role,
		AccessDB: []string{},
	}

	if err := s.auth.Store().SaveUser(u); err != nil {
		return Err(Msg(err.Error()))
	}

	return Respond(OK)
}

func (s *Server) delUserCommand(sess *Session, parts []string) Response {
	if !sess.IsAuth() {
		return Err(NoAuth)
	}

	if !sess.user.IsSuperuser() {
		return Err(NoPerm)
	}

	if len(parts) != 2 {
		return Usage("DELUSER <username>")
	}

	_, err := s.auth.Store().GetUser(parts[1])
	if err != nil {
		return Err(Msg(err.Error()))
	}

	if err := s.auth.Store().DeleteUser(parts[1]); err != nil {
		return Err(Msg(err.Error()))
	}

	return Respond(OK)
}

func (s *Server) grantDBCommand(sess *Session, parts []string) Response {
	if !sess.IsAuth() {
		return Err(NoAuth)
	}

	if !sess.user.IsSuperuser() {
		return Err(NoPerm)
	}

	if len(parts) != 3 {
		return Usage("GRANTDB <user> <dbname>")
	}

	u, err := s.auth.Store().GetUser(parts[1])
	if err != nil {
		return Err(Msg(err.Error()))
	}

	u.AccessDB = append(u.AccessDB, parts[2])

	if err := s.auth.Store().SaveUser(u); err != nil {
		return Err(Msg("Failed to grant DB"))
	}

	return Respond(OK)
}

func (s *Server) revokeDBCommand(sess *Session, parts []string) Response {
	if !sess.IsAuth() {
		return Err(NoAuth)
	}

	if !sess.user.IsSuperuser() {
		return Err(NoPerm)
	}

	if len(parts) != 3 {
		return Usage("REVOKEDB <user> <dbname>")
	}

	u, err := s.auth.Store().GetUser(parts[1])
	if err != nil {
		return Err(Msg(err.Error()))
	}

	if i := slices.Index(u.AccessDB, parts[2]); i != -1 {
		u.AccessDB = append(u.AccessDB[:i], u.AccessDB[i+1:]...)
	}

	if err := s.auth.Store().SaveUser(u); err != nil {
		return Err(Msg(err.Error()))
	}

	return Respond(OK)
}

func (s *Server) dropDBCommand(sess *Session, parts []string) Response {
	if !sess.IsAuth() {
		return Err(NoAuth)
	}

	if !sess.user.IsSuperuser() {
		return Err(NoPerm)
	}

	if len(parts) != 2 {
		return Usage("DROPDB <dbname>")
	}

	dbname := parts[1]
	path := "/tmp" + dbname + ".db"

	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return Err(Msg("Could not find DB"))
	}

	if err := os.Remove(path); err != nil {
		return Err(Msg("Failed to remove DB"))
	}

	return Respond(OK)
}
