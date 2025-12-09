package auth

import "fmt"

type Authenticator struct {
	store Store
}

func NewAuthenticator(store *Store) *Authenticator {
	return &Authenticator{store: store}
}

func (a *Authenticator) Authenticate(username, password string) (*User, error) {
	u, err := a.store.GetUser(username)
	// For now return "user not found" -  update both to Invalid Credentials after testing
	if err != nil {
		return nil, err
	}

	if !CheckPassword(u.Password, password) {
		return nil, fmt.Errorf("Invalid Credentials")
	}
	return u, nil
}

func (u *User) IsSuperuser() bool {
	return u.Role == RoleSuperuser
}

func (u *User) IsGuest() bool {
	return u.Role == RoleGuest
}

func (u *User) CanOpenDB(db string) bool {
	if u.IsSuperuser() {
		return true
	}

	for _, name := range u.AccessDB {
		if name == db {
			return true
		}
	}
	return false
}
