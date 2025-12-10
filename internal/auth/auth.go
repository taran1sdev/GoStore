package auth

import "fmt"

type Authenticator struct {
	store Store
}

func NewAuthenticator(store Store) *Authenticator {
	return &Authenticator{store: store}
}

func (a *Authenticator) Authenticate(username, password string) (*User, error) {
	u := a.store.GetUser(username)
	if u == nil {
		return nil, fmt.Errorf("Invlid Credentials")
	}

	if !CheckPassword([]byte(u.Password), password) {
		return nil, fmt.Errorf("Invalid Credentials")
	}
	return u, nil
}

// This should only be called after verifying SU permissions
func (a *Authenticator) Store() Store {
	return a.store
}
