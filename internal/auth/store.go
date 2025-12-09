package auth

type Store interface {
	GetUser(username string) (*User, error)
	SaveUser(*User) error
	ListUsers() ([]*User, error)
}
