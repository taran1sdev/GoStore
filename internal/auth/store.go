package auth

type Store interface {
	GetUser(username string) (*User, error)
	SaveUser(*User) error
	DeleteUser(username string) error
	ListUsers() ([]*User, error)
}
