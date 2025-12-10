package auth

type Store interface {
	GetUser(username string) *User
	SaveUser(*User) error
	DeleteUser(username string) error
	ListUsers() ([]*User, error)
}
