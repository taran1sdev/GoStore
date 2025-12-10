package auth

import (
	"golang.org/x/crypto/bcrypt"
)

type Role string

const (
	// Read / Write on allowed DB
	RoleUser Role = "user"
	// Readonly on allowed DB
	RoleGuest Role = "guest"
)

type User struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	Role     Role     `json:"role"`
	AccessDB []string `json:"access_db"`
}

// Basic password hashing - might be fun to implement from scratch later
func HashPassword(plain string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
}

func CheckPassword(hash []byte, plain string) bool {
	return bcrypt.CompareHashAndPassword(hash, []byte(plain)) == nil
}

func (u *User) IsGuest() bool {
	return u.Role == RoleGuest
}

func (u *User) CanOpenDB(db string) bool {
	for _, name := range u.AccessDB {
		if name == db {
			return true
		}
	}
	return false
}
