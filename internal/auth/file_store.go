package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type FileStore struct {
	path  string
	mu    sync.RWMutex
	users map[string]*User
}

func NewFileStore(path string) (*FileStore, error) {
	fs := &FileStore{
		path:  path,
		users: make(map[string]*User),
	}

	if err := fs.load(); err != nil {
		return nil, err
	}

	return fs, nil
}

// Load the user catalog from fs.path
func (fs *FileStore) load() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	f, err := os.Open(fs.path)
	if os.IsNotExist(err) {
		// Should only happen on first run
		// handle this later
		return nil
	}

	if err != nil {
		return err
	}
	defer f.Close()

	// parse the json file and populate the users map
	var list []*User
	if err := json.NewDecoder(f).Decode(&list); err != nil {
		return err
	}

	for _, u := range list {
		fs.users[u.Username] = u
	}
	return nil
}

// write from memory to user catalog
func (fs *FileStore) persist() error {
	f, err := os.Create(fs.path)
	if err != nil {
		return err
	}

	defer f.Close()

	list := make([]*User, 0, len(fs.users))
	for _, u := range fs.users {
		list = append(list, u)
	}
	return json.NewEncoder(f).Encode(list)
}

func (fs *FileStore) GetUser(username string) (*User, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	u, ok := fs.users[username]
	if !ok {
		return nil, fmt.Errorf("Error: user not found")
	}

	// Create a deep copy so we aren't holding a reference
	user := &User{
		Username: u.Username,
		Role:     u.Role,
		Password: u.Password,
		AccessDB: append([]string(nil), u.AccessDB...),
	}

	return user, nil
}

func (fs *FileStore) SaveUser(u *User) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.users[u.Username] = u
	return fs.persist()
}

func (fs *FileStore) DeleteUser(username string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	delete(fs.users, username)
	return fs.persist()
}

func (fs *FileStore) ListUsers() ([]*User, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	list := make([]*User, 0, len(fs.users))
	for _, u := range fs.users {
		list = append(list, u)
	}

	return list, nil
}
