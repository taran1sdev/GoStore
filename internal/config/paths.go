package config

import (
	"os"
	"path/filepath"
)

type Paths struct {
	Home     string
	Config   string
	UserFile string
	DataDir  string
	LogDir   string
}

// Allow user to set app home through env variable
// otherwise default to ~/.local/share/gostore

func ResolvePaths(homeOverride, configOverride string) (*Paths, error) {
	home := homeOverride
	if home == "" {
		home = os.Getenv("GOSTORE_HOME")
	}

	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		home = filepath.Join(userHome, ".local", "share", "gostore")
	}

	if err := os.MkdirAll(home, 0o755); err != nil {
		return nil, err
	}

	cfgPath := configOverride
	if cfgPath == "" {
		cfgPath = filepath.Join(home, "config.yaml")
	}

	dataDir := filepath.Join(home, "data")
	logDir := filepath.Join(home, "log")
	userFile := filepath.Join(home, "users.json")

	_ = os.MkdirAll(dataDir, 0o755)
	_ = os.MkdirAll(logDir, 0o755)

	return &Paths{
		Home:     home,
		Config:   cfgPath,
		UserFile: userFile,
		DataDir:  dataDir,
		LogDir:   logDir,
	}, nil
}
