package config

import (
	"errors"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	Addr     string `yaml:"addr"`
	Home     string `yaml:"home"`
	DataDir  string `yaml:"data_dir"`
	LogDir   string `yaml:"log_dir"`
	UserFile string `yaml:"user_file"`
}

func LoadConfig(homeOverride, configOverride string) (*Config, error) {
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

	cfg := &Config{
		Addr:     "127.0.0.1:57083",
		Home:     home,
		DataDir:  filepath.Join(home, "data"),
		LogDir:   filepath.Join(home, "log"),
		UserFile: filepath.Join(home, "users.json"),
	}

	cfgPath := configOverride
	if cfgPath == "" {
		cfgPath = filepath.Join(home, "config.yaml")
	}

	if f, err := os.Open(cfgPath); err == nil {
		defer f.Close()
		if err := yaml.NewDecoder(f).Decode(cfg); err != nil {
			return nil, err
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err := WriteConfig(cfgPath, cfg); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	_ = os.MkdirAll(cfg.DataDir, 0o755)
	_ = os.MkdirAll(cfg.LogDir, 0o755)

	return cfg, nil
}

func WriteConfig(path string, cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return err
	}

	return nil
}
