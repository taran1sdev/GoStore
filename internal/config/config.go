package config

import (
	"errors"
	"fmt"
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

	EnableTLS bool   `yaml:"enable_tls"`
	CertDir   string `yaml:"cert_dir"`
	TLSCert   string `yaml:"tls_cert"`
	TLSKey    string `yaml:"tls_key"`
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

		EnableTLS: false,
		CertDir:   filepath.Join(home, "cert"),
		TLSCert:   filepath.Join(home, "cert", "server.crt"),
		TLSKey:    filepath.Join(home, "cert", "server.key"),
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
	_ = os.MkdirAll(cfg.CertDir, 0o755)

	if cfg.EnableTLS {
		if _, err := os.Stat(cfg.TLSCert); err != nil {
			return nil, fmt.Errorf("Could not find TLS certificate: %w")
		}
		if _, err := os.Stat(cfg.TLSKey); err != nil {
			return nil, fmt.Errorf("Could not find TLS key: %w")
		}
	}
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
