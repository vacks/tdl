package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	ListenAddr      string
	DataDir         string
	DownloadDir     string
	AdminUsername   string
	InitialPassword string
	DatabaseDSN     string
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:      value("TDL_LISTEN_ADDR", ":8080"),
		DataDir:         value("TDL_DATA_DIR", "./data"),
		DownloadDir:     value("TDL_DOWNLOAD_DIR", "./downloads"),
		AdminUsername:   value("TDL_ADMIN_USERNAME", "admin"),
		InitialPassword: value("TDL_ADMIN_INITIAL_PASSWORD", "admin"),
		DatabaseDSN: strings.TrimSpace(os.Getenv("TDL_DATABASE_URL")),
	}
	if cfg.DatabaseDSN == "" {
		return Config{}, fmt.Errorf("TDL_DATABASE_URL is required")
	}
	if err := os.MkdirAll(filepath.Clean(cfg.DataDir), 0o700); err != nil {
		return Config{}, fmt.Errorf("create data directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Clean(cfg.DownloadDir), 0o755); err != nil {
		return Config{}, fmt.Errorf("create download directory: %w", err)
	}
	return cfg, nil
}

func value(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
