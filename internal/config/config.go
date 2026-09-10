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
	CookieSecure    bool
	TrustProxy      bool
	TrustedOrigins  []string
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:      value("TDL_LISTEN_ADDR", ":8080"),
		DataDir:         value("TDL_DATA_DIR", "./data"),
		DownloadDir:     value("TDL_DOWNLOAD_DIR", "./downloads"),
		AdminUsername:   value("TDL_ADMIN_USERNAME", "admin"),
		InitialPassword: os.Getenv("TDL_ADMIN_INITIAL_PASSWORD"),
		CookieSecure:    boolValue("TDL_COOKIE_SECURE", false),
		TrustProxy:      boolValue("TDL_TRUST_PROXY", false),
		TrustedOrigins:  splitValues(os.Getenv("TDL_TRUSTED_ORIGINS")),
	}
	if strings.TrimSpace(cfg.InitialPassword) == "" {
		return Config{}, fmt.Errorf("TDL_ADMIN_INITIAL_PASSWORD is required for the first start")
	}
	if err := os.MkdirAll(filepath.Clean(cfg.DataDir), 0o700); err != nil {
		return Config{}, fmt.Errorf("create data directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Clean(cfg.DownloadDir), 0o755); err != nil {
		return Config{}, fmt.Errorf("create download directory: %w", err)
	}
	return cfg, nil
}

func boolValue(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return strings.EqualFold(value, "true") || value == "1" || strings.EqualFold(value, "yes")
}

func splitValues(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}

func value(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
