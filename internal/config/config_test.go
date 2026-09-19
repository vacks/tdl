package config

import (
	"path/filepath"
	"testing"
)

func TestLoadUsesDatabaseURL(t *testing.T) {
	t.Setenv("TDL_DATA_DIR", filepath.Join(t.TempDir(), "data"))
	t.Setenv("TDL_DOWNLOAD_DIR", filepath.Join(t.TempDir(), "downloads"))
	t.Setenv("TDL_DATABASE_URL", "postgres://user:password@db.example:5432/tdl?sslmode=require")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseDSN != "postgres://user:password@db.example:5432/tdl?sslmode=require" {
		t.Fatalf("unexpected database URL: %q", cfg.DatabaseDSN)
	}
}

func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("TDL_DATABASE_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("Load() succeeded without TDL_DATABASE_URL")
	}
}
