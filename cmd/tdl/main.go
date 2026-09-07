package main

import (
	"log"
	"net/http"

	"github.com/vacks/tdl/internal/config"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/httpapi"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}

	server, err := httpapi.New(cfg)
	if err != nil {
		log.Fatalf("create server: %v", err)
	}

	applog.Info("app", "service_listening", "address", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, server); err != nil {
		log.Fatal(err)
	}
}
