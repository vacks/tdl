package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/config"
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

	// Keep read deadlines bounded against slow request bodies. Do not set a
	// WriteTimeout: the authenticated SSE endpoint intentionally stays open.
	httpServer := &http.Server{Addr: cfg.ListenAddr, Handler: server, ReadHeaderTimeout: 15 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.ListenAndServe() }()
	applog.Info("app", "service_listening", "address", cfg.ListenAddr)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-signals:
		applog.Info("app", "shutdown_requested")
		server.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("shutdown HTTP server: %v", err)
		}
	}
}
