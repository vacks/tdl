package main

import (
	"context"
	"errors"
	"log"
	"net"
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

	// Claim the listen address before building the server. Creating the server
	// runs startup recovery against the database — re-queueing rows that another
	// instance may be transferring right now — so a port conflict has to be
	// detected first. Otherwise a second instance corrupts a running instance's
	// in-flight tasks and only then exits because the port was taken.
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", cfg.ListenAddr, err)
	}

	server, err := httpapi.New(cfg)
	if err != nil {
		_ = listener.Close()
		log.Fatalf("create server: %v", err)
	}

	// Keep ordinary requests bounded against slow readers and writers. The
	// authenticated SSE handler explicitly clears its deadline after it has
	// acquired one of the small, dedicated streaming slots.
	httpServer := &http.Server{Handler: server, ReadHeaderTimeout: 15 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.Serve(listener) }()
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
		// Drain HTTP before stopping the services underneath it. server.Stop
		// closes the database, and a request served against a closed database
		// turns a graceful shutdown into a burst of errors for whoever is still
		// connected.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("shutdown HTTP server: %v", err)
		}
		cancel()
		server.Stop()
	}
}
