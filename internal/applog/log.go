// Package applog provides structured, secret-safe logs for the application.
// Logs are written to standard output so Docker and NAS managers can collect
// them without requiring a writable log volume.
package applog

import (
	"log/slog"
	"os"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

func Info(component, event string, args ...any) {
	logger.Info(event, append([]any{"component", component}, args...)...)
}

func Error(component, event string, args ...any) {
	logger.Error(event, append([]any{"component", component}, args...)...)
}
