package tgclient

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/vacks/tdl/internal/applog"
)

// gotdLogger routes the library's own log into this application's log.
//
// The library is otherwise silent - telegram.Options.Logger defaults to
// zap.NewNop - and when a transfer connection dies the library is the only
// thing that knows why: it logs the connection death, the reason it was closed,
// the pool's decision to replace or drop it, and every reconnect it makes
// underneath the callbacks it does not re-run. Without this, a transfer that
// stopped halfway could only be described by its consequence.
//
// Warnings and errors are kept; the debug stream is what a protocol-level
// investigation needs and is far too large to keep by default.
func gotdLogger() *zap.Logger {
	return zap.New(&logCore{}, zap.AddCallerSkip(1))
}

type logCore struct{}

func (c *logCore) Enabled(level zapcore.Level) bool { return level >= zapcore.WarnLevel }

func (c *logCore) With(fields []zapcore.Field) zapcore.Core { return c }

func (c *logCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c *logCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	encoder := zapcore.NewMapObjectEncoder()
	for _, field := range fields {
		field.AddTo(encoder)
	}
	args := make([]any, 0, len(encoder.Fields)*2+2)
	args = append(args, "at", entry.Caller.TrimmedPath())
	for key, value := range encoder.Fields {
		args = append(args, key, value)
	}
	switch entry.Level {
	case zapcore.ErrorLevel, zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		applog.Error("telegram", entry.Message, args...)
	default:
		applog.Warn("telegram", entry.Message, args...)
	}
	return nil
}

func (c *logCore) Sync() error { return nil }
