package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// GetLogger returns a configured zap logger based on the LOG_LEVEL environment variable.
// It defaults to Info level if LOG_LEVEL is not set or is invalid.
func GetLogger() *zap.Logger {
	level := os.Getenv("LOG_LEVEL")
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zapLevel)
	logger, _ := cfg.Build()
	return logger
}

func GetDebugLogger() *zap.Logger {
	cfg := zap.NewDevelopmentConfig()
	logger, _ := cfg.Build()
	return logger
}

func GetTestLogger() *zap.Logger {
	return zap.NewNop()
}
