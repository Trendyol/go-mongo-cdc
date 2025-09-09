package logger

import (
	"log/slog"

	"go.uber.org/zap"
)

type Logger interface {
	Debug(msg string, fields ...zap.Field)
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Fatal(msg string, fields ...zap.Field)
}

var defaultLogger *zap.Logger

func InitLogger(customLogger *zap.Logger) *zap.Logger {
	if customLogger != nil {
		defaultLogger = customLogger
		return customLogger
	}

	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	defaultLogger = logger
	return logger
}

func InitLoggerWithLevel(customLogger *zap.Logger, level slog.Level) *zap.Logger {
	if customLogger != nil {
		defaultLogger = customLogger
		return customLogger
	}

	config := zap.NewProductionConfig()

	var zapLevel zap.AtomicLevel
	switch level {
	case slog.LevelDebug:
		zapLevel = zap.NewAtomicLevelAt(zap.DebugLevel)
	case slog.LevelInfo:
		zapLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	case slog.LevelWarn:
		zapLevel = zap.NewAtomicLevelAt(zap.WarnLevel)
	case slog.LevelError:
		zapLevel = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		zapLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	config.Level = zapLevel
	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	defaultLogger = logger
	return logger
}

func GetLogger() *zap.Logger {
	if defaultLogger == nil {
		return InitLogger(nil)
	}
	return defaultLogger
}

func Debug(msg string, fields ...zap.Field) {
	GetLogger().Debug(msg, fields...)
}

func Info(msg string, fields ...zap.Field) {
	GetLogger().Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	GetLogger().Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	GetLogger().Error(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	GetLogger().Fatal(msg, fields...)
}

func NewSlog(level slog.Level) Logger {
	config := zap.NewProductionConfig()

	var zapLevel zap.AtomicLevel
	switch level {
	case slog.LevelDebug:
		zapLevel = zap.NewAtomicLevelAt(zap.DebugLevel)
	case slog.LevelInfo:
		zapLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	case slog.LevelWarn:
		zapLevel = zap.NewAtomicLevelAt(zap.WarnLevel)
	case slog.LevelError:
		zapLevel = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		zapLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	config.Level = zapLevel
	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	return &zapLogger{logger: logger}
}

type zapLogger struct {
	logger *zap.Logger
}

func (z *zapLogger) Debug(msg string, fields ...zap.Field) {
	z.logger.Debug(msg, fields...)
}

func (z *zapLogger) Info(msg string, fields ...zap.Field) {
	z.logger.Info(msg, fields...)
}

func (z *zapLogger) Warn(msg string, fields ...zap.Field) {
	z.logger.Warn(msg, fields...)
}

func (z *zapLogger) Error(msg string, fields ...zap.Field) {
	z.logger.Error(msg, fields...)
}

func (z *zapLogger) Fatal(msg string, fields ...zap.Field) {
	z.logger.Fatal(msg, fields...)
}
