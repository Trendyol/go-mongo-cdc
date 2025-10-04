package logger

import (
	"fmt"
	"go.uber.org/zap"
)

var Log Logger

const (
	ERROR = "error"
	WARN  = "warn"
	INFO  = "info"
	DEBUG = "debug"
	TRACE = "trace"
)

type Logger interface {
	Trace(message string, args ...interface{})
	Debug(message string, args ...interface{})
	Info(message string, args ...interface{})
	Warn(message string, args ...interface{})
	Error(message string, args ...interface{})
	Log(level string, message string, args ...interface{})
}

type Loggers struct {
	Zap *zap.Logger
}

func (loggers *Loggers) Trace(message string, args ...interface{}) {
	loggers.Log(TRACE, message, args...)
}

func (loggers *Loggers) Debug(message string, args ...interface{}) {
	loggers.Log(DEBUG, message, args...)
}

func (loggers *Loggers) Info(message string, args ...interface{}) {
	loggers.Log(INFO, message, args...)
}

func (loggers *Loggers) Warn(message string, args ...interface{}) {
	loggers.Log(WARN, message, args...)
}

func (loggers *Loggers) Error(message string, args ...interface{}) {
	loggers.Log(ERROR, message, args...)
}

func (loggers *Loggers) Log(level string, message string, args ...interface{}) {
	formattedMsg := fmt.Sprintf(message, args...)

	switch level {
	case ERROR:
		loggers.Zap.Error(formattedMsg)
	case WARN:
		loggers.Zap.Warn(formattedMsg)
	case INFO:
		loggers.Zap.Info(formattedMsg)
	case DEBUG:
		loggers.Zap.Debug(formattedMsg)
	case TRACE:
		loggers.Zap.Debug(formattedMsg)
	default:
		loggers.Zap.Info(formattedMsg)
	}
}

func InitDefaultLogger(logLevel string) {
	config := zap.NewProductionConfig()

	level, err := parseLogLevel(logLevel)
	if err != nil {
		zap.L().Error("error while logger parse level", zap.Error(err))
		panic(err)
	}

	config.Level = level
	config.Encoding = "json"
	config.EncoderConfig.MessageKey = "message"

	zapLogger, err := config.Build(zap.AddCallerSkip(2))
	if err != nil {
		panic(err)
	}

	Log = &Loggers{
		Zap: zapLogger,
	}
}

func parseLogLevel(level string) (zap.AtomicLevel, error) {
	switch level {
	case ERROR:
		return zap.NewAtomicLevelAt(zap.ErrorLevel), nil
	case WARN:
		return zap.NewAtomicLevelAt(zap.WarnLevel), nil
	case INFO:
		return zap.NewAtomicLevelAt(zap.InfoLevel), nil
	case DEBUG, TRACE:
		return zap.NewAtomicLevelAt(zap.DebugLevel), nil
	default:
		return zap.NewAtomicLevelAt(zap.InfoLevel), fmt.Errorf("unknown log level: %s", level)
	}
}
