package logger

import (
	"io"
	"log"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

type Logger struct {
	level  Level
	logger *log.Logger
}

func New(out io.Writer, level Level) *Logger {
	return &Logger{
		level:  level,
		logger: log.New(out, "", log.LstdFlags|log.Lmicroseconds),
	}
}

func (l *Logger) logf(level Level, format string, args ...any) {
	if level < l.level {
		return
	}
	l.logger.Printf(format, args...)
}

func (l *Logger) Debugf(format string, args ...any) {
	l.logf(DEBUG, format, args...)
}

func (l *Logger) Infof(format string, args ...any) {
	l.logf(INFO, format, args...)
}

func (l *Logger) Warnf(format string, args ...any) {
	l.logf(WARN, format, args...)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.logf(ERROR, format, args...)
}
